// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Converts stackless bytecode into Model AST.
//!
//! See [this article](https://medium.com/leaningtech/solving-the-structured-control-flow-problem-once-and-for-all-5123117b1ee2)
//! for an inspiration how this code works.

use crate::{
    fat_loop::{build_loop_info, FatLoopFunctionInfo},
    function_target::FunctionTarget,
    stackless_bytecode::{AttrId, Bytecode, Label, Operation as BytecodeOperation},
    stackless_control_flow_graph::{BlockContent, BlockId, StacklessControlFlowGraph},
};
use itertools::Itertools;
use move_binary_format::file_format::CodeOffset;
use move_model::{
    ast::{Exp, ExpData, Operation, Pattern, TempIndex},
    model::{GlobalEnv, NodeId},
    symbol::Symbol,
    ty::Type,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

struct Context<'a> {
    target: &'a FunctionTarget<'a>,
    loop_info: BTreeMap<Label, LoopInfo>,
    back_edges: BTreeSet<CodeOffset>,
    forward_cfg: StacklessControlFlowGraph,
    backward_cfg: StacklessControlFlowGraph,
    label_offsets: BTreeMap<Label, CodeOffset>,
    if_true_blocks: BTreeSet<BlockId>,
}

struct Generator<'a> {
    temps: BTreeMap<TempIndex, (NodeId, Symbol)>,
    block_stack: Vec<BlockInfo<'a>>,
}

#[derive(Debug)]
struct BlockInfo<'a> {
    stms: Vec<Exp>,
    kind: BlockKind<'a>,
}

#[derive(Debug)]
enum BlockKind<'a> {
    Root,
    Loop {
        info: &'a LoopInfo,
    },
    IfStart {
        cond: Exp,
        if_true_block: BlockId,
        if_false_block: BlockId,
    },
    IfThen {
        cond: Exp,
        if_false_block: BlockId,
        opt_end_block: Option<BlockId>,
    },
    IfElse {
        cond: Exp,
        then: Exp,
        end_block: BlockId,
    },
    Jump {
        target: BlockId,
    },
}

#[derive(Debug)]
struct LoopInfo {
    header_label: Label,
    back_edges: BTreeSet<CodeOffset>,
    back_edge_blocks: BTreeSet<BlockId>,
    all_blocks: BTreeSet<BlockId>,
}

pub fn generate_ast(target: &FunctionTarget) -> Option<Exp> {
    let fat_loop_info = match build_loop_info(target) {
        Ok(loop_info) => loop_info,
        Err(err) => {
            target.global_env().error(
                &target.get_loc(),
                &format!("cannot decompile function: {}", err),
            );
            return None;
        },
    };
    let back_edges = fat_loop_info.back_edges_locations();
    let code = target.get_bytecode();
    let forward_cfg = StacklessControlFlowGraph::new_forward(code);
    let backward_cfg = StacklessControlFlowGraph::new_backward(code, false);
    let label_offsets = Bytecode::label_offsets(code);
    let loop_info = compute_loop_info(&backward_cfg, &label_offsets, &fat_loop_info);
    let if_true_blocks = compute_if_true_blocks(code, &label_offsets);
    let ctx = Context {
        target,
        loop_info,
        back_edges,
        forward_cfg,
        backward_cfg,
        label_offsets,
        if_true_blocks,
    };
    let mut gen = Generator {
        temps: BTreeMap::new(),
        block_stack: vec![],
    };
    Some(gen.gen(&ctx))
}

fn compute_loop_info(
    backward_cfg: &StacklessControlFlowGraph,
    label_offsets: &BTreeMap<Label, CodeOffset>,
    loop_info: &FatLoopFunctionInfo,
) -> BTreeMap<Label, LoopInfo> {
    let mut loop_infos = BTreeMap::new();
    for (label, fat_loop) in &loop_info.fat_loops {
        let header_block_id = label_offsets
            .get(label)
            .expect("loop header label has code offset");
        let back_edges = fat_loop.back_edges.clone();
        let back_edge_blocks = back_edges
            .iter()
            .map(|code_offset| backward_cfg.enclosing_block(*code_offset))
            .collect();
        let mut all_blocks = BTreeSet::new();
        all_blocks.insert(*header_block_id);
        let mut todo = back_edges.iter().cloned().collect_vec();
        while let Some(blk_id) = todo.pop() {
            if !all_blocks.insert(blk_id) {
                // Already processed, or header
                continue;
            }
            if let Some(succs) = backward_cfg.get_successors_map().get(&blk_id) {
                todo.extend(succs.iter().cloned())
            }
        }
        loop_infos.insert(*label, LoopInfo {
            header_label: *label,
            back_edges,
            back_edge_blocks,
            all_blocks,
        });
    }
    loop_infos
}

fn compute_if_true_blocks(
    code: &[Bytecode],
    label_offsets: &BTreeMap<Label, CodeOffset>,
) -> BTreeSet<BlockId> {
    let mut result = BTreeSet::new();
    for bc in code {
        if let Bytecode::Branch(_, if_true, ..) = bc {
            result.insert(
                label_offsets
                    .get(if_true)
                    .expect("label offset for if_true block target")
                    .clone(),
            );
        }
    }
    result
}

impl<'a> Context<'a> {
    fn code(&self) -> &[Bytecode] {
        self.target.get_bytecode()
    }

    fn block_of_label(&self, label: Label) -> BlockId {
        *self
            .label_offsets
            .get(&label)
            .expect("label has code offset")
    }

    fn label_of_block(&self, ctx: &Context, block_id: BlockId) -> Option<Label> {
        if let Some(Bytecode::Label(_, label)) = &ctx.code().get(block_id as usize) {
            Some(*label)
        } else {
            None
        }
    }

    fn env(&self) -> &GlobalEnv {
        self.target.global_env()
    }
}

impl<'a> Generator<'a> {
    fn gen(&mut self, ctx: &'a Context) -> Exp {
        let mut blocks = vec![];
        self.sort_blocks_forward(
            ctx,
            ctx.forward_cfg.entry_block(),
            &mut BTreeSet::new(),
            &mut blocks,
        );
        eprintln!("{} blocks", blocks.len());
        self.block_stack.push(BlockInfo {
            stms: vec![],
            kind: BlockKind::Root,
        });
        for i in 0..blocks.len() {
            let blk_id = blocks[i];
            if i + 1 == blocks.len() {
                self.gen_block(ctx, blk_id, None)
            } else {
                // Check whether block needs to be closed
                let next_blk_id = blocks[i + 1];
                self.gen_block(ctx, blk_id, Some(next_blk_id));
                match self.top_block().kind {
                    BlockKind::Root => {
                        // Never closes
                    },
                    BlockKind::Loop { info } => {
                        // We are closing this loop if the next block is not part of it
                        if !info.all_blocks.contains(&next_blk_id) {
                            self.close_block(ctx)
                        }
                    },
                    BlockKind::IfStart { if_true_block, .. } => {
                        assert_eq!(
                            if_true_block, next_blk_id,
                            "expected then-branch to be direct successor"
                        );
                        self.close_block(ctx)
                    },
                    BlockKind::IfThen { if_false_block, .. } if if_false_block == next_blk_id => {
                        self.close_block(ctx)
                    },
                    BlockKind::IfElse { end_block, .. } if end_block == next_blk_id => {
                        self.close_block(ctx)
                    },
                    BlockKind::Jump { target } => {
                        assert_eq!(target, next_blk_id, "expected Jump to be direct successor");
                        self.close_block(ctx)
                    },
                    _ => {
                        // no closing needed
                    },
                }
            }
        }
        while self.block_stack.len() > 1 {
            self.close_block(ctx)
        }
        let BlockInfo {
            stms,
            kind: BlockKind::Root,
        } = self.block_stack.pop().unwrap()
        else {
            panic!("unexpected block stack member")
        };
        self.make_seq(ctx, self.default_node_id(ctx), stms)
    }

    fn close_block(&mut self, ctx: &Context) {
        eprintln!("closing current block at {:?}", self.block_stack);
        let BlockInfo { stms, kind } = self.block_stack.pop().unwrap();
        let body = self.make_seq(ctx, self.default_node_id(ctx), stms);
        match kind {
            BlockKind::Root => {
                panic!("unexpected close of root block")
            },
            BlockKind::Loop { .. } => {
                let exp = ExpData::Loop(self.default_node_id(ctx), body).into_exp();
                self.add_stm(exp)
            },
            BlockKind::IfStart {
                cond,
                if_false_block,
                ..
            } => {
                self.block_stack.push(BlockInfo {
                    stms: vec![],
                    kind: BlockKind::IfThen {
                        cond,
                        if_false_block,
                        opt_end_block: None,
                    },
                });
            },
            BlockKind::IfThen {
                cond,
                opt_end_block: None,
                ..
            } => {
                let exp =
                    ExpData::IfElse(self.default_node_id(ctx), cond, body, self.make_nop(ctx))
                        .into_exp();
                self.add_stm(exp)
            },
            BlockKind::IfThen {
                cond,
                opt_end_block: Some(end_block),
                ..
            } => self.block_stack.push(BlockInfo {
                stms: vec![],
                kind: BlockKind::IfElse {
                    cond,
                    then: body,
                    end_block,
                },
            }),
            BlockKind::IfElse { cond, then, .. } => {
                let exp = ExpData::IfElse(self.default_node_id(ctx), cond, then, body).into_exp();
                self.add_stm(exp)
            },
            BlockKind::Jump { .. } => self.add_stm(body),
        }
    }

    fn gen_block(&mut self, ctx: &'a Context, blk_id: BlockId, next_block_id: Option<BlockId>) {
        eprintln!("processing {:?}", ctx.forward_cfg.content(blk_id));
        let BlockContent::Basic { lower, upper } = ctx.forward_cfg.content(blk_id) else {
            // Dummy block, skip
            return;
        };
        if let Some(info) = ctx
            .label_of_block(ctx, blk_id)
            .and_then(|l| ctx.loop_info.get(&l))
        {
            // This is a loop, start a loop block
            self.block_stack.push(BlockInfo {
                stms: vec![],
                kind: BlockKind::Loop { info },
            })
        }
        if *upper > *lower {
            self.gen_block_content(ctx, *lower, *upper - 1);
        }
        let code = ctx.code();
        match &code[*upper as usize] {
            Bytecode::Ret(id, temps) => {
                let exp = ExpData::Return(
                    self.make_node_id(ctx, *id),
                    self.make_temp_tuple(ctx, *id, temps),
                )
                .into_exp();
                self.add_stm(exp)
            },
            Bytecode::Abort(id, temp) => {
                let exp = ExpData::Call(self.make_node_id(ctx, *id), Operation::Abort, vec![
                    self.make_temp(ctx, *id, *temp)
                ])
                .into_exp();
                self.add_stm(exp)
            },
            Bytecode::Branch(id, if_true, if_false, cond) => {
                let mut remaining_targets = vec![];
                for label in [*if_true, *if_false] {
                    if let Some(exp) = self.maybe_gen_loop_edge(ctx, *upper, *id, label) {
                        self.add_stm(exp)
                    } else {
                        remaining_targets.push(ctx.block_of_label(label))
                    }
                }
                if remaining_targets.len() == 2 {
                    let cond = self.make_temp(ctx, *id, *cond);
                    let (cond, if_true_block, if_false_block) = match next_block_id {
                        Some(next_blk_id) if remaining_targets[1] == next_blk_id => (
                            self.make_not(ctx, cond),
                            remaining_targets[1],
                            remaining_targets[0],
                        ),
                        _ => (cond, remaining_targets[0], remaining_targets[1]),
                    };
                    self.block_stack.push(BlockInfo {
                        stms: vec![],
                        kind: BlockKind::IfStart {
                            cond,
                            if_true_block,
                            if_false_block,
                        },
                    })
                } else if remaining_targets.len() == 1 {
                    // Deal with as Jump
                    self.block_stack.push(BlockInfo {
                        stms: vec![],
                        kind: BlockKind::Jump {
                            target: remaining_targets[0],
                        },
                    })
                } else {
                    // Ignore
                }
            },
            Bytecode::Jump(id, label) => {
                if let Some(exp) = self.maybe_gen_loop_edge(ctx, *upper, *id, *label) {
                    self.add_stm(exp)
                } else {
                    // Expecting this to happen only at the end of the 'then' of an if
                    if let BlockKind::IfThen { opt_end_block, .. } = &mut self.top_block_mut().kind
                    {
                        *opt_end_block = Some(blk_id);
                    }
                }
            },
            _ => {
                panic!("malformed control flow graph block without terminating instruction")
            },
        }
    }

    fn maybe_gen_loop_edge(
        &mut self,
        ctx: &Context,
        code_offset: CodeOffset,
        id: AttrId,
        label: Label,
    ) -> Option<Exp> {
        if let Some(BlockInfo {
            kind: BlockKind::Loop { info, .. },
            ..
        }) = self.block_stack.iter_mut().find(|info| {
            matches!(info, BlockInfo {
                kind: BlockKind::Loop { .. },
                ..
            })
        }) {
            if info.back_edges.contains(&code_offset) {
                // Back edge, generate continue
                return Some(ExpData::LoopCont(self.make_node_id(ctx, id), true).into_exp());
            } else {
                let target_blk = ctx.block_of_label(label);
                if !info.all_blocks.contains(&target_blk) {
                    // Exit edge, generate break
                    return Some(ExpData::LoopCont(self.make_node_id(ctx, id), false).into_exp());
                }
            }
        }
        None
    }

    fn gen_block_content(&mut self, ctx: &Context, lower: CodeOffset, upper: CodeOffset) {
        use Bytecode::*;
        use BytecodeOperation::*;
        for bc in &ctx.target.get_bytecode()[lower as usize..upper as usize] {
            eprintln!(
                "generating for {}",
                bc.display(&ctx.target, &ctx.label_offsets)
            );
            let node_id = self.make_node_id(ctx, bc.get_attr_id());
            let exp = match bc {
                Nop(_) | Label(_, _) => continue,
                Assign(id, lhs, rhs, _) => ExpData::Assign(
                    node_id,
                    self.make_temp_pat(ctx, *id, *lhs),
                    self.make_temp(ctx, *id, *rhs),
                )
                .into_exp(),
                Call(id, dests, Function(mid, fid, inst), srcs, _) => {
                    let call_id = self.clone_node_id(ctx, node_id);
                    ctx.target
                        .global_env()
                        .set_node_instantiation(call_id, inst.clone());
                    let call = ExpData::Call(
                        node_id,
                        Operation::MoveFunction(*mid, *fid),
                        srcs.iter().map(|s| self.make_temp(ctx, *id, *s)).collect(),
                    )
                    .into_exp();
                    ExpData::Assign(node_id, self.make_temp_pat_tuple(ctx, *id, dests), call)
                        .into_exp()
                },
                Call(id, dests, oper, srcs, _) => {
                    unimplemented!("operation {}", oper.display(&ctx.target))
                },
                Load(id, temp, value) => {
                    let lhs = self.make_temp_pat(ctx, *id, *temp);
                    let value_id = self.clone_node_id(ctx, lhs.node_id());
                    ExpData::Assign(
                        node_id,
                        self.make_temp_pat(ctx, *id, *temp),
                        ExpData::Value(value_id, value.to_model_value()).into_exp(),
                    )
                    .into_exp()
                },
                Branch(..) | Ret(..) | Jump(..) | Abort(..) => {
                    panic!("unexpected branching instruction in middle of block")
                },
                SpecBlock(_, _) | SaveMem(_, _, _) | SaveSpecVar(_, _, _) | Prop(_, _, _) => {
                    unimplemented!("ast generation of specs")
                },
            };
            self.add_stm(exp)
        }
    }

    fn top_block(&self) -> &BlockInfo<'a> {
        self.block_stack.last().expect("block stack not empty")
    }

    fn top_block_mut<'b>(&mut self) -> &mut BlockInfo<'a> {
        self.block_stack.last_mut().expect("block stack not empty")
    }

    fn add_stm(&mut self, exp: Exp) {
        self.top_block_mut().stms.push(exp)
    }

    fn sort_blocks(
        &self,
        ctx: &Context,
        blk_id: BlockId,
        visited: &mut BTreeSet<BlockId>,
        result: &mut Vec<BlockId>,
    ) {
        if !visited.insert(blk_id) {
            return;
        }
        let mut predecessors = ctx.backward_cfg.successors(blk_id).clone();
        predecessors.sort_by(|b1, b2| {
            if ctx.if_true_blocks.contains(b1) {
                if ctx.if_true_blocks.iter().contains(b2) {
                    Ordering::Equal
                } else {
                    Ordering::Less
                }
            } else {
                Ordering::Greater
            }
        });

        for predecessor in predecessors {
            if !ctx.back_edges.contains(&predecessor) {
                self.sort_blocks(ctx, predecessor, visited, result)
            }
        }
        result.push(blk_id)
    }

    fn sort_blocks_forward(
        &self,
        ctx: &Context,
        blk_id: BlockId,
        visited: &mut BTreeSet<BlockId>,
        result: &mut Vec<BlockId>,
    ) {
        if !visited.insert(blk_id) {
            return;
        }
        result.push(blk_id);
        let mut successors = ctx.forward_cfg.successors(blk_id).clone();
        successors.sort_by(|b1, b2| {
            if ctx.if_true_blocks.contains(b1) {
                if ctx.if_true_blocks.iter().contains(b2) {
                    Ordering::Equal
                } else {
                    Ordering::Less
                }
            } else {
                Ordering::Greater
            }
        });
        for succ in successors {
            if !ctx.back_edges.contains(&succ) {
                self.sort_blocks(ctx, succ, visited, result)
            }
        }
    }

    fn make_seq(&self, _ctx: &Context, id: NodeId, mut exps: Vec<Exp>) -> Exp {
        eprintln!("making sequence len {}", exps.len());
        if exps.len() == 1 {
            exps.pop().unwrap()
        } else {
            ExpData::Sequence(id, exps).into_exp()
        }
    }

    fn make_nop(&self, ctx: &Context) -> Exp {
        ExpData::Call(self.default_node_id(ctx), Operation::NoOp, vec![]).into_exp()
    }

    fn make_temp_decl(&mut self, ctx: &Context, id: AttrId, temp: TempIndex) -> (NodeId, Symbol) {
        let loc = ctx.target.get_bytecode_loc(id);
        self.temps
            .entry(temp)
            .or_insert_with(|| {
                let name = ctx.target.get_local_name(temp);
                let id = ctx
                    .target
                    .global_env()
                    .new_node(loc, ctx.target.get_local_type(temp).clone());
                (id, name)
            })
            .clone()
    }

    fn make_temp(&mut self, ctx: &Context, id: AttrId, temp: TempIndex) -> Exp {
        let (id, name) = self.make_temp_decl(ctx, id, temp);
        ExpData::LocalVar(id, name).into_exp()
    }

    fn make_temp_pat(&mut self, ctx: &Context, id: AttrId, temp: TempIndex) -> Pattern {
        let (id, name) = self.make_temp_decl(ctx, id, temp);
        Pattern::Var(id, name)
    }

    fn make_temp_tuple(&mut self, ctx: &Context, id: AttrId, temps: &[TempIndex]) -> Exp {
        let mut exps = temps
            .iter()
            .map(|temp| self.make_temp(ctx, id, *temp))
            .collect_vec();
        if exps.len() == 1 {
            exps.pop().unwrap()
        } else {
            let ty = Type::Tuple(
                exps.iter()
                    .map(|e| ctx.env().get_node_type(e.node_id()))
                    .collect(),
            );
            let node_id = ctx.env().new_node(ctx.target.get_bytecode_loc(id), ty);
            ExpData::Call(node_id, Operation::Tuple, exps).into_exp()
        }
    }

    fn make_temp_pat_tuple(&mut self, ctx: &Context, id: AttrId, temps: &[TempIndex]) -> Pattern {
        let mut pats = temps
            .iter()
            .map(|temp| self.make_temp_pat(ctx, id, *temp))
            .collect_vec();
        if pats.len() == 1 {
            pats.pop().unwrap()
        } else {
            let ty = Type::Tuple(
                pats.iter()
                    .map(|p| ctx.env().get_node_type(p.node_id()))
                    .collect(),
            );
            let node_id = ctx.env().new_node(ctx.target.get_bytecode_loc(id), ty);
            Pattern::Tuple(node_id, pats)
        }
    }

    fn make_not(&self, _ctx: &Context, exp: Exp) -> Exp {
        ExpData::Call(exp.node_id(), Operation::Not, vec![exp]).into_exp()
    }

    fn make_node_id(&self, ctx: &Context, id: AttrId) -> NodeId {
        let loc = ctx.target.get_bytecode_loc(id);
        ctx.env().new_node(loc, Type::unit())
    }

    fn clone_node_id(&self, ctx: &Context, id: NodeId) -> NodeId {
        let env = ctx.env();
        let new_id = env.new_node(env.get_node_loc(id), env.get_node_type(id));
        if let Some(inst) = env.get_node_instantiation_opt(id) {
            env.set_node_instantiation(new_id, inst.clone())
        }
        new_id
    }

    fn default_node_id(&self, ctx: &Context) -> NodeId {
        ctx.target
            .global_env()
            .new_node(ctx.target.get_loc(), Type::unit())
    }
}
