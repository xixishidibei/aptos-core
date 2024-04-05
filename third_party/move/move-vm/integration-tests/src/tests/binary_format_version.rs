// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use move_binary_format::{
    deserializer::DeserializerConfig,
    file_format::{basic_test_module, basic_test_script},
    file_format_common::{IDENTIFIER_SIZE_MAX, VERSION_DEFAULT, VERSION_MAX},
};
use move_core_types::{account_address::AccountAddress, vm_status::StatusCode};
use move_vm_runtime::{config::VMConfig, module_traversal::*, move_vm::MoveVM};
use move_vm_test_utils::InMemoryStorage;
use move_vm_types::gas::UnmeteredGasMeter;

#[test]
fn test_publish_module_with_custom_max_binary_format_version() {
    let mut m = basic_test_module();
    let mut b_new = vec![];
    let mut b_old = vec![];
    m.serialize_for_version(Some(VERSION_MAX), &mut b_new)
        .unwrap();
    m.version = VERSION_DEFAULT;
    m.serialize_for_version(Some(VERSION_MAX.checked_sub(1).unwrap()), &mut b_old)
        .unwrap();

    // Should accept both modules with the default settings
    {
        let storage = InMemoryStorage::new();
        let vm = MoveVM::new(move_stdlib::natives::all_natives(
            AccountAddress::from_hex_literal("0x1").unwrap(),
            move_stdlib::natives::GasParameters::zeros(),
        ))
        .unwrap();
        let mut sess = vm.new_session(&storage);

        sess.publish_module(
            b_new.clone(),
            *m.self_id().address(),
            &mut UnmeteredGasMeter,
        )
        .unwrap();

        sess.publish_module(
            b_old.clone(),
            *m.self_id().address(),
            &mut UnmeteredGasMeter,
        )
        .unwrap();
    }

    // Should reject the module with newer version with max binary format version being set to VERSION_MAX - 1
    {
        let storage = InMemoryStorage::new();
        let vm = MoveVM::new_with_config(
            move_stdlib::natives::all_natives(
                AccountAddress::from_hex_literal("0x1").unwrap(),
                move_stdlib::natives::GasParameters::zeros(),
            ),
            VMConfig {
                deserializer_config: DeserializerConfig::new(
                    VERSION_MAX.checked_sub(1).unwrap(),
                    IDENTIFIER_SIZE_MAX,
                ),
                ..Default::default()
            },
        )
        .unwrap();
        let mut sess = vm.new_session(&storage);

        assert_eq!(
            sess.publish_module(
                b_new.clone(),
                *m.self_id().address(),
                &mut UnmeteredGasMeter,
            )
            .unwrap_err()
            .major_status(),
            StatusCode::UNKNOWN_VERSION
        );

        sess.publish_module(
            b_old.clone(),
            *m.self_id().address(),
            &mut UnmeteredGasMeter,
        )
        .unwrap();
    }
}

#[test]
fn test_run_script_with_custom_max_binary_format_version() {
    let mut s = basic_test_script();
    let mut b_new = vec![];
    let mut b_old = vec![];
    s.serialize_for_version(Some(VERSION_MAX), &mut b_new)
        .unwrap();
    s.version = VERSION_DEFAULT;
    s.serialize_for_version(Some(VERSION_MAX.checked_sub(1).unwrap()), &mut b_old)
        .unwrap();
    let traversal_storage = TraversalStorage::new();

    // Should accept both modules with the default settings
    {
        let storage = InMemoryStorage::new();
        let vm = MoveVM::new(move_stdlib::natives::all_natives(
            AccountAddress::from_hex_literal("0x1").unwrap(),
            move_stdlib::natives::GasParameters::zeros(),
        ))
        .unwrap();
        let mut sess = vm.new_session(&storage);

        let args: Vec<Vec<u8>> = vec![];
        sess.execute_script(
            b_new.clone(),
            vec![],
            args.clone(),
            &mut UnmeteredGasMeter,
            &mut TraversalContext::new(&traversal_storage),
        )
        .unwrap();

        sess.execute_script(
            b_old.clone(),
            vec![],
            args,
            &mut UnmeteredGasMeter,
            &mut TraversalContext::new(&traversal_storage),
        )
        .unwrap();
    }

    // Should reject the module with newer version with max binary format version being set to VERSION_MAX - 1
    {
        let storage = InMemoryStorage::new();
        let vm = MoveVM::new_with_config(
            move_stdlib::natives::all_natives(
                AccountAddress::from_hex_literal("0x1").unwrap(),
                move_stdlib::natives::GasParameters::zeros(),
            ),
            VMConfig {
                deserializer_config: DeserializerConfig::new(
                    VERSION_MAX.checked_sub(1).unwrap(),
                    IDENTIFIER_SIZE_MAX,
                ),
                ..Default::default()
            },
        )
        .unwrap();
        let mut sess = vm.new_session(&storage);

        let args: Vec<Vec<u8>> = vec![];
        assert_eq!(
            sess.execute_script(
                b_new.clone(),
                vec![],
                args.clone(),
                &mut UnmeteredGasMeter,
                &mut TraversalContext::new(&traversal_storage)
            )
            .unwrap_err()
            .major_status(),
            StatusCode::CODE_DESERIALIZATION_ERROR
        );

        sess.execute_script(
            b_old.clone(),
            vec![],
            args,
            &mut UnmeteredGasMeter,
            &mut TraversalContext::new(&traversal_storage),
        )
        .unwrap();
    }
}
