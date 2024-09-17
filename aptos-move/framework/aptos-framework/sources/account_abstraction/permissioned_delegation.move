module aptos_framework::permissioned_delegation {
    use std::error;
    use std::option;
    use std::option::Option;
    use std::signer;
    use aptos_std::ed25519;
    use aptos_std::from_bcs;
    use aptos_std::smart_table;
    use aptos_std::smart_table::SmartTable;
    use aptos_framework::transaction_context;

    /// Only fungible asset metadata owner can make changes.
    const EINVALID_PUBLIC_KEY: u64 = 1;
    const EPUBLIC_KEY_NOT_FOUND: u64 = 2;
    const EINVALID_SIGNATURE: u64 = 3;

    struct PermissionedHandle {}

    struct Signature {
        public_key: ed25519::UnvalidatedPublicKey,
        signature: ed25519::Signature,
    }

    /// Store the BLS public key.
    struct Delegation has key, drop {
        handles: SmartTable<ed25519::UnvalidatedPublicKey, PermissionedHandle>
    }

    /// Update the public key.
    public fun update_permissioned_handle(
        master: &signer,
        key: vector<u8>,
        handle: Option<PermissionedHandle>
    ) acquires Delegation {
        assert!(!is_permissioned_signer(master), error::permission_denied(ENOT_MASTER_SIGNER));
        let addr = signer::address_of(master);
        let pubkey = ed25519::new_unvalidated_public_key_from_bytes(key);
        if (!exists<Delegation>(addr)) {
            move_to(master, Delegation {
                handles: smart_table::new()
            });
        };
        let handles = &mut borrow_global_mut<Delegation>(addr).handles;
        if (smart_table::contains(handles, pubkey)) {
            if (option::is_some(&handle)) {
                *smart_table::borrow_mut(handles, pubkey) = option::destroy_some(handle);
            } else {
                smart_table::remove(handles, pubkey);
            }
        } else if (option::is_some(&handle)) {
            smart_table::add(handles, pubkey, option::destroy_some(handle));
        };
    }

    /// Authorization function for account abstraction.
    public fun authenticate(account: signer, signature: vector<u8>): signer {
        let addr = signer::address_of(&account);
        let Signature {
            public_key,
            signature,
        } = from_bcs::from_bytes<Signature>(signature);
        assert!(
            ed25519::signature_verify_strict(
                &signature,
                &public_key,
                transaction_context::get_transaction_hash(),
            ),
            EINVALID_SIGNATURE
        );
        if (exists<Delegation>(addr)) {
            let handles = &borrow_global<Delegation>(addr).handles;
            if (smart_table::contains(handles, public_key)) {
                signer_from_permissioned(smart_table::borrow(handles, public_key))
            } else {
                account
            }
        }
    }
}
