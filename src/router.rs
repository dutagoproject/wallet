use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs;
use zeroize::Zeroize;
use duta_core::amount::{
    parse_duta_to_dut_i64, BASE_UNIT, DEFAULT_DUST_CHANGE_DUT,
    DEFAULT_MAX_WALLET_FEE_DUT, DEFAULT_MIN_RELAY_FEE_PER_KB_DUT, DEFAULT_WALLET_FEE_DUT,
    DISPLAY_UNIT, DUTA_DECIMALS,
};

const MAX_WALLET_SEND_INPUTS: usize = 64;

fn format_dut_i64(amount_dut: i64) -> String {
    let scale = 10i128.pow(DUTA_DECIMALS as u32);
    let magnitude = (amount_dut as i128).abs();
    let whole = magnitude / scale;
    let frac = magnitude % scale;
    let sign = if amount_dut < 0 { "-" } else { "" };
    format!(
        "{sign}{whole}.{frac:0width$}",
        width = DUTA_DECIMALS as usize
    )
}

fn status_for_body_err(detail: &str) -> tiny_http::StatusCode {
    if detail.starts_with("body_too_large") {
        tiny_http::StatusCode(413)
    } else {
        tiny_http::StatusCode(400)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        advance_utxo_sync_from_pruned_boundary, blocks_from_pruned_error, classify_wallet_history_tx, db_wallet_path,
        decode_seed_hex_for_migration, merge_pending_wallet_txs, net_from_name,
        parse_wallet_utxo_snapshot_response, wallet_utxo_snapshot_retry_after_secs,
        insufficient_funds_body, net_from_wallet_path, prune_confirmed_pending_txs,
        parse_prune_below_from_blocks_from_error, query_param, relay_fee_for_tx_bytes,
        retry_from_pruned_boundary_for_empty_wallet,
        require_non_empty_passphrase, resolve_owned_input, select_inputs_for_need,
        send_success_body, should_probe_daemon_utxo_presence, sign_send_tx, simulate_send_plan,
        status_for_body_err, tx_output_address, wallet_needs_full_utxo_rebuild,
        wallet_public_name, wallet_refresh_error_code, wallet_state_network, OwnedInput,
        PendingBalanceStats, WalletSigner, MAX_WALLET_SEND_INPUTS,
    };
    use duta_core::netparams::Network;
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap, HashSet};

    #[test]
    fn db_wallet_path_only_accepts_encrypted_wallet_extensions() {
        assert!(db_wallet_path("wallet.db"));
        assert!(db_wallet_path("wallet.dat"));
        assert!(!db_wallet_path("wallet.json"));
    }

    #[test]
    fn passphrase_requirement_rejects_empty_values() {
        assert_eq!(
            require_non_empty_passphrase(""),
            Err("missing_passphrase".to_string())
        );
        assert_eq!(
            require_non_empty_passphrase("   "),
            Err("missing_passphrase".to_string())
        );
        assert_eq!(
            require_non_empty_passphrase("strong-pass"),
            Err("passphrase_too_short".to_string())
        );
        assert!(require_non_empty_passphrase("strong-pass-123").is_ok());
    }

    #[test]
    fn migration_seed_decode_requires_present_valid_seed() {
        assert_eq!(
            decode_seed_hex_for_migration(&json!({})),
            Err("wallet_seed_missing".to_string())
        );
        assert_eq!(
            decode_seed_hex_for_migration(&json!({"seed_hex":"zz"})),
            Err("wallet_seed_invalid".to_string())
        );
        assert_eq!(
            decode_seed_hex_for_migration(&json!({"seed_hex":"00ff"})).expect("seed should decode"),
            vec![0x00, 0xff]
        );
    }

    #[test]
    fn resolve_owned_input_prefers_wallet_utxo_address_when_present() {
        let utxo = super::super::Utxo {
            value: 46,
            height: 100,
            coinbase: true,
            address: "test1111111111111111111111111111111111111111".to_string(),
            txid: "abcd".to_string(),
            vout: 0,
        };
        let signer = WalletSigner {
            addr: utxo.address.clone(),
            sk_hex: "11".repeat(32),
            pub_hex: "22".repeat(32),
        };
        let by_pkh = HashMap::new();
        let by_addr = HashMap::from([(utxo.address.clone(), signer.clone())]);

        let owned = resolve_owned_input(18083, &utxo, &by_pkh, &by_addr)
            .expect("address-owned utxo should resolve without daemon");

        let OwnedInput { utxo: got_utxo, signer: got_signer } = owned.expect("owned input");
        assert_eq!(got_utxo.txid, "abcd");
        assert_eq!(got_signer.addr, signer.addr);
    }

    #[test]
    fn pruned_blocks_from_error_includes_prune_boundary() {
        assert_eq!(
            blocks_from_pruned_error(0, Some(512)),
            "daemon_pruned_wallet_rescan_incomplete: from=0 prune_below=512"
        );
    }

    #[test]
    fn pruned_blocks_from_error_without_boundary_still_fails_closed() {
        assert_eq!(
            blocks_from_pruned_error(1024, None),
            "daemon_pruned_wallet_rescan_incomplete: from=1024"
        );
    }

    #[test]
    fn status_for_body_err_maps_oversized_payloads_to_413() {
        assert_eq!(status_for_body_err("body_too_large: 1048577"), tiny_http::StatusCode(413));
        assert_eq!(status_for_body_err("invalid_json"), tiny_http::StatusCode(400));
    }

    #[test]
    fn wallet_refresh_error_code_distinguishes_transport_from_state_failures() {
        assert_eq!(
            wallet_refresh_error_code("connect_failed: connection refused"),
            "daemon_unreachable"
        );
        assert_eq!(
            wallet_refresh_error_code("read_failed: timed out"),
            "daemon_unreachable"
        );
        assert_eq!(
            wallet_refresh_error_code("daemon_pruned_wallet_rescan_incomplete: from=0"),
            "wallet_state_refresh_failed"
        );
        assert_eq!(
            wallet_refresh_error_code("blocks_from_invalid_json: expected value"),
            "wallet_state_refresh_failed"
        );
    }

    #[test]
    fn parse_prune_below_from_blocks_from_error_extracts_boundary() {
        assert_eq!(
            parse_prune_below_from_blocks_from_error(
                "daemon_pruned_wallet_rescan_incomplete: from=0 prune_below=1186"
            ),
            Some(1186)
        );
        assert_eq!(
            parse_prune_below_from_blocks_from_error(
                "daemon_pruned_wallet_rescan_incomplete: from=0"
            ),
            None
        );
    }

    #[test]
    fn empty_wallet_with_known_anchor_does_not_force_full_rebuild() {
        assert!(!wallet_needs_full_utxo_rebuild(&[], 1500, 1185));
        assert!(wallet_needs_full_utxo_rebuild(&[], 1500, 0));
    }

    #[test]
    fn retry_from_pruned_boundary_only_applies_to_empty_wallets() {
        assert_eq!(
            retry_from_pruned_boundary_for_empty_wallet(
                &[],
                "daemon_pruned_wallet_rescan_incomplete: from=0 prune_below=1186"
            ),
            Some(1186)
        );
        let populated = vec![super::super::Utxo {
            value: 1,
            height: 1200,
            coinbase: false,
            address: "duta6b2fe7cce017891863991b60d21b0d785b22bb3".to_string(),
            txid: "ab".repeat(32),
            vout: 0,
        }];
        assert_eq!(
            retry_from_pruned_boundary_for_empty_wallet(
                &populated,
                "daemon_pruned_wallet_rescan_incomplete: from=0 prune_below=1186"
            ),
            None
        );
    }

    #[test]
    fn history_scan_advances_to_prune_boundary_when_available() {
        assert_eq!(
            super::advance_history_scan_from_pruned_boundary(0, Some(328)),
            Some(328)
        );
        assert_eq!(
            super::advance_history_scan_from_pruned_boundary(328, Some(328)),
            None
        );
        assert_eq!(
            super::advance_history_scan_from_pruned_boundary(400, Some(328)),
            None
        );
        assert_eq!(
            super::advance_history_scan_from_pruned_boundary(0, None),
            None
        );
    }

    #[test]
    fn utxo_sync_advances_empty_wallet_to_prune_boundary() {
        assert_eq!(
            advance_utxo_sync_from_pruned_boundary(&[], 2662, Some(5515)).unwrap(),
            Some(5515)
        );
    }

    #[test]
    fn utxo_sync_fails_closed_when_pruned_gap_covers_tracked_utxos() {
        let tracked = vec![super::super::Utxo {
            value: 50_000_000,
            height: 2500,
            coinbase: false,
            address: "duta6b2fe7cce017891863991b60d21b0d785b22bb3".to_string(),
            txid: "ab".repeat(32),
            vout: 0,
        }];
        let err = advance_utxo_sync_from_pruned_boundary(&tracked, 2662, Some(5515)).unwrap_err();
        assert!(err.contains("wallet_pruned_history_gap"));
        assert!(err.contains("from=2662"));
        assert!(err.contains("prune_below=5515"));
        assert!(err.contains("tracked_utxos=1"));
    }

    #[test]
    fn utxo_sync_does_not_move_when_request_is_already_inside_retained_window() {
        assert_eq!(
            advance_utxo_sync_from_pruned_boundary(&[], 5515, Some(5515)).unwrap(),
            None
        );
        assert_eq!(
            advance_utxo_sync_from_pruned_boundary(&[], 6000, Some(5515)).unwrap(),
            None
        );
    }

    #[test]
    fn wallet_utxo_snapshot_response_parses_active_daemon_state() {
        let parsed = parse_wallet_utxo_snapshot_response(&json!({
            "ok": true,
            "tip_height": 5515,
            "utxos": [
                {
                    "txid": "ab".repeat(32),
                    "vout": 1,
                    "amount_dut": 10_000,
                    "height": 5500,
                    "coinbase": false,
                    "address": "dut1111111111111111111111111111111111111111"
                }
            ]
        }))
        .unwrap();
        assert_eq!(parsed.0, 5515);
        assert_eq!(parsed.1.len(), 1);
        assert_eq!(parsed.1[0].value, 10_000);
        assert_eq!(parsed.1[0].height, 5500);
        assert_eq!(parsed.1[0].vout, 1);
    }

    #[test]
    fn wallet_utxo_snapshot_response_rejects_non_success_payloads() {
        let err = parse_wallet_utxo_snapshot_response(&json!({
            "ok": false,
            "error": "wallet_utxo_snapshot_failed",
            "detail": "invalid_address"
        }))
        .unwrap_err();
        assert!(err.contains("wallet_utxo_snapshot_failed"));
        assert!(err.contains("invalid_address"));
    }

    #[test]
    fn wallet_utxo_snapshot_retry_after_secs_extracts_rate_limit_hint() {
        assert_eq!(
            wallet_utxo_snapshot_retry_after_secs(&json!({
                "ok": false,
                "error": "rate_limited",
                "retry_after_secs": 1
            })),
            Some(1)
        );
        assert_eq!(
            wallet_utxo_snapshot_retry_after_secs(&json!({
                "ok": false,
                "error": "wallet_utxo_snapshot_failed"
            })),
            None
        );
    }

    #[test]
    fn net_from_name_accepts_known_networks_and_defaults_to_mainnet() {
        assert_eq!(net_from_name("mainnet"), Network::Mainnet);
        assert_eq!(net_from_name("testnet"), Network::Testnet);
        assert_eq!(net_from_name("stagenet"), Network::Stagenet);
        assert_eq!(net_from_name("unknown"), Network::Mainnet);
    }

    #[test]
    fn net_from_wallet_path_detects_network_from_path_segments() {
        assert_eq!(net_from_wallet_path("C:/wallets/testnet/dev.db"), Network::Testnet);
        assert_eq!(net_from_wallet_path("C:/wallets/stagenet/dev.db"), Network::Stagenet);
        assert_eq!(net_from_wallet_path("C:/wallets/main/dev.db"), Network::Mainnet);
    }

    #[test]
    fn wallet_public_name_keeps_filename_only() {
        assert_eq!(wallet_public_name("C:/wallets/testnet/dev.db"), "dev.db");
        assert_eq!(wallet_public_name("/root/wallets/primary.dat"), "primary.dat");
    }

    #[test]
    fn query_param_extracts_expected_values() {
        assert_eq!(
            query_param("/send?address=dut123&amount=50", "address"),
            Some("dut123".to_string())
        );
        assert_eq!(
            query_param("/send?address=dut123&amount=50", "amount"),
            Some("50".to_string())
        );
        assert_eq!(query_param("/send?address=dut123", "missing"), None);
    }

    #[test]
    fn wallet_state_network_prefers_primary_address() {
        let ws = super::super::WalletState {
            wallet_path: "C:/wallets/testnet/dev.db".to_string(),
            primary_address: "dut111111111111111111111111111111111111111111".to_string(),
            keys: BTreeMap::new(),
            pubkeys: BTreeMap::new(),
            utxos: Vec::new(),
            pending_txs: Vec::new(),
            reserved_inputs: Vec::new(),
            last_sync_height: 0,
            seed_hex: None,
            next_index: 0,
            is_db: true,
            locked: false,
            db_passphrase: None,
        };
        assert_eq!(wallet_state_network(&ws), Network::Mainnet);
    }

    #[test]
    fn wallet_state_network_falls_back_to_known_address_map() {
        let mut pubkeys = BTreeMap::new();
        pubkeys.insert(
            "test1111111111111111111111111111111111111111".to_string(),
            "22".repeat(32),
        );
        let ws = super::super::WalletState {
            wallet_path: "C:/wallets/main/dev.db".to_string(),
            primary_address: String::new(),
            keys: BTreeMap::new(),
            pubkeys,
            utxos: Vec::new(),
            pending_txs: Vec::new(),
            reserved_inputs: Vec::new(),
            last_sync_height: 0,
            seed_hex: None,
            next_index: 0,
            is_db: true,
            locked: false,
            db_passphrase: None,
        };
        assert_eq!(wallet_state_network(&ws), Network::Testnet);
    }

    #[test]
    fn wallet_state_network_falls_back_to_wallet_path() {
        let ws = super::super::WalletState {
            wallet_path: "C:/wallets/stagenet/dev.db".to_string(),
            primary_address: String::new(),
            keys: BTreeMap::new(),
            pubkeys: BTreeMap::new(),
            utxos: Vec::new(),
            pending_txs: Vec::new(),
            reserved_inputs: Vec::new(),
            last_sync_height: 0,
            seed_hex: None,
            next_index: 0,
            is_db: true,
            locked: false,
            db_passphrase: None,
        };
        assert_eq!(wallet_state_network(&ws), Network::Stagenet);
    }

    #[test]
    fn send_success_body_marks_persist_success() {
        let body = send_success_body("tx123", 50, 1, 9, 2, 123, Ok(()));
        assert_eq!(body.get("ok").and_then(|x| x.as_bool()), Some(true));
        assert_eq!(body.get("amount").and_then(|x| x.as_str()), Some("0.00000050"));
        assert_eq!(body.get("amount_dut").and_then(|x| x.as_i64()), Some(50));
        assert_eq!(body.get("fee").and_then(|x| x.as_str()), Some("0.00000001"));
        assert_eq!(body.get("fee_dut").and_then(|x| x.as_i64()), Some(1));
        assert_eq!(body.get("change").and_then(|x| x.as_str()), Some("0.00000009"));
        assert_eq!(body.get("change_dut").and_then(|x| x.as_i64()), Some(9));
        assert_eq!(body.get("unit").and_then(|x| x.as_str()), Some("DUTA"));
        assert_eq!(
            body.get("display_unit").and_then(|x| x.as_str()),
            Some("DUTA")
        );
        assert_eq!(body.get("base_unit").and_then(|x| x.as_str()), Some("dut"));
        assert_eq!(body.get("decimals").and_then(|x| x.as_u64()), Some(8));
        assert_eq!(
            body.get("wallet_state_persisted").and_then(|x| x.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn amount_detail_json_exposes_display_and_base_unit_metadata() {
        let body = super::amount_detail_json("send", "dut1dest", -10_001);
        assert_eq!(body.get("category").and_then(|x| x.as_str()), Some("send"));
        assert_eq!(body.get("address").and_then(|x| x.as_str()), Some("dut1dest"));
        assert_eq!(body.get("amount").and_then(|x| x.as_str()), Some("-0.00010001"));
        assert_eq!(body.get("amount_dut").and_then(|x| x.as_i64()), Some(-10_001));
        assert_eq!(body.get("unit").and_then(|x| x.as_str()), Some("DUTA"));
        assert_eq!(
            body.get("display_unit").and_then(|x| x.as_str()),
            Some("DUTA")
        );
        assert_eq!(body.get("base_unit").and_then(|x| x.as_str()), Some("dut"));
        assert_eq!(body.get("decimals").and_then(|x| x.as_u64()), Some(8));
    }

    #[test]
    fn normalize_history_details_backfills_display_and_raw_amount_fields() {
        let details = vec![json!({
            "category": "receive",
            "address": "dut1recv",
            "amount_dut": 1
        })];
        let normalized = super::normalize_history_details(&details);
        assert_eq!(normalized.len(), 1);
        let item = &normalized[0];
        assert_eq!(item.get("category").and_then(|x| x.as_str()), Some("receive"));
        assert_eq!(item.get("address").and_then(|x| x.as_str()), Some("dut1recv"));
        assert_eq!(item.get("amount").and_then(|x| x.as_str()), Some("0.00000001"));
        assert_eq!(item.get("amount_dut").and_then(|x| x.as_i64()), Some(1));
        assert_eq!(item.get("unit").and_then(|x| x.as_str()), Some("DUTA"));
        assert_eq!(
            item.get("display_unit").and_then(|x| x.as_str()),
            Some("DUTA")
        );
        assert_eq!(item.get("base_unit").and_then(|x| x.as_str()), Some("dut"));
        assert_eq!(item.get("decimals").and_then(|x| x.as_u64()), Some(8));
    }

    #[test]
    fn display_amount_parser_accepts_decimal_strings() {
        assert_eq!(
            super::parse_display_amount_value(&json!("1.25")).unwrap(),
            125_000_000
        );
        assert_eq!(
            super::parse_display_amount_value(&json!(2)).unwrap(),
            200_000_000
        );
    }

    #[test]
    fn optional_fee_parser_defaults_to_core_wallet_fee() {
        assert_eq!(
            super::parse_optional_fee_param(None, None).unwrap(),
            duta_core::amount::DEFAULT_WALLET_FEE_DUT
        );
        assert_eq!(
            super::parse_optional_fee_param(Some(&json!("0.0002")), None).unwrap(),
            20_000
        );
    }

    #[test]
    fn amount_parser_rejects_conflicting_display_and_raw_values() {
        assert_eq!(
            super::parse_required_amount_param(Some(&json!("1.0")), Some(1)).unwrap_err(),
            "amount_mismatch"
        );
        assert_eq!(
            super::parse_optional_fee_param(Some(&json!("0.0002")), Some(1)).unwrap_err(),
            "fee_mismatch"
        );
    }

    #[test]
    fn pending_mempool_visibility_only_required_when_pending_exists() {
        let pending: Vec<super::super::PendingTx> = Vec::new();
        assert!(!super::pending_mempool_visibility_required(&pending));
        let pending = vec![super::super::PendingTx {
            txid: "aa".repeat(32),
            category: "send".to_string(),
            amount: -2,
            fee: 1,
            change: 0,
            timestamp: 1,
            details: Vec::new(),
            spent_inputs: Vec::new(),
        }];
        assert!(super::pending_mempool_visibility_required(&pending));
    }

    #[test]
    fn daemon_utxo_probe_skips_unconfirmed_wallet_change() {
        let utxo = super::super::Utxo {
            value: 9,
            height: 0,
            coinbase: false,
            address: "dut1change".to_string(),
            txid: "ab".repeat(32),
            vout: 1,
        };
        assert!(!should_probe_daemon_utxo_presence(&utxo, 100));
    }

    #[test]
    fn daemon_utxo_probe_keeps_confirmed_outputs_eligible() {
        let utxo = super::super::Utxo {
            value: 9,
            height: 55,
            coinbase: false,
            address: "dut1confirmed".to_string(),
            txid: "cd".repeat(32),
            vout: 0,
        };
        assert!(should_probe_daemon_utxo_presence(&utxo, 100));
    }

    #[test]
    fn tx_output_address_accepts_newer_address_key() {
        assert_eq!(
            tx_output_address(&json!({"address":"dut1new","value":1})),
            "dut1new"
        );
    }

    #[test]
    fn tx_output_address_falls_back_to_legacy_addr_key() {
        assert_eq!(tx_output_address(&json!({"addr":"dut1old","value":1})), "dut1old");
    }

    #[test]
    fn pending_wallet_txs_are_appended_when_unconfirmed() {
        let merged = merge_pending_wallet_txs(
            vec![(
                "confirmed".to_string(),
                12,
                100,
                "receive".to_string(),
                5,
                0,
                false,
                Vec::new(),
            )],
            &[super::super::PendingTx {
                txid: "pending".to_string(),
                category: "send".to_string(),
                amount: -7,
                fee: 1,
                change: 2,
                timestamp: 200,
                details: vec![json!({"category":"send","address":"dut1dest","amount":-6})],
                spent_inputs: Vec::new(),
            }],
        );
        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|(txid, h, _, category, amount, fee, _, _)| {
            txid == "pending" && *h == 0 && category == "send" && *amount == 6 && *fee == 1
        }));
        assert_eq!(merged[0].0, "pending");
    }

    #[test]
    fn pending_wallet_txs_are_sorted_before_confirmed_history() {
        let merged = merge_pending_wallet_txs(
            vec![(
                "confirmed".to_string(),
                120,
                100,
                "receive".to_string(),
                46,
                0,
                false,
                Vec::new(),
            )],
            &[super::super::PendingTx {
                txid: "pending".to_string(),
                category: "send".to_string(),
                amount: -2,
                fee: 1,
                change: 44,
                timestamp: 200,
                details: vec![json!({"category":"send","address":"dut1dest","amount":-1})],
                spent_inputs: Vec::new(),
            }],
        );
        assert_eq!(merged[0].0, "pending");
        assert_eq!(merged[1].0, "confirmed");
    }

    #[test]
    fn pending_wallet_txs_are_hidden_after_confirmation() {
        let merged = merge_pending_wallet_txs(
            vec![(
                "same-txid".to_string(),
                12,
                100,
                "send".to_string(),
                6,
                1,
                false,
                Vec::new(),
            )],
            &[super::super::PendingTx {
                txid: "same-txid".to_string(),
                category: "send".to_string(),
                amount: -7,
                fee: 1,
                change: 2,
                timestamp: 200,
                details: Vec::new(),
                spent_inputs: Vec::new(),
            }],
        );
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn prune_confirmed_pending_txs_removes_confirmed_entries() {
        let mut pending = vec![
            super::super::PendingTx {
                txid: "confirmed".to_string(),
                category: "send".to_string(),
                amount: -7,
                fee: 1,
                change: 2,
                timestamp: 1,
                details: Vec::new(),
                spent_inputs: Vec::new(),
            },
            super::super::PendingTx {
                txid: "pending".to_string(),
                category: "send".to_string(),
                amount: -3,
                fee: 1,
                change: 0,
                timestamp: 2,
                details: Vec::new(),
                spent_inputs: Vec::new(),
            },
        ];
        let changed = prune_confirmed_pending_txs(
            &mut pending,
            &[(
                "confirmed".to_string(),
                10,
                100,
                "send".to_string(),
                6,
                1,
                false,
                Vec::new(),
            )],
        );
        assert!(changed);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].txid, "pending");
    }

    #[test]
    fn confirmed_pending_chain_probe_runs_even_when_txid_is_still_in_mempool() {
        let pending = vec![super::super::PendingTx {
            txid: "confirmed".to_string(),
            category: "send".to_string(),
            amount: -7,
            fee: 1,
            change: 2,
            timestamp: 1,
            details: Vec::new(),
            spent_inputs: Vec::new(),
        }];
        let wallet_addrs = vec!["duta123".to_string()];
        let mut mempool_txids = HashSet::new();
        mempool_txids.insert("confirmed".to_string());
        assert!(super::pending_chain_probe_required(
            &wallet_addrs,
            &pending,
            &mempool_txids,
        ));
    }

    #[test]
    fn pending_reserved_outpoints_collects_spent_inputs() {
        let reserved = super::pending_reserved_outpoints(&[super::super::PendingTx {
            txid: "pending".to_string(),
            category: "send".to_string(),
            amount: -5,
            fee: 1,
            change: 0,
            timestamp: 1,
            details: Vec::new(),
            spent_inputs: vec![
                super::super::PendingInput {
                    txid: "aa".repeat(32),
                    vout: 1,
                },
                super::super::PendingInput {
                    txid: "bb".repeat(32),
                    vout: 2,
                },
            ],
        }]);
        assert!(reserved.contains(&("aa".repeat(32), 1)));
        assert!(reserved.contains(&("bb".repeat(32), 2)));
        assert_eq!(reserved.len(), 2);
    }

    #[test]
    fn collect_mempool_spent_outpoints_reads_vins() {
        let spent = super::collect_mempool_spent_outpoints(&json!({
            "txids": ["tx1"],
            "txs": {
                "tx1": {
                    "vin": [
                        {"txid": "cc".repeat(32), "vout": 3},
                        {"txid": "dd".repeat(32), "vout": 4}
                    ]
                }
            }
        }));
        assert!(spent.contains(&("cc".repeat(32), 3)));
        assert!(spent.contains(&("dd".repeat(32), 4)));
        assert_eq!(spent.len(), 2);
    }

    #[test]
    fn selected_inputs_conflict_with_reserved_detects_pending_mempool_conflict() {
        let selected = vec![OwnedInput {
            utxo: super::super::Utxo {
                value: 46,
                height: 100,
                coinbase: false,
                address: "dut1owned".to_string(),
                txid: "aa".repeat(32),
                vout: 1,
            },
            signer: WalletSigner {
                addr: "dut1owned".to_string(),
                sk_hex: "11".repeat(32),
                pub_hex: "22".repeat(32),
            },
        }];
        let reserved = std::collections::HashSet::from([("aa".repeat(32), 1u32)]);
        let conflicts = super::selected_inputs_conflict_with_reserved(&selected, &reserved);
        assert_eq!(conflicts, vec![("aa".repeat(32), 1u32)]);
    }

    #[test]
    fn unconfirmed_change_is_not_spendable() {
        let unconfirmed_change = super::super::Utxo {
            value: 44,
            height: 0,
            coinbase: false,
            address: "dut1change".to_string(),
            txid: "ee".repeat(32),
            vout: 1,
        };
        assert!(!super::is_wallet_utxo_spendable(&unconfirmed_change, 250));
    }

    #[test]
    fn sequential_payouts_reserve_inputs_for_followup_send() {
        let mut pending = Vec::new();
        super::record_pending_send(
            &mut pending,
            "tx1",
            "dut1dest1",
            5,
            1,
            40,
            &[super::super::PendingInput {
                txid: "aa".repeat(32),
                vout: 0,
            }],
        );
        super::record_pending_send(
            &mut pending,
            "tx2",
            "dut1dest2",
            6,
            1,
            30,
            &[super::super::PendingInput {
                txid: "bb".repeat(32),
                vout: 1,
            }],
        );
        let reserved = super::pending_reserved_outpoints(&pending);
        assert!(reserved.contains(&("aa".repeat(32), 0)));
        assert!(reserved.contains(&("bb".repeat(32), 1)));
        assert_eq!(reserved.len(), 2);
    }

    #[test]
    fn stale_reserved_inputs_are_released_when_not_pending_or_in_mempool() {
        let mut reserved_inputs = vec![super::super::ReservedInput {
            txid: "aa".repeat(32),
            vout: 1,
            timestamp: 1,
        }];
        let utxos = vec![super::super::Utxo {
            value: 25,
            height: 100,
            coinbase: false,
            address: "dut1owned".to_string(),
            txid: "aa".repeat(32),
            vout: 1,
        }];
        let changed = super::prune_stale_reserved_inputs(
            &mut reserved_inputs,
            &utxos,
            &[],
            &std::collections::HashSet::new(),
            1 + 121,
        );
        assert!(changed);
        assert!(reserved_inputs.is_empty());
    }

    #[test]
    fn pending_balance_stats_track_reserved_and_pending_change() {
        let utxos = vec![
            super::super::Utxo {
                value: 70,
                height: 100,
                coinbase: false,
                address: "dut1owned".to_string(),
                txid: "aa".repeat(32),
                vout: 0,
            },
            super::super::Utxo {
                value: 11,
                height: 0,
                coinbase: false,
                address: "dut1change".to_string(),
                txid: "bb".repeat(32),
                vout: 1,
            },
        ];
        let pending = vec![super::super::PendingTx {
            txid: "pending".to_string(),
            category: "send".to_string(),
            amount: -9,
            fee: 1,
            change: 11,
            timestamp: 1,
            details: Vec::new(),
            spent_inputs: vec![super::super::PendingInput {
                txid: "aa".repeat(32),
                vout: 0,
            }],
        }];
        let reserved = std::collections::HashSet::from([("aa".repeat(32), 0u32)]);
        let stats = super::pending_balance_stats(&utxos, &reserved, &pending);
        assert_eq!(stats.reserved_dut, 70);
        assert_eq!(stats.pending_send_dut, 9);
        assert_eq!(stats.pending_change_dut, 11);
        assert_eq!(stats.pending_txs, 1);
    }

    #[test]
    fn insufficient_funds_body_exposes_operator_debug_fields() {
        let body = insufficient_funds_body(
            150_001,
            0,
            10_000,
            73,
            0,
            20,
            &PendingBalanceStats {
                reserved_dut: 0,
                pending_send_dut: 2_300_020,
                pending_change_dut: 91_997_699_980,
                pending_txs: 20,
            },
        );
        assert_eq!(body.get("error").and_then(|v| v.as_str()), Some("insufficient_funds"));
        assert_eq!(body.get("detail").and_then(|v| v.as_str()), Some("confirmed_spendable_utxos_exhausted_or_reserved"));
        assert_eq!(body.get("need").and_then(|v| v.as_str()), Some("0.00150001"));
        assert_eq!(body.get("need_dut").and_then(|v| v.as_i64()), Some(150_001));
        assert_eq!(body.get("have").and_then(|v| v.as_str()), Some("0.00000000"));
        assert_eq!(body.get("have_dut").and_then(|v| v.as_i64()), Some(0));
        assert_eq!(body.get("fee").and_then(|v| v.as_str()), Some("0.00010000"));
        assert_eq!(body.get("fee_dut").and_then(|v| v.as_i64()), Some(10_000));
        assert_eq!(body.get("spendable_utxos").and_then(|v| v.as_u64()), Some(0));
        assert_eq!(body.get("reserved_outpoints").and_then(|v| v.as_u64()), Some(20));
        assert_eq!(body.get("pending_send").and_then(|v| v.as_str()), Some("0.02300020"));
        assert_eq!(body.get("pending_send_dut").and_then(|v| v.as_i64()), Some(2_300_020));
        assert_eq!(body.get("pending_change").and_then(|v| v.as_str()), Some("919.97699980"));
        assert_eq!(body.get("pending_change_dut").and_then(|v| v.as_i64()), Some(91_997_699_980));
        assert_eq!(body.get("unit").and_then(|v| v.as_str()), Some("DUTA"));
        assert_eq!(body.get("base_unit").and_then(|v| v.as_str()), Some("dut"));
    }

    #[test]
    fn fee_error_bodies_expose_display_and_raw_amounts() {
        let low = super::fee_too_low_body(10_000, 40_000, 3567);
        assert_eq!(low.get("error").and_then(|v| v.as_str()), Some("fee_too_low"));
        assert_eq!(low.get("fee").and_then(|v| v.as_str()), Some("0.00010000"));
        assert_eq!(low.get("fee_dut").and_then(|v| v.as_i64()), Some(10_000));
        assert_eq!(low.get("min_fee").and_then(|v| v.as_str()), Some("0.00040000"));
        assert_eq!(low.get("min_fee_dut").and_then(|v| v.as_i64()), Some(40_000));
        assert_eq!(low.get("size").and_then(|v| v.as_u64()), Some(3567));
        assert_eq!(low.get("unit").and_then(|v| v.as_str()), Some("DUTA"));

        let high = super::fee_too_high_body(2_000_000_000, 1_000_000_000);
        assert_eq!(high.get("error").and_then(|v| v.as_str()), Some("fee_too_high"));
        assert_eq!(high.get("fee").and_then(|v| v.as_str()), Some("20.00000000"));
        assert_eq!(high.get("fee_dut").and_then(|v| v.as_i64()), Some(2_000_000_000));
        assert_eq!(high.get("max_fee").and_then(|v| v.as_str()), Some("10.00000000"));
        assert_eq!(high.get("max_fee_dut").and_then(|v| v.as_i64()), Some(1_000_000_000));
        assert_eq!(high.get("unit").and_then(|v| v.as_str()), Some("DUTA"));
    }

    #[test]
    fn too_many_inputs_body_exposes_display_and_raw_amounts() {
        let stats = PendingBalanceStats {
            reserved_dut: 45_000_000_000,
            pending_send_dut: 120_000,
            pending_change_dut: 89_000_000,
            pending_txs: 3,
        };
        let body = super::too_many_inputs_body(150_001, 50_000, 10_000, 77, 64, 4, 64, &stats);
        assert_eq!(body.get("error").and_then(|v| v.as_str()), Some("too_many_inputs"));
        assert_eq!(body.get("need").and_then(|v| v.as_str()), Some("0.00150001"));
        assert_eq!(body.get("need_dut").and_then(|v| v.as_i64()), Some(150_001));
        assert_eq!(body.get("have").and_then(|v| v.as_str()), Some("0.00050000"));
        assert_eq!(body.get("have_dut").and_then(|v| v.as_i64()), Some(50_000));
        assert_eq!(body.get("fee").and_then(|v| v.as_str()), Some("0.00010000"));
        assert_eq!(body.get("fee_dut").and_then(|v| v.as_i64()), Some(10_000));
        assert_eq!(body.get("pending_send").and_then(|v| v.as_str()), Some("0.00120000"));
        assert_eq!(body.get("pending_change").and_then(|v| v.as_str()), Some("0.89000000"));
        assert_eq!(body.get("reserved").and_then(|v| v.as_str()), Some("450.00000000"));
        assert_eq!(body.get("unit").and_then(|v| v.as_str()), Some("DUTA"));
    }

    #[test]
    fn wallet_display_amounts_keep_fixed_eight_decimals_while_raw_stays_intact() {
        let body = super::fee_too_high_body(50_000_000, 100_000_000);
        assert_eq!(body.get("fee").and_then(|v| v.as_str()), Some("0.50000000"));
        assert_eq!(body.get("fee_dut").and_then(|v| v.as_i64()), Some(50_000_000));
        assert_eq!(body.get("max_fee").and_then(|v| v.as_str()), Some("1.00000000"));
        assert_eq!(body.get("max_fee_dut").and_then(|v| v.as_i64()), Some(100_000_000));
    }

    #[test]
    fn select_inputs_for_need_prefers_exact_match() {
        let inputs = vec![
            OwnedInput {
                utxo: super::super::Utxo {
                    value: 70,
                    height: 100,
                    coinbase: false,
                    address: "dut1a".to_string(),
                    txid: "aa".repeat(32),
                    vout: 0,
                },
                signer: WalletSigner {
                    addr: "dut1a".to_string(),
                    sk_hex: "11".repeat(32),
                    pub_hex: "22".repeat(32),
                },
            },
            OwnedInput {
                utxo: super::super::Utxo {
                    value: 50,
                    height: 100,
                    coinbase: false,
                    address: "dut1b".to_string(),
                    txid: "bb".repeat(32),
                    vout: 0,
                },
                signer: WalletSigner {
                    addr: "dut1b".to_string(),
                    sk_hex: "33".repeat(32),
                    pub_hex: "44".repeat(32),
                },
            },
        ];
        let (selected, total) = select_inputs_for_need(&inputs, 50, MAX_WALLET_SEND_INPUTS);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].utxo.value, 50);
        assert_eq!(total, 50);
    }

    #[test]
    fn send_plan_counts_identical_payout_capacity() {
        let values = vec![4_600_000_000; 12];
        let plan = simulate_send_plan(&values, 20_001, MAX_WALLET_SEND_INPUTS, Some(20));
        assert_eq!(plan.max_outputs, 12);
        assert_eq!(plan.requested_outputs, Some(20));
        assert!(!plan.requested_outputs_fit);
        assert_eq!(plan.last_total_in, 0);
        assert_eq!(plan.selected_input_count_for_failure, 0);
    }

    #[test]
    fn send_plan_handles_multi_input_payouts() {
        let values = vec![30, 30, 30];
        let plan = simulate_send_plan(&values, 50, MAX_WALLET_SEND_INPUTS, Some(2));
        assert_eq!(plan.max_outputs, 1);
        assert!(!plan.requested_outputs_fit);
        assert_eq!(plan.last_total_in, 30);
        assert_eq!(plan.selected_input_count_for_failure, 1);
    }

    #[test]
    fn relay_fee_rounds_up_per_started_kilobyte() {
        assert_eq!(relay_fee_for_tx_bytes(1), 10_000);
        assert_eq!(relay_fee_for_tx_bytes(1000), 10_000);
        assert_eq!(relay_fee_for_tx_bytes(1001), 20_000);
        assert_eq!(relay_fee_for_tx_bytes(3501), 40_000);
    }

    #[test]
    fn sign_send_tx_estimates_large_batch_fee_above_default() {
        let signer = WalletSigner {
            addr: "test1111111111111111111111111111111111111111".to_string(),
            sk_hex: "11".repeat(32),
            pub_hex: "22".repeat(32),
        };
        let selected = vec![OwnedInput {
            utxo: super::super::Utxo {
                value: 5_000_000_000,
                height: 100,
                coinbase: false,
                address: signer.addr.clone(),
                txid: "aa".repeat(32),
                vout: 0,
            },
            signer: signer.clone(),
        }];
        let recipients: Vec<(String, i64)> = (0..24)
            .map(|idx| (format!("test{:040}", idx), 10_001))
            .collect();

        let (tx, final_fee, _final_change, _change_vout) =
            sign_send_tx(&selected, &recipients, &signer.addr, 10_000).expect("signed batch tx");
        let tx_size = serde_json::to_vec(&tx).expect("tx bytes").len();
        let min_fee = relay_fee_for_tx_bytes(tx_size);

        assert_eq!(final_fee, 10_000);
        assert!(min_fee > final_fee);
    }

    #[test]
    fn classify_wallet_history_tx_keeps_fee_on_internal_move() {
        let addr_set = HashSet::from(["test_wallet", "test_change"]);
        let tx = json!({
            "vin": [{"prev_addr":"test_wallet","txid":"aa","vout":0}],
            "vout": [
                {"address":"test_change","value": 4599989999i64},
                {"address":"test_wallet","value": 1i64}
            ],
            "fee": 10000i64
        });

        let entry = classify_wallet_history_tx(&addr_set, "tx1", 117, 12345, &tx)
            .expect("wallet tx should be classified");
        assert_eq!(entry.3, "move");
        assert_eq!(entry.4, 0);
        assert_eq!(entry.5, 10000);
        assert!(entry.7.iter().any(|detail| {
            detail.get("category").and_then(|x| x.as_str()) == Some("fee")
                && detail.get("amount_dut").and_then(|x| x.as_i64()) == Some(-10000)
        }));
    }

    #[test]
    fn sync_then_send_keeps_pending_inputs_reserved() {
        let mut pending = vec![super::super::PendingTx {
            txid: "tx-pending".to_string(),
            category: "send".to_string(),
            amount: -7,
            fee: 1,
            change: 0,
            timestamp: 100,
            details: Vec::new(),
            spent_inputs: vec![super::super::PendingInput {
                txid: "cc".repeat(32),
                vout: 2,
            }],
        }];
        let confirmed: Vec<(String, i64, i64, String, i64, i64, bool, Vec<serde_json::Value>)> =
            Vec::new();
        let changed = super::prune_confirmed_pending_txs(&mut pending, &confirmed);
        assert!(!changed);
        let reserved = super::pending_reserved_outpoints(&pending);
        assert!(reserved.contains(&("cc".repeat(32), 2)));
    }
}

fn net_from_name(net: &str) -> duta_core::netparams::Network {
    match net {
        "testnet" => duta_core::netparams::Network::Testnet,
        "stagenet" => duta_core::netparams::Network::Stagenet,
        _ => duta_core::netparams::Network::Mainnet,
    }
}

fn net_from_wallet_path(path: &str) -> duta_core::netparams::Network {
    let p = path.replace('\\', "/").to_ascii_lowercase();
    if p.ends_with("/testnet") || p.contains("/testnet/") {
        duta_core::netparams::Network::Testnet
    } else if p.ends_with("/stagenet") || p.contains("/stagenet/") {
        duta_core::netparams::Network::Stagenet
    } else {
        duta_core::netparams::Network::Mainnet
    }
}

fn wallet_state_network(ws: &super::WalletState) -> duta_core::netparams::Network {
    if !ws.primary_address.is_empty() {
        if let Some(net) = duta_core::address::detect_network(&ws.primary_address) {
            return net;
        }
    }
    if let Some(first_addr) = ws.pubkeys.keys().next().or_else(|| ws.keys.keys().next()) {
        if let Some(net) = duta_core::address::detect_network(first_addr) {
            return net;
        }
    }
    net_from_wallet_path(&ws.wallet_path)
}

fn wallet_public_name(path: &str) -> String {
    let normalized = path.replace('\\', "/");
    let name = std::path::Path::new(&normalized)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .trim();
    if name.is_empty() {
        "wallet".to_string()
    } else {
        name.to_string()
    }
}

fn db_wallet_path(path: &str) -> bool {
    path.ends_with(".db") || path.ends_with(".dat")
}

fn require_non_empty_passphrase(passphrase: &str) -> Result<(), String> {
    if passphrase.trim().is_empty() {
        return Err("missing_passphrase".to_string());
    }
    if passphrase.trim().len() < 12 {
        return Err("passphrase_too_short".to_string());
    }
    Ok(())
}

fn decode_seed_hex_for_migration(v: &serde_json::Value) -> Result<Vec<u8>, String> {
    let seed_hex = v
        .get("seed_hex")
        .and_then(|x| x.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "wallet_seed_missing".to_string())?;
    let seed = hex::decode(seed_hex).map_err(|_| "wallet_seed_invalid".to_string())?;
    if seed.is_empty() {
        return Err("wallet_seed_invalid".to_string());
    }
    Ok(seed)
}

fn respond_http_error(request: tiny_http::Request, status: tiny_http::StatusCode, error: &str) {
    super::respond_json(
        request,
        status,
        json!({"ok":false,"error":error}).to_string(),
    );
}

fn respond_http_error_detail(
    request: tiny_http::Request,
    status: tiny_http::StatusCode,
    error: &str,
    detail: impl Into<String>,
) {
    super::respond_json(
        request,
        status,
        json!({"ok":false,"error":error,"detail":detail.into()}).to_string(),
    );
}

fn respond_method_not_allowed(request: tiny_http::Request) {
    respond_http_error(request, tiny_http::StatusCode(405), "method_not_allowed");
}

fn respond_wallet_not_open(request: tiny_http::Request) {
    respond_http_error(request, tiny_http::StatusCode(400), "wallet_not_open");
}

fn secret_export_enabled() -> bool {
    matches!(
        std::env::var("DUTA_WALLET_ENABLE_SECRET_EXPORT")
            .ok()
            .as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

fn query_param(url: &str, key: &str) -> Option<String> {
    let q = url.splitn(2, '?').nth(1)?;
    for part in q.split('&') {
        if let Some((k, v)) = part.split_once('=') {
            if k == key {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn blocks_from_pruned_error(from: i64, prune_below: Option<i64>) -> String {
    match prune_below {
        Some(pb) => format!(
            "daemon_pruned_wallet_rescan_incomplete: from={} prune_below={}",
            from, pb
        ),
        None => format!("daemon_pruned_wallet_rescan_incomplete: from={}", from),
    }
}

fn parse_prune_below_from_blocks_from_error(detail: &str) -> Option<i64> {
    detail
        .split("prune_below=")
        .nth(1)
        .and_then(|tail| tail.split_whitespace().next())
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
}

fn advance_history_scan_from_pruned_boundary(from: i64, prune_below: Option<i64>) -> Option<i64> {
    let prune_below = prune_below?;
    if prune_below > from {
        Some(prune_below)
    } else {
        None
    }
}

fn advance_utxo_sync_from_pruned_boundary(
    current_utxos: &[super::Utxo],
    from: i64,
    prune_below: Option<i64>,
) -> Result<Option<i64>, String> {
    let Some(prune_below) = prune_below else {
        return Ok(None);
    };
    if prune_below <= from {
        return Ok(None);
    }
    if current_utxos.is_empty() {
        return Ok(Some(prune_below));
    }
    Err(format!(
        "wallet_pruned_history_gap: from={} prune_below={} tracked_utxos={} action=recover_with_unpruned_daemon_or_reopen_with_fresh_wallet_state",
        from,
        prune_below,
        current_utxos.len()
    ))
}

fn retry_from_pruned_boundary_for_empty_wallet(
    current_utxos: &[super::Utxo],
    detail: &str,
) -> Option<i64> {
    if !current_utxos.is_empty() {
        return None;
    }
    parse_prune_below_from_blocks_from_error(detail)
}

fn daemon_blocks_from_with_retry(
    daemon_rpc_port: u16,
    from: i64,
    limit: i64,
) -> Result<serde_json::Value, String> {
    for attempt in 0..3 {
        let path = format!("/blocks_from?from={}&limit={}", from, limit);
        let body = super::http_get_local("127.0.0.1", daemon_rpc_port, &path)?;
        let v: serde_json::Value =
            serde_json::from_str(&body).map_err(|e| format!("blocks_from_invalid_json: {}", e))?;

        let is_rate_limited =
            v.get("error").and_then(|x| x.as_str()) == Some("rate_limited");
        if is_rate_limited {
            let retry_secs = v
                .get("retry_after_secs")
                .and_then(|x| x.as_u64())
                .unwrap_or(1)
                .min(2);
            wwlog!(
                "wallet_rpc: blocks_from_rate_limited port={} from={} limit={} retry_after_secs={} attempt={}",
                daemon_rpc_port,
                from,
                limit,
                retry_secs,
                attempt + 1
            );
            std::thread::sleep(std::time::Duration::from_secs(retry_secs));
            continue;
        }

        return Ok(v);
    }

    Err(format!(
        "blocks_from_unavailable_after_retries: from={} limit={}",
        from, limit
    ))
}

fn parse_wallet_utxo_snapshot_response(
    value: &serde_json::Value,
) -> Result<(i64, Vec<super::Utxo>), String> {
    if value.get("ok").and_then(|x| x.as_bool()) != Some(true) {
        let error = value
            .get("error")
            .and_then(|x| x.as_str())
            .unwrap_or("wallet_utxo_snapshot_failed");
        let detail = value
            .get("detail")
            .and_then(|x| x.as_str())
            .unwrap_or("daemon_rejected_wallet_utxo_snapshot");
        return Err(format!("{error}: {detail}"));
    }

    let tip_height = value
        .get("tip_height")
        .and_then(|x| x.as_i64())
        .ok_or_else(|| "daemon_bad_response:missing_tip_height".to_string())?;
    let utxos_v = value
        .get("utxos")
        .and_then(|x| x.as_array())
        .ok_or_else(|| "daemon_bad_response:missing_utxos".to_string())?;
    let mut utxos = Vec::with_capacity(utxos_v.len());
    for entry in utxos_v {
        let txid = entry
            .get("txid")
            .and_then(|x| x.as_str())
            .ok_or_else(|| "daemon_bad_response:missing_txid".to_string())?;
        let vout = entry
            .get("vout")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| "daemon_bad_response:missing_vout".to_string())?;
        let amount_dut = entry
            .get("amount_dut")
            .and_then(|x| x.as_i64())
            .ok_or_else(|| "daemon_bad_response:missing_amount_dut".to_string())?;
        let height = entry
            .get("height")
            .and_then(|x| x.as_i64())
            .ok_or_else(|| "daemon_bad_response:missing_height".to_string())?;
        let coinbase = entry
            .get("coinbase")
            .and_then(|x| x.as_bool())
            .unwrap_or(false);
        let address = entry
            .get("address")
            .and_then(|x| x.as_str())
            .ok_or_else(|| "daemon_bad_response:missing_address".to_string())?;
        utxos.push(super::Utxo {
            value: amount_dut,
            height,
            coinbase,
            address: address.to_string(),
            txid: txid.to_string(),
            vout: vout as u32,
        });
    }
    utxos.sort_by(|a, b| (a.txid.clone(), a.vout).cmp(&(b.txid.clone(), b.vout)));
    Ok((tip_height, utxos))
}

fn wallet_utxo_snapshot_retry_after_secs(value: &serde_json::Value) -> Option<u64> {
    if value.get("error").and_then(|x| x.as_str()) == Some("rate_limited") {
        return value
            .get("retry_after_secs")
            .and_then(|x| x.as_u64())
            .map(|secs| secs.min(2).max(1));
    }
    None
}

fn rebuild_wallet_utxos_via_daemon_snapshot(
    addrs: &[String],
    daemon_rpc_port: u16,
) -> Result<(i64, Vec<super::Utxo>), String> {
    for attempt in 0..3 {
        let body = json!({ "addresses": addrs }).to_string();
        let resp = super::http_post_local(
            "127.0.0.1",
            daemon_rpc_port,
            "/wallet_utxos",
            "application/json",
            body.as_bytes(),
        )?;
        let value: serde_json::Value = serde_json::from_str(&resp)
            .map_err(|e| format!("wallet_utxo_snapshot_invalid_json: {}", e))?;
        if let Some(retry_secs) = wallet_utxo_snapshot_retry_after_secs(&value) {
            wwlog!(
                "wallet_rpc: wallet_utxo_snapshot_rate_limited port={} retry_after_secs={} attempt={}",
                daemon_rpc_port,
                retry_secs,
                attempt + 1
            );
            std::thread::sleep(std::time::Duration::from_secs(retry_secs));
            continue;
        }
        return parse_wallet_utxo_snapshot_response(&value);
    }

    Err("wallet_utxo_snapshot_unavailable_after_retries".to_string())
}

fn wallet_refresh_error_code(detail: &str) -> &'static str {
    if detail.starts_with("connect_failed:")
        || detail.starts_with("write_failed:")
        || detail.starts_with("read_failed:")
        || detail.starts_with("http_invalid:")
    {
        "daemon_unreachable"
    } else {
        "wallet_state_refresh_failed"
    }
}

fn send_success_body(
    txid: &str,
    amount_dut: i64,
    fee_dut: i64,
    change_dut: i64,
    inputs: usize,
    height: i64,
    persist_result: Result<(), String>,
) -> serde_json::Value {
    let mut body = json!({
        "ok": true,
        "txid": txid,
        "amount": format_dut_i64(amount_dut),
        "amount_dut": amount_dut,
        "fee": format_dut_i64(fee_dut),
        "fee_dut": fee_dut,
        "change": format_dut_i64(change_dut),
        "change_dut": change_dut,
        "inputs": inputs,
        "height": height,
        "wallet_state_persisted": persist_result.is_ok(),
        "unit": DISPLAY_UNIT,
        "display_unit": DISPLAY_UNIT,
        "base_unit": BASE_UNIT,
        "decimals": DUTA_DECIMALS
    });
    if let Err(e) = persist_result {
        body["wallet_state_persist_error"] = json!(e);
    }
    body
}

fn amount_json_value(amount_dut: i64) -> serde_json::Value {
    json!(format_dut_i64(amount_dut))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PendingBalanceStats {
    reserved_dut: i64,
    pending_send_dut: i64,
    pending_change_dut: i64,
    pending_txs: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SendPlanResult {
    max_outputs: usize,
    requested_outputs: Option<usize>,
    requested_outputs_fit: bool,
    last_total_in: i64,
    selected_input_count_for_failure: usize,
}

fn parse_display_amount_value(v: &serde_json::Value) -> Result<i64, String> {
    match v {
        serde_json::Value::String(s) => parse_duta_to_dut_i64(s),
        serde_json::Value::Number(n) => parse_duta_to_dut_i64(&n.to_string()),
        _ => Err("amount_invalid".to_string()),
    }
}

fn parse_required_amount_param(
    display_value: Option<&serde_json::Value>,
    raw_dut: Option<i64>,
) -> Result<i64, String> {
    if let Some(raw) = raw_dut {
        if let Some(display) = display_value {
            let parsed = parse_display_amount_value(display)?;
            if parsed != raw {
                return Err("amount_mismatch".to_string());
            }
        }
        return Ok(raw);
    }
    let value = display_value.ok_or_else(|| "missing_amount".to_string())?;
    parse_display_amount_value(value)
}

fn parse_optional_fee_param(
    display_value: Option<&serde_json::Value>,
    raw_dut: Option<i64>,
) -> Result<i64, String> {
    if let Some(raw) = raw_dut {
        if let Some(display) = display_value {
            let parsed = parse_display_amount_value(display)?;
            if parsed != raw {
                return Err("fee_mismatch".to_string());
            }
        }
        return Ok(raw);
    }
    match display_value {
        Some(value) => parse_display_amount_value(value),
        None => Ok(DEFAULT_WALLET_FEE_DUT),
    }
}

fn wallet_send_lock_or_recover() -> std::sync::MutexGuard<'static, ()> {
    match super::wallet_send_lock().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            wwlog!("wallet_rpc: mutex_poison_recovered name=wallet_send");
            poisoned.into_inner()
        }
    }
}

fn should_probe_daemon_utxo_presence(u: &super::Utxo, cur_h: i64) -> bool {
    if u.txid.is_empty() {
        return false;
    }
    if u.height <= 0 {
        return false;
    }
    u.height <= cur_h
}

fn is_wallet_utxo_spendable(u: &super::Utxo, cur_h: i64) -> bool {
    if u.height <= 0 {
        return false;
    }
    if u.coinbase {
        return (cur_h - u.height) >= 60;
    }
    true
}

fn tx_output_address(ov: &serde_json::Value) -> &str {
    ov.get("address")
        .and_then(|x| x.as_str())
        .or_else(|| ov.get("addr").and_then(|x| x.as_str()))
        .unwrap_or("")
}

fn normalized_history_amount(category: &str, amount: i64, fee: i64) -> i64 {
    if category == "send" {
        amount.abs().saturating_sub(fee.abs())
    } else {
        amount
    }
}

fn merge_pending_wallet_txs(
    mut confirmed: Vec<(String, i64, i64, String, i64, i64, bool, Vec<serde_json::Value>)>,
    pending: &[super::PendingTx],
) -> Vec<(String, i64, i64, String, i64, i64, bool, Vec<serde_json::Value>)> {
    let confirmed_ids: HashSet<String> = confirmed
        .iter()
        .map(|(txid, _, _, _, _, _, _, _)| txid.clone())
        .collect();
    for p in pending.iter() {
        if p.txid.is_empty() || confirmed_ids.contains(&p.txid) {
            continue;
        }
        confirmed.push((
            p.txid.clone(),
            0,
            p.timestamp,
            p.category.clone(),
            normalized_history_amount(&p.category, p.amount, p.fee),
            p.fee,
            false,
            p.details.clone(),
        ));
    }
    confirmed.sort_by(|a, b| {
        let a_pending = a.1 <= 0;
        let b_pending = b.1 <= 0;
        b_pending
            .cmp(&a_pending)
            .then_with(|| b.1.cmp(&a.1))
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.0.cmp(&b.0))
    });
    confirmed
}

fn prune_confirmed_pending_txs(
    pending: &mut Vec<super::PendingTx>,
    confirmed: &[(String, i64, i64, String, i64, i64, bool, Vec<serde_json::Value>)],
) -> bool {
    let confirmed_ids: HashSet<&str> = confirmed
        .iter()
        .map(|(txid, _, _, _, _, _, _, _)| txid.as_str())
        .collect();
    let before = pending.len();
    pending.retain(|p| p.txid.is_empty() || !confirmed_ids.contains(p.txid.as_str()));
    before != pending.len()
}

const PENDING_MEMPOOL_GRACE_SECS: i64 = 120;

fn pending_tx_is_recent(pending: &super::PendingTx, now_secs: i64) -> bool {
    pending.timestamp > 0 && now_secs.saturating_sub(pending.timestamp) <= PENDING_MEMPOOL_GRACE_SECS
}

fn pending_reserved_outpoints(pending: &[super::PendingTx]) -> HashSet<(String, u32)> {
    pending
        .iter()
        .flat_map(|p| p.spent_inputs.iter())
        .filter(|i| !i.txid.is_empty())
        .map(|i| (i.txid.clone(), i.vout))
        .collect()
}

fn explicit_reserved_outpoints(reserved_inputs: &[super::ReservedInput]) -> HashSet<(String, u32)> {
    reserved_inputs
        .iter()
        .filter(|i| !i.txid.is_empty())
        .map(|i| (i.txid.clone(), i.vout))
        .collect()
}

fn selected_inputs_conflict_with_reserved(
    selected: &[OwnedInput],
    reserved_outpoints: &HashSet<(String, u32)>,
) -> Vec<(String, u32)> {
    selected
        .iter()
        .filter_map(|input| {
            let key = (input.utxo.txid.clone(), input.utxo.vout);
            reserved_outpoints.contains(&key).then_some(key)
        })
        .collect()
}

fn append_selected_reserved_inputs(
    reserved_inputs: &mut Vec<super::ReservedInput>,
    selected: &[OwnedInput],
    now_secs: i64,
) {
    let mut existing = explicit_reserved_outpoints(reserved_inputs);
    for input in selected {
        let key = (input.utxo.txid.clone(), input.utxo.vout);
        if existing.insert(key.clone()) {
            reserved_inputs.push(super::ReservedInput {
                txid: key.0,
                vout: key.1,
                timestamp: now_secs,
            });
        }
    }
}

fn release_selected_reserved_inputs(
    reserved_inputs: &mut Vec<super::ReservedInput>,
    selected: &[OwnedInput],
) {
    let selected_keys: HashSet<(String, u32)> = selected
        .iter()
        .map(|input| (input.utxo.txid.clone(), input.utxo.vout))
        .collect();
    reserved_inputs.retain(|input| !selected_keys.contains(&(input.txid.clone(), input.vout)));
}

fn persist_runtime_reserved_inputs(
    wallet_path: &str,
    utxos: &[super::Utxo],
    cur_h: i64,
    reserved_inputs: &[super::ReservedInput],
) -> Result<(), String> {
    {
        let mut g = super::wallet_lock_or_recover();
        if let Some(ws) = g.as_mut() {
            ws.utxos = utxos.to_vec();
            ws.last_sync_height = cur_h;
            ws.reserved_inputs = reserved_inputs.to_vec();
        }
    }
    super::save_wallet_sync_state(wallet_path, utxos, cur_h, reserved_inputs)
}

fn prune_stale_reserved_inputs(
    reserved_inputs: &mut Vec<super::ReservedInput>,
    current_utxos: &[super::Utxo],
    pending_txs: &[super::PendingTx],
    mempool_reserved: &HashSet<(String, u32)>,
    now_secs: i64,
) -> bool {
    const RESERVED_INPUT_TTL_SECS: i64 = 120;
    let before = reserved_inputs.len();
    let pending_reserved = pending_reserved_outpoints(pending_txs);
    reserved_inputs.retain(|input| {
        if input.txid.is_empty() {
            return false;
        }
        let key = (input.txid.clone(), input.vout);
        if pending_reserved.contains(&key) {
            return false;
        }
        if mempool_reserved.contains(&key) {
            return true;
        }
        let still_present = current_utxos
            .iter()
            .any(|utxo| utxo.txid == input.txid && utxo.vout == input.vout);
        still_present && now_secs.saturating_sub(input.timestamp) <= RESERVED_INPUT_TTL_SECS
    });
    before != reserved_inputs.len()
}

fn collect_mempool_txids(v: &serde_json::Value) -> HashSet<String> {
    v.get("txids")
        .and_then(|x| x.as_array())
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect()
}

fn collect_mempool_spent_outpoints(v: &serde_json::Value) -> HashSet<(String, u32)> {
    let mut out = HashSet::new();
    let Some(txs) = v.get("txs").and_then(|x| x.as_object()) else {
        return out;
    };
    for tx in txs.values() {
        let Some(vins) = tx.get("vin").and_then(|x| x.as_array()) else {
            continue;
        };
        for vin in vins {
            let prev_txid = vin.get("txid").and_then(|x| x.as_str()).unwrap_or("");
            let prev_vout = vin.get("vout").and_then(|x| x.as_u64()).unwrap_or(u32::MAX as u64);
            if !prev_txid.is_empty() && prev_vout <= u32::MAX as u64 {
                out.insert((prev_txid.to_string(), prev_vout as u32));
            }
        }
    }
    out
}

fn daemon_mempool_state(
    daemon_rpc_port: u16,
) -> Result<(HashSet<String>, HashSet<(String, u32)>), String> {
    let body = super::http_get_local("127.0.0.1", daemon_rpc_port, "/mempool")
        .map_err(|e| format!("mempool_fetch_failed:{e}"))?;
    let v: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| format!("mempool_invalid_json:{e}"))?;
    Ok((collect_mempool_txids(&v), collect_mempool_spent_outpoints(&v)))
}

fn prune_inactive_pending_txs(
    pending: &mut Vec<super::PendingTx>,
    mempool_txids: &HashSet<String>,
    now_secs: i64,
) -> bool {
    let before = pending.len();
    pending.retain(|p| {
        if p.txid.is_empty() {
            return false;
        }
        mempool_txids.contains(&p.txid) || pending_tx_is_recent(p, now_secs)
    });
    before != pending.len()
}

fn pending_balance_stats(
    utxos: &[super::Utxo],
    reserved_outpoints: &HashSet<(String, u32)>,
    pending_txs: &[super::PendingTx],
) -> PendingBalanceStats {
    let reserved_dut = utxos
        .iter()
        .filter(|u| reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
        .map(|u| u.value)
        .sum();
    let pending_send_dut = pending_txs
        .iter()
        .filter(|p| p.category == "send" && p.amount < 0)
        .map(|p| -p.amount)
        .sum();
    let pending_change_dut = pending_txs
        .iter()
        .map(|p| p.change.max(0))
        .sum();
    PendingBalanceStats {
        reserved_dut,
        pending_send_dut,
        pending_change_dut,
        pending_txs: pending_txs.len(),
    }
}

fn insufficient_funds_body(
    need: i64,
    have: i64,
    fee: i64,
    height: i64,
    spendable_utxos: usize,
    reserved_outpoints: usize,
    pending_stats: &PendingBalanceStats,
) -> serde_json::Value {
    json!({
        "error":"insufficient_funds",
        "need": format_dut_i64(need),
        "need_dut": need,
        "have": format_dut_i64(have),
        "have_dut": have,
        "fee": format_dut_i64(fee),
        "fee_dut": fee,
        "height": height,
        "spendable_utxos": spendable_utxos,
        "reserved_outpoints": reserved_outpoints,
        "pending_send": format_dut_i64(pending_stats.pending_send_dut),
        "pending_send_dut": pending_stats.pending_send_dut,
        "pending_change": format_dut_i64(pending_stats.pending_change_dut),
        "pending_change_dut": pending_stats.pending_change_dut,
        "reserved": format_dut_i64(pending_stats.reserved_dut),
        "reserved_dut": pending_stats.reserved_dut,
        "detail":"confirmed_spendable_utxos_exhausted_or_reserved",
        "unit": DISPLAY_UNIT,
        "display_unit": DISPLAY_UNIT,
        "base_unit": BASE_UNIT,
        "decimals": DUTA_DECIMALS
    })
}

fn fee_too_low_body(fee: i64, min_fee: i64, size: usize) -> serde_json::Value {
    json!({
        "error":"fee_too_low",
        "fee": format_dut_i64(fee),
        "fee_dut": fee,
        "min_fee": format_dut_i64(min_fee),
        "min_fee_dut": min_fee,
        "size": size,
        "unit": DISPLAY_UNIT,
        "display_unit": DISPLAY_UNIT,
        "base_unit": BASE_UNIT,
        "decimals": DUTA_DECIMALS
    })
}

fn fee_too_high_body(fee: i64, max_fee: i64) -> serde_json::Value {
    json!({
        "error":"fee_too_high",
        "fee": format_dut_i64(fee),
        "fee_dut": fee,
        "max_fee": format_dut_i64(max_fee),
        "max_fee_dut": max_fee,
        "unit": DISPLAY_UNIT,
        "display_unit": DISPLAY_UNIT,
        "base_unit": BASE_UNIT,
        "decimals": DUTA_DECIMALS
    })
}

fn too_many_inputs_body(
    need: i64,
    have: i64,
    fee: i64,
    height: i64,
    spendable_utxos: usize,
    reserved_outpoints: usize,
    max_inputs: usize,
    pending_stats: &PendingBalanceStats,
) -> serde_json::Value {
    json!({
        "error":"too_many_inputs",
        "max_inputs": max_inputs,
        "need": format_dut_i64(need),
        "need_dut": need,
        "have": format_dut_i64(have),
        "have_dut": have,
        "fee": format_dut_i64(fee),
        "fee_dut": fee,
        "height": height,
        "spendable_utxos": spendable_utxos,
        "reserved_outpoints": reserved_outpoints,
        "pending_send": format_dut_i64(pending_stats.pending_send_dut),
        "pending_send_dut": pending_stats.pending_send_dut,
        "pending_change": format_dut_i64(pending_stats.pending_change_dut),
        "pending_change_dut": pending_stats.pending_change_dut,
        "reserved": format_dut_i64(pending_stats.reserved_dut),
        "reserved_dut": pending_stats.reserved_dut,
        "unit": DISPLAY_UNIT,
        "display_unit": DISPLAY_UNIT,
        "base_unit": BASE_UNIT,
        "decimals": DUTA_DECIMALS
    })
}

fn select_inputs_for_need(
    spendable_utxos: &[OwnedInput],
    need: i64,
    max_inputs: usize,
) -> (Vec<OwnedInput>, i64) {
    let mut selected = Vec::new();
    let mut total_in = 0;

    if let Some(u) = spendable_utxos.iter().find(|u| u.utxo.value == need).cloned() {
        selected.push(u.clone());
        total_in = u.utxo.value;
        return (selected, total_in);
    }

    let mut ordered = spendable_utxos.to_vec();
    ordered.sort_by(|a, b| b.utxo.value.cmp(&a.utxo.value));
    for u in ordered {
        if selected.len() >= max_inputs {
            break;
        }
        selected.push(u.clone());
        total_in += u.utxo.value;
        if total_in >= need {
            break;
        }
    }
    (selected, total_in)
}

fn simulate_send_plan(
    spendable_values: &[i64],
    need: i64,
    max_inputs: usize,
    requested_outputs: Option<usize>,
) -> SendPlanResult {
    let mut remaining: Vec<i64> = spendable_values
        .iter()
        .copied()
        .filter(|v| *v > 0)
        .collect();
    let mut max_outputs = 0usize;
    let target = requested_outputs.unwrap_or(usize::MAX);
    let mut last_total_in = 0i64;
    let mut selected_input_count_for_failure = 0usize;

    while max_outputs < target {
        if let Some(pos) = remaining.iter().position(|value| *value == need) {
            remaining.swap_remove(pos);
            max_outputs += 1;
            continue;
        }

        remaining.sort_unstable_by(|a, b| b.cmp(a));
        let mut selected_indexes = Vec::new();
        let mut total_in = 0i64;
        for (idx, value) in remaining.iter().enumerate() {
            if selected_indexes.len() >= max_inputs {
                break;
            }
            selected_indexes.push(idx);
            total_in += *value;
            if total_in >= need {
                break;
            }
        }
        if total_in < need {
            last_total_in = total_in;
            selected_input_count_for_failure = selected_indexes.len();
            break;
        }
        for idx in selected_indexes.into_iter().rev() {
            remaining.remove(idx);
        }
        max_outputs += 1;
    }

    SendPlanResult {
        max_outputs,
        requested_outputs,
        requested_outputs_fit: requested_outputs.map(|v| max_outputs >= v).unwrap_or(true),
        last_total_in,
        selected_input_count_for_failure,
    }
}

fn reconcile_pending_and_reserved_state(
    wallet_path: &str,
    utxos: &[super::Utxo],
    cur_h: i64,
    active_pending: &mut Vec<super::PendingTx>,
    reserved_inputs: &mut Vec<super::ReservedInput>,
    daemon_rpc_port: u16,
) -> Result<HashSet<(String, u32)>, String> {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mut reserved_outpoints = pending_reserved_outpoints(active_pending);
    reserved_outpoints.extend(explicit_reserved_outpoints(reserved_inputs));

    match pending_mempool_state_or_err(wallet_path, active_pending, daemon_rpc_port)? {
        Some((mempool_txids, mempool_reserved)) => {
            let mut state_changed = false;
            if prune_inactive_pending_txs(active_pending, &mempool_txids, now_secs) {
                state_changed = true;
            }
            if prune_stale_reserved_inputs(
                reserved_inputs,
                utxos,
                active_pending,
                &mempool_reserved,
                now_secs,
            ) {
                state_changed = true;
            }
            if state_changed {
                {
                    let mut g = super::wallet_lock_or_recover();
                    if let Some(ws) = g.as_mut() {
                        ws.pending_txs = active_pending.clone();
                        ws.reserved_inputs = reserved_inputs.clone();
                        ws.utxos = utxos.to_vec();
                        ws.last_sync_height = cur_h;
                    }
                }
                if let Err(e) = super::save_wallet_full_state(
                    wallet_path,
                    utxos,
                    cur_h,
                    active_pending,
                    reserved_inputs,
                ) {
                    wwlog!(
                        "wallet_rpc: pending_reserved_reconcile_persist_failed wallet={} err={}",
                        wallet_public_name(wallet_path),
                        e
                    );
                }
            }
            reserved_outpoints = pending_reserved_outpoints(active_pending);
            reserved_outpoints.extend(explicit_reserved_outpoints(reserved_inputs));
            reserved_outpoints.extend(mempool_reserved);
        }
        None => {}
    }

    Ok(reserved_outpoints)
}

fn refresh_wallet_utxos_after_submit_conflict(
    wallet_path: &str,
    addrs: &[String],
    daemon_rpc_port: u16,
    current_utxos: &[super::Utxo],
    last_sync_height: i64,
) -> Result<(), String> {
    if addrs.is_empty() {
        return Ok(());
    }
    let (cur_h, utxos) =
        refresh_wallet_utxos_runtime(addrs, daemon_rpc_port, current_utxos, last_sync_height)?;
    let reserved_inputs = {
        let g = super::wallet_lock_or_recover();
        g.as_ref()
            .map(|ws| ws.reserved_inputs.clone())
            .unwrap_or_default()
    };
    {
        let mut g = super::wallet_lock_or_recover();
        if let Some(ws) = g.as_mut() {
            ws.utxos = utxos.clone();
            ws.last_sync_height = cur_h;
        }
    }
    super::save_wallet_sync_state(wallet_path, &utxos, cur_h, &reserved_inputs)
}

fn sync_pending_txs_with_chain_and_mempool(
    wallet_path: &str,
    wallet_addrs: &[String],
    pending_txs: &mut Vec<super::PendingTx>,
    confirmed: &[(String, i64, i64, String, i64, i64, bool, Vec<serde_json::Value>)],
    daemon_rpc_port: u16,
) {
    let mut changed = prune_confirmed_pending_txs(pending_txs, confirmed);
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    if let Ok((mempool_txids, _)) = daemon_mempool_state(daemon_rpc_port) {
        let needs_chain_probe =
            pending_chain_probe_required(wallet_addrs, pending_txs, &mempool_txids);
        if needs_chain_probe {
            if let Ok((_cur_h, scanned_confirmed)) =
                scan_wallet_txs_via_blocks_from(wallet_addrs, daemon_rpc_port)
            {
                if prune_confirmed_pending_txs(pending_txs, &scanned_confirmed) {
                    changed = true;
                }
            }
        }
        if prune_inactive_pending_txs(pending_txs, &mempool_txids, now_secs) {
            changed = true;
        }
    }
    if changed {
        {
            let mut g = super::wallet_lock_or_recover();
            if let Some(ws) = g.as_mut() {
                ws.pending_txs = pending_txs.clone();
            }
        }
        if let Err(e) = super::save_wallet_pending_txs(wallet_path, pending_txs) {
            wwlog!("wallet_rpc: pending_txs_prune_persist_failed wallet={} err={}", wallet_public_name(wallet_path), e);
        }
    }
}

fn pending_chain_probe_required(
    wallet_addrs: &[String],
    pending_txs: &[super::PendingTx],
    _mempool_txids: &HashSet<String>,
) -> bool {
    !wallet_addrs.is_empty() && pending_txs.iter().any(|p| !p.txid.is_empty())
}

fn pending_mempool_state_or_err(
    wallet_path: &str,
    pending_txs: &[super::PendingTx],
    daemon_rpc_port: u16,
) -> Result<Option<(HashSet<String>, HashSet<(String, u32)>)>, String> {
    match daemon_mempool_state(daemon_rpc_port) {
        Ok(state) => Ok(Some(state)),
        Err(e) => {
            wwlog!(
                "wallet_rpc: mempool_reservation_probe_failed wallet={} err={}",
                wallet_public_name(wallet_path),
                e
            );
            if !pending_mempool_visibility_required(pending_txs) {
                Ok(None)
            } else {
                Err(format!("daemon_mempool_unreachable:{e}"))
            }
        }
    }
}

fn pending_mempool_visibility_required(pending_txs: &[super::PendingTx]) -> bool {
    !pending_txs.is_empty()
}

fn send_mempool_state_or_err(
    wallet_path: &str,
    daemon_rpc_port: u16,
) -> Result<(HashSet<String>, HashSet<(String, u32)>), String> {
    daemon_mempool_state(daemon_rpc_port).map_err(|e| {
        wwlog!(
            "wallet_rpc: mempool_send_probe_failed wallet={} err={}",
            wallet_public_name(wallet_path),
            e
        );
        format!("daemon_mempool_unreachable:{e}")
    })
}

fn record_pending_send(
    pending_txs: &mut Vec<super::PendingTx>,
    txid: &str,
    to_addr: &str,
    amount: i64,
    fee: i64,
    change: i64,
    spent_inputs: &[super::PendingInput],
) {
    if txid.is_empty() {
        return;
    }
    const MAX_PENDING_TXS: usize = 256;
    pending_txs.retain(|p| p.txid != txid);
    let mut details = vec![amount_detail_json("send", to_addr, -amount)];
    if fee > 0 {
        details.push(amount_detail_json("fee", "", -fee));
    }
    if change > 0 {
        details.push(amount_detail_json("receive", "change", change));
    }
    pending_txs.push(super::PendingTx {
        txid: txid.to_string(),
        category: "send".to_string(),
        amount: -(amount.saturating_add(fee)),
        fee,
        change,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64,
        details,
        spent_inputs: spent_inputs.to_vec(),
    });
    if pending_txs.len() > MAX_PENDING_TXS {
        let drop_n = pending_txs.len().saturating_sub(MAX_PENDING_TXS);
        pending_txs.drain(0..drop_n);
    }
}

fn record_pending_send_many(
    pending_txs: &mut Vec<super::PendingTx>,
    txid: &str,
    recipients: &[(String, i64)],
    fee: i64,
    change: i64,
    spent_inputs: &[super::PendingInput],
) {
    if txid.is_empty() {
        return;
    }
    const MAX_PENDING_TXS: usize = 256;
    pending_txs.retain(|p| p.txid != txid);
    let total_amount: i64 = recipients.iter().map(|(_, amount)| *amount).sum();
    let mut details: Vec<serde_json::Value> = recipients
        .iter()
        .map(|(to_addr, amount)| amount_detail_json("send", to_addr, -*amount))
        .collect();
    if fee > 0 {
        details.push(amount_detail_json("fee", "", -fee));
    }
    if change > 0 {
        details.push(amount_detail_json("receive", "change", change));
    }
    pending_txs.push(super::PendingTx {
        txid: txid.to_string(),
        category: "send".to_string(),
        amount: -(total_amount.saturating_add(fee)),
        fee,
        change,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64,
        details,
        spent_inputs: spent_inputs.to_vec(),
    });
    if pending_txs.len() > MAX_PENDING_TXS {
        let drop_n = pending_txs.len().saturating_sub(MAX_PENDING_TXS);
        pending_txs.drain(0..drop_n);
    }
}

fn amount_detail_json(category: &str, address: &str, amount_dut: i64) -> serde_json::Value {
    json!({
        "category": category,
        "address": address,
        "amount": format_dut_i64(amount_dut),
        "amount_dut": amount_dut,
        "unit": DISPLAY_UNIT,
        "display_unit": DISPLAY_UNIT,
        "base_unit": BASE_UNIT,
        "decimals": DUTA_DECIMALS
    })
}

fn normalize_history_details(details: &[serde_json::Value]) -> Vec<serde_json::Value> {
    details
        .iter()
        .map(|detail| {
            let category = detail.get("category").and_then(|x| x.as_str()).unwrap_or("tx");
            let address = detail.get("address").and_then(|x| x.as_str()).unwrap_or("");
            let amount_dut = detail.get("amount_dut").and_then(|x| x.as_i64()).or_else(|| {
                detail.get("amount").and_then(|x| x.as_i64())
            }).unwrap_or(0);
            let mut out = detail.clone();
            if let Some(obj) = out.as_object_mut() {
                obj.insert("amount".to_string(), json!(format_dut_i64(amount_dut)));
                obj.insert("amount_dut".to_string(), json!(amount_dut));
                obj.insert("unit".to_string(), json!(DISPLAY_UNIT));
                obj.insert("display_unit".to_string(), json!(DISPLAY_UNIT));
                obj.insert("base_unit".to_string(), json!(BASE_UNIT));
                obj.insert("decimals".to_string(), json!(DUTA_DECIMALS));
                if !address.is_empty() {
                    obj.insert("address".to_string(), json!(address));
                }
                obj.insert("category".to_string(), json!(category));
            }
            out
        })
        .collect()
}

fn rebuild_wallet_utxos_via_blocks_from(
    addrs: &[String],
    daemon_rpc_port: u16,
) -> Result<(i64, Vec<super::Utxo>), String> {
    sync_wallet_utxos_via_blocks_from(addrs, daemon_rpc_port, 0, &[])
}

fn rebuild_wallet_utxos_with_pruned_fallback(
    addrs: &[String],
    daemon_rpc_port: u16,
) -> Result<(i64, Vec<super::Utxo>), String> {
    match rebuild_wallet_utxos_via_daemon_snapshot(addrs, daemon_rpc_port) {
        Ok(snapshot) => Ok(snapshot),
        Err(snapshot_err)
            if snapshot_err.starts_with("connect_failed:")
                || snapshot_err.starts_with("write_failed:")
                || snapshot_err.starts_with("read_failed:")
                || snapshot_err.starts_with("http_invalid:") =>
        {
            Err(snapshot_err)
        }
        Err(_) => rebuild_wallet_utxos_via_blocks_from(addrs, daemon_rpc_port),
    }
}

fn sync_wallet_utxos_via_blocks_from(
    addrs: &[String],
    daemon_rpc_port: u16,
    from_height: i64,
    base_utxos: &[super::Utxo],
) -> Result<(i64, Vec<super::Utxo>), String> {
    let cur_h = daemon_tip_height_with_retry(daemon_rpc_port, 0)?;

    let addr_set: HashSet<&str> = addrs.iter().map(|s| s.as_str()).collect();
    if addr_set.is_empty() {
        return Ok((cur_h, Vec::new()));
    }

    let mut map: HashMap<(String, u32), super::Utxo> = base_utxos
        .iter()
        .cloned()
        .map(|u| ((u.txid.clone(), u.vout), u))
        .collect();
    let mut from: i64 = from_height.max(0);
    let limit: i64 = 256;

    loop {
        let v = daemon_blocks_from_with_retry(daemon_rpc_port, from, limit)?;
        // Daemon may return {"error":"chain_unavailable"} when polling beyond tip.
        // Treat that as empty result (no more blocks).
        if v.get("error").and_then(|x| x.as_str()) == Some("chain_unavailable") {
            break;
        }
        if v.get("error").and_then(|x| x.as_str()) == Some("pruned") {
            match advance_utxo_sync_from_pruned_boundary(
                base_utxos,
                from,
                v.get("prune_below").and_then(|x| x.as_i64()),
            )? {
                Some(next_from) => {
                    from = next_from;
                    continue;
                }
                None => {
                    return Err(blocks_from_pruned_error(
                        from,
                        v.get("prune_below").and_then(|x| x.as_i64()),
                    ));
                }
            }
        }

        let blocks = v
            .as_array()
            .ok_or_else(|| "blocks_from_not_array".to_string())?;

        if blocks.is_empty() {
            break;
        }

        for b in blocks.iter() {
            let bh = b.get("height").and_then(|x| x.as_i64()).unwrap_or(0);
            let txs = match b.get("txs") {
                Some(v) => v,
                None => continue,
            };
            let tx_map = match txs.as_object() {
                Some(m) => m,
                None => continue,
            };

            for (txid, txv) in tx_map.iter() {
                let vin = txv
                    .get("vin")
                    .and_then(|x| x.as_array())
                    .cloned()
                    .unwrap_or_default();
                let vout = txv
                    .get("vout")
                    .and_then(|x| x.as_array())
                    .cloned()
                    .unwrap_or_default();

                for inv in vin.iter() {
                    let ptxid = inv
                        .get("txid")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let pvout = inv.get("vout").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
                    if !ptxid.is_empty() {
                        map.remove(&(ptxid, pvout));
                    }
                }

                let is_coinbase = vin.is_empty()
                    || (vin.len() == 1
                        && vin[0]
                            .get("txid")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .is_empty());

                for (i, ov) in vout.iter().enumerate() {
                    let oaddr = tx_output_address(ov);
                    if !addr_set.contains(oaddr) {
                        continue;
                    }
                    let val = ov.get("value").and_then(|x| x.as_i64()).unwrap_or(0);
                    if val <= 0 {
                        continue;
                    }
                    let u = super::Utxo {
                        value: val,
                        height: bh,
                        coinbase: is_coinbase,
                        address: oaddr.to_string(),
                        txid: txid.clone(),
                        vout: i as u32,
                    };
                    map.insert((txid.clone(), i as u32), u);
                }
            }

            if bh >= from {
                from = bh + 1;
            }
        }
    }

    let mut utxos: Vec<super::Utxo> = map.values().cloned().collect();
    utxos.sort_by(|a, b| (a.txid.clone(), a.vout).cmp(&(b.txid.clone(), b.vout)));
    Ok((cur_h, utxos))
}

fn wallet_needs_full_utxo_rebuild(utxos: &[super::Utxo], cur_h: i64, last_sync_height: i64) -> bool {
    if cur_h <= 0 {
        return false;
    }
    if last_sync_height <= 0 {
        return true;
    }
    utxos.iter().any(|u| u.height > cur_h)
}

fn refresh_wallet_utxos_runtime(
    addrs: &[String],
    daemon_rpc_port: u16,
    current_utxos: &[super::Utxo],
    last_sync_height: i64,
) -> Result<(i64, Vec<super::Utxo>), String> {
    let cur_h = daemon_tip_height_with_retry(daemon_rpc_port, last_sync_height)?;
    if addrs.is_empty() || cur_h <= 0 {
        return Ok((cur_h, current_utxos.to_vec()));
    }

    let full_rebuild = wallet_needs_full_utxo_rebuild(current_utxos, cur_h, last_sync_height);
    if full_rebuild {
        match rebuild_wallet_utxos_with_pruned_fallback(addrs, daemon_rpc_port) {
            Ok(v) => return Ok(v),
            Err(e) => {
                if let Some(prune_below) =
                    retry_from_pruned_boundary_for_empty_wallet(current_utxos, &e)
                {
                    return sync_wallet_utxos_via_blocks_from(
                        addrs,
                        daemon_rpc_port,
                        prune_below,
                        current_utxos,
                    );
                }
                return Err(e);
            }
        }
    }

    if cur_h > last_sync_height {
        return match sync_wallet_utxos_via_blocks_from(
            addrs,
            daemon_rpc_port,
            last_sync_height.saturating_add(1),
            current_utxos,
        ) {
            Ok(v) => Ok(v),
            Err(e)
                if e.starts_with("wallet_pruned_history_gap:")
                    || e.starts_with("daemon_pruned_wallet_rescan_incomplete:") =>
            {
                rebuild_wallet_utxos_with_pruned_fallback(addrs, daemon_rpc_port).or(Err(e))
            }
            Err(e) => Err(e),
        };
    }

    Ok((cur_h, current_utxos.to_vec()))
}

fn classify_wallet_history_tx(
    addr_set: &HashSet<&str>,
    txid: &str,
    height: i64,
    block_time: i64,
    txv: &serde_json::Value,
) -> Option<(String, i64, i64, String, i64, i64, bool, Vec<serde_json::Value>)> {
    let vin = txv
        .get("vin")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let vout = txv
        .get("vout")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    let is_coinbase = vin.is_empty()
        || (vin.len() == 1
            && vin[0]
                .get("txid")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .is_empty());

    let mut recv_total: i64 = 0;
    let mut recv_details: Vec<serde_json::Value> = Vec::new();
    let mut external_total: i64 = 0;
    let mut send_details: Vec<serde_json::Value> = Vec::new();
    for ov in vout.iter() {
        let oaddr = tx_output_address(ov);
        let val = ov.get("value").and_then(|x| x.as_i64()).unwrap_or(0);
        if val <= 0 {
            continue;
        }
        if addr_set.contains(oaddr) {
            recv_total = recv_total.saturating_add(val);
            recv_details.push(amount_detail_json("receive", oaddr, val));
        } else {
            external_total = external_total.saturating_add(val);
            send_details.push(amount_detail_json("send", oaddr, -val));
        }
    }

    let wallet_input = vin.iter().any(|iv| {
        let prev_addr = iv.get("prev_addr").and_then(|x| x.as_str()).unwrap_or("");
        addr_set.contains(prev_addr)
    });

    if !(wallet_input || recv_total > 0) {
        return None;
    }

    let fee = txv.get("fee").and_then(|x| x.as_i64()).unwrap_or(0);
    let (category, amount, fee_amount, details) = if wallet_input {
        if external_total > 0 {
            if fee > 0 {
                send_details.push(amount_detail_json("fee", "", -fee));
            }
            ("send".to_string(), external_total, fee, send_details)
        } else {
            let mut move_details = recv_details;
            if fee > 0 {
                move_details.push(amount_detail_json("fee", "", -fee));
            }
            ("move".to_string(), 0, fee, move_details)
        }
    } else {
        ("receive".to_string(), recv_total, 0, recv_details)
    };

    Some((
        txid.to_string(),
        height,
        block_time,
        category,
        amount,
        fee_amount,
        is_coinbase,
        details,
    ))
}

fn scan_wallet_txs_via_blocks_from(
    addrs: &[String],
    daemon_rpc_port: u16,
) -> Result<
    (
        i64,
        Vec<(String, i64, i64, String, i64, i64, bool, Vec<serde_json::Value>)>,
    ),
    String,
> {
    let addr_set: HashSet<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let cur_h = daemon_tip_height_with_retry(daemon_rpc_port, 0)?;

    let mut out: Vec<(String, i64, i64, String, i64, i64, bool, Vec<serde_json::Value>)> =
        Vec::new();
    let mut from: i64 = 0;
    let limit: i64 = 256;

    loop {
        let v = daemon_blocks_from_with_retry(daemon_rpc_port, from, limit)?;
        if v.get("error").and_then(|x| x.as_str()) == Some("chain_unavailable") {
            break;
        }
        if v.get("error").and_then(|x| x.as_str()) == Some("pruned") {
            if let Some(next_from) = advance_history_scan_from_pruned_boundary(
                from,
                v.get("prune_below").and_then(|x| x.as_i64()),
            ) {
                from = next_from;
                continue;
            }
            return Err(blocks_from_pruned_error(
                from,
                v.get("prune_below").and_then(|x| x.as_i64()),
            ));
        }

        let blocks = v
            .as_array()
            .ok_or_else(|| "blocks_from_not_array".to_string())?;

        if blocks.is_empty() {
            break;
        }

        for b in blocks.iter() {
            let bh = b.get("height").and_then(|x| x.as_i64()).unwrap_or(0);
            let block_time = b.get("timestamp").and_then(|x| x.as_i64()).unwrap_or(0);
            let txs = match b.get("txs") {
                Some(v) => v,
                None => continue,
            };
            let tx_map = match txs.as_object() {
                Some(m) => m,
                None => continue,
            };

            for (txid, txv) in tx_map.iter() {
                if let Some(entry) = classify_wallet_history_tx(&addr_set, txid, bh, block_time, txv)
                {
                    out.push(entry);
                }
            }

            if bh >= from {
                from = bh + 1;
            }
        }
    }

    out.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.0.cmp(&b.0))
    });
    Ok((cur_h, out))
}

fn new_wallet_keypair_from_entropy(
    net: duta_core::netparams::Network,
    entropy32: [u8; 32],
) -> (String, String, String) {
    use ed25519_dalek::SigningKey;

    let sk = SigningKey::from_bytes(&entropy32);
    let pk = sk.verifying_key().to_bytes();
    let addr = duta_core::address::pkh_to_address_for_network(
        net,
        &duta_core::address::pkh_from_pubkey(&pk),
    );
    (addr, hex::encode(entropy32), hex::encode(pk))
}

fn new_wallet_keypair_random(net: duta_core::netparams::Network) -> (String, String, String) {
    use rand::RngCore;

    let mut ent = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut ent);
    new_wallet_keypair_from_entropy(net, ent)
}

fn new_wallet_keypair_from_seed(
    net: duta_core::netparams::Network,
    seed64: &[u8],
    index: u32,
) -> (String, String, String) {
    // Deterministic derivation (mnemonic -> seed -> per-index key) for Duta.
    // NOTE: This is NOT Bitcoin BIP32/BIP44; it's a simple deterministic scheme
    // that yields stable keys from the wallet seed.
    let mut data = Vec::with_capacity(seed64.len() + 4);
    data.extend_from_slice(seed64);
    data.extend_from_slice(&index.to_be_bytes());
    let duta_core::types::H32(h) = duta_core::hash::sha3_256(&data);
    let mut ent = [0u8; 32];
    ent.copy_from_slice(&h);
    new_wallet_keypair_from_entropy(net, ent)
}

/// Deterministic JSON encoding for consensus hashing (txid/merkle).
/// Objects: keys sorted. Arrays preserve order. Floats rejected.
fn canonical_json_bytes(v: &serde_json::Value) -> Result<Vec<u8>, String> {
    fn write_canon(v: &serde_json::Value, out: &mut Vec<u8>) -> Result<(), String> {
        match v {
            serde_json::Value::Null => out.extend_from_slice(b"null"),
            serde_json::Value::Bool(b) => {
                if *b {
                    out.extend_from_slice(b"true")
                } else {
                    out.extend_from_slice(b"false")
                }
            }
            serde_json::Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    out.extend_from_slice(u.to_string().as_bytes());
                } else if let Some(i) = n.as_i64() {
                    out.extend_from_slice(i.to_string().as_bytes());
                } else {
                    return Err("float_not_allowed".to_string());
                }
            }
            serde_json::Value::String(s) => {
                let q =
                    serde_json::to_string(s).map_err(|e| format!("json_string_failed: {}", e))?;
                out.extend_from_slice(q.as_bytes());
            }
            serde_json::Value::Array(arr) => {
                out.push(b'[');
                for (idx, item) in arr.iter().enumerate() {
                    if idx != 0 {
                        out.push(b',');
                    }
                    write_canon(item, out)?;
                }
                out.push(b']');
            }
            serde_json::Value::Object(obj) => {
                out.push(b'{');
                let mut keys: Vec<&String> = obj.keys().collect();
                keys.sort();
                for (idx, k) in keys.iter().enumerate() {
                    if idx != 0 {
                        out.push(b',');
                    }
                    let qk =
                        serde_json::to_string(k).map_err(|e| format!("json_key_failed: {}", e))?;
                    out.extend_from_slice(qk.as_bytes());
                    out.push(b':');
                    write_canon(&obj[*k], out)?;
                }
                out.push(b'}');
            }
        }
        Ok(())
    }

    let mut out = Vec::new();
    write_canon(v, &mut out)?;
    Ok(out)
}

/// Sighash preimage: tx with each vin's "sig" and "pubkey" stripped.
/// Canonical JSON bytes are hashed with SHA3-256.
fn sighash(txv: &serde_json::Value) -> Result<[u8; 32], String> {
    let mut t = txv.clone();
    if let Some(vins) = t.get_mut("vin").and_then(|x| x.as_array_mut()) {
        for vin in vins.iter_mut() {
            if let Some(o) = vin.as_object_mut() {
                o.remove("sig");
                o.remove("pubkey");
            }
        }
    }
    let b = canonical_json_bytes(&t)?;
    Ok(duta_core::hash::sha3_256(&b).0)
}

fn relay_fee_for_tx_bytes(tx_bytes: usize) -> i64 {
    let kb = (tx_bytes.saturating_add(999) / 1000).max(1);
    i64::try_from((kb as u64).saturating_mul(DEFAULT_MIN_RELAY_FEE_PER_KB_DUT))
        .unwrap_or(i64::MAX)
}

#[derive(serde::Deserialize)]
struct SendRequest {
    to: String,
    #[serde(default)]
    amount: Option<serde_json::Value>,
    #[serde(default)]
    amount_dut: Option<i64>,
    #[serde(default)]
    fee: Option<serde_json::Value>,
    #[serde(default)]
    fee_dut: Option<i64>,
}

#[derive(serde::Deserialize, Clone)]
struct SendManyRecipient {
    to: String,
    #[serde(default)]
    amount: Option<serde_json::Value>,
    #[serde(default)]
    amount_dut: Option<i64>,
}

#[derive(serde::Deserialize)]
struct SendManyRequest {
    outputs: Vec<SendManyRecipient>,
    #[serde(default)]
    fee: Option<serde_json::Value>,
    #[serde(default)]
    fee_dut: Option<i64>,
}

#[derive(serde::Deserialize)]
struct SendManyPlanRequest {
    outputs: Vec<SendManyRecipient>,
    #[serde(default)]
    fee: Option<serde_json::Value>,
    #[serde(default)]
    fee_dut: Option<i64>,
}

#[derive(serde::Deserialize)]
struct SendPlanRequest {
    #[serde(default)]
    amount: Option<serde_json::Value>,
    #[serde(default)]
    amount_dut: Option<i64>,
    #[serde(default)]
    fee: Option<serde_json::Value>,
    #[serde(default)]
    fee_dut: Option<i64>,
    #[serde(default)]
    outputs: Option<usize>,
}

#[derive(Clone)]
struct WalletSigner {
    addr: String,
    sk_hex: String,
    pub_hex: String,
}

#[derive(Clone)]
struct OwnedInput {
    utxo: super::Utxo,
    signer: WalletSigner,
}

fn sign_send_tx(
    selected: &[OwnedInput],
    recipients: &[(String, i64)],
    change_addr: &str,
    fee: i64,
) -> Result<(serde_json::Value, i64, i64, u32), String> {
    let total_in: i64 = selected.iter().map(|u| u.utxo.value).sum();
    let total_out: i64 = recipients.iter().map(|(_, amount)| *amount).sum();
    let need = total_out
        .checked_add(fee)
        .ok_or_else(|| "amount_overflow".to_string())?;
    if total_in < need {
        return Err("insufficient_inputs".to_string());
    }

    let mut final_fee = fee;
    let mut final_change = total_in - need;
    if final_change > 0 && final_change <= DEFAULT_DUST_CHANGE_DUT {
        final_fee = final_fee
            .checked_add(final_change)
            .ok_or_else(|| "fee_overflow".to_string())?;
        final_change = 0;
    }

    let vin: Vec<serde_json::Value> = selected
        .iter()
        .map(|u| {
            json!({
                "txid": u.utxo.txid,
                "vout": u.utxo.vout,
                "pubkey": "",
                "sig": "",
                "prev_addr": u.signer.addr,
            })
        })
        .collect();
    let mut vout: Vec<serde_json::Value> = recipients
        .iter()
        .map(|(to_addr, amount)| json!({"addr": to_addr, "address": to_addr, "value": amount}))
        .collect();
    let change_vout = vout.len() as u32;
    if final_change > 0 {
        vout.push(json!({"addr": change_addr, "address": change_addr, "value": final_change}));
    }
    let mut tx = json!({"vin": vin, "vout": vout, "fee": final_fee});
    let msg = sighash(&tx)?;
    if let Some(vins) = tx.get_mut("vin").and_then(|x| x.as_array_mut()) {
        for (vin, input) in vins.iter_mut().zip(selected.iter()) {
            let sk_b = hex::decode(&input.signer.sk_hex)
                .map_err(|_| "wallet_key_invalid:sk_hex".to_string())?;
            if sk_b.len() != 32 {
                return Err("wallet_key_invalid:sk_len".to_string());
            }
            let mut ent = [0u8; 32];
            ent.copy_from_slice(&sk_b);
            let sk = ed25519_dalek::SigningKey::from_bytes(&ent);
            let sig = ed25519_dalek::Signer::sign(&sk, &msg);
            if let Some(o) = vin.as_object_mut() {
                o.insert(
                    "pubkey".to_string(),
                    serde_json::Value::String(input.signer.pub_hex.clone()),
                );
                o.insert(
                    "sig".to_string(),
                    serde_json::Value::String(hex::encode(sig.to_bytes())),
                );
            }
        }
    }
    Ok((tx, final_fee, final_change, change_vout))
}

fn parse_sendmany_recipients(
    outputs: &[SendManyRecipient],
    net: &str,
) -> Result<(Vec<(String, i64)>, i64), (String, String)> {
    if outputs.is_empty() {
        return Err(("missing_outputs".to_string(), String::new()));
    }
    let mut recipients: Vec<(String, i64)> = Vec::with_capacity(outputs.len());
    let mut total_amount = 0i64;
    for output in outputs.iter() {
        let to = output.to.trim();
        if duta_core::address::parse_address_for_network(net_from_name(net), to).is_none() {
            return Err(("invalid_address".to_string(), "outputs.to".to_string()));
        }
        let amount = parse_required_amount_param(output.amount.as_ref(), output.amount_dut)
            .map_err(|e| ("invalid_amount".to_string(), e))?;
        if amount <= 0 {
            return Err((
                "invalid_amount".to_string(),
                "amount_must_be_positive".to_string(),
            ));
        }
        total_amount = total_amount
            .checked_add(amount)
            .ok_or_else(|| ("bad_request".to_string(), "amount_overflow".to_string()))?;
        recipients.push((to.to_string(), amount));
    }
    Ok((recipients, total_amount))
}

fn wallet_signers_by_pkh(
    ws: &super::WalletState,
) -> Result<HashMap<String, WalletSigner>, String> {
    let mut out = HashMap::new();
    let addrs: Vec<String> = if !ws.pubkeys.is_empty() {
        ws.pubkeys.keys().cloned().collect()
    } else {
        ws.keys.keys().cloned().collect()
    };
    for addr in addrs {
        let sk_hex = ws
            .keys
            .get(&addr)
            .cloned()
            .ok_or_else(|| format!("wallet_key_missing:{addr}"))?;
        let sk_b = hex::decode(&sk_hex).map_err(|_| format!("wallet_key_invalid:{addr}"))?;
        if sk_b.len() != 32 {
            return Err(format!("wallet_key_invalid:{addr}"));
        }
        let mut ent = [0u8; 32];
        ent.copy_from_slice(&sk_b);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&ent);
        let derived_pub_hex = hex::encode(signing_key.verifying_key().to_bytes());
        let pub_hex = ws
            .pubkeys
            .get(&addr)
            .cloned()
            .unwrap_or(derived_pub_hex);
        let pub_b = hex::decode(&pub_hex).map_err(|_| format!("wallet_pubkey_invalid:{addr}"))?;
        let pkh_hex = duta_core::address::pkh_to_hex(&duta_core::address::pkh_from_pubkey(&pub_b));
        out.insert(
            pkh_hex,
            WalletSigner {
                addr,
                sk_hex,
                pub_hex,
            },
        );
    }
    if out.is_empty() {
        return Err("wallet_no_signers".to_string());
    }
    Ok(out)
}

fn wallet_signers_by_addr(
    ws: &super::WalletState,
) -> Result<HashMap<String, WalletSigner>, String> {
    let mut out = HashMap::new();
    let addrs: Vec<String> = if !ws.pubkeys.is_empty() {
        ws.pubkeys.keys().cloned().collect()
    } else {
        ws.keys.keys().cloned().collect()
    };
    for addr in addrs {
        let sk_hex = ws
            .keys
            .get(&addr)
            .cloned()
            .ok_or_else(|| format!("wallet_key_missing:{addr}"))?;
        let sk_b = hex::decode(&sk_hex).map_err(|_| format!("wallet_key_invalid:{addr}"))?;
        if sk_b.len() != 32 {
            return Err(format!("wallet_key_invalid:{addr}"));
        }
        let mut ent = [0u8; 32];
        ent.copy_from_slice(&sk_b);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&ent);
        let derived_pub_hex = hex::encode(signing_key.verifying_key().to_bytes());
        let pub_hex = ws
            .pubkeys
            .get(&addr)
            .cloned()
            .unwrap_or(derived_pub_hex);
        out.insert(
            addr.clone(),
            WalletSigner {
                addr,
                sk_hex,
                pub_hex,
            },
        );
    }
    if out.is_empty() {
        return Err("wallet_no_signers".to_string());
    }
    Ok(out)
}

fn resolve_owned_input(
    daemon_rpc_port: u16,
    utxo: &super::Utxo,
    signers_by_pkh: &HashMap<String, WalletSigner>,
    signers_by_addr: &HashMap<String, WalletSigner>,
) -> Result<Option<OwnedInput>, String> {
    if !utxo.address.is_empty() {
        if let Some(signer) = signers_by_addr.get(&utxo.address).cloned() {
            return Ok(Some(OwnedInput {
                utxo: utxo.clone(),
                signer,
            }));
        }
    }
    let path = format!("/utxo?txid={}&vout={}", utxo.txid, utxo.vout);
    let body = super::http_get_local("127.0.0.1", daemon_rpc_port, &path)
        .map_err(|e| format!("daemon_unreachable:{e}"))?;
    let v: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| format!("daemon_bad_response:{e}"))?;
    if v.get("found").and_then(|x| x.as_bool()) != Some(true) {
        return Ok(None);
    }
    let pkh = v
        .get("pkh")
        .and_then(|x| x.as_str())
        .map(str::to_string)
        .ok_or_else(|| "daemon_bad_response:missing_pkh".to_string())?;
    Ok(signers_by_pkh.get(&pkh).cloned().map(|signer| OwnedInput {
        utxo: utxo.clone(),
        signer,
    }))
}

fn rpc_response_ok(id: serde_json::Value, result: serde_json::Value) -> String {
    json!({"result": result, "error": serde_json::Value::Null, "id": id}).to_string()
}

fn rpc_response_err(id: serde_json::Value, code: i32, message: &str) -> String {
    json!({"result": serde_json::Value::Null, "error": {"code": code, "message": message}, "id": id}).to_string()
}

fn unlock_db_wallet_state(ws: &mut super::WalletState, passphrase: &str) -> Result<(), String> {
    super::clear_wallet_sensitive_state(ws);
    let db = super::walletdb::WalletDb::open(&ws.wallet_path)?;
    let rows = db
        .decrypt_all_keys(passphrase)
        .map_err(|e| format!("unlock_keys_failed:{e}"))?;
    let seed = db
        .decrypt_seed(passphrase)
        .map_err(|e| format!("unlock_seed_failed:{e}"))?;
    let next_index = db.read_next_index()?.max(0) as u32;
    let mut primary_address = db.read_primary_address().unwrap_or_default();

    let mut new_keys = std::collections::BTreeMap::new();
    let mut new_pubkeys = std::collections::BTreeMap::new();
    for (addr, pub_hex, ent) in rows {
        new_pubkeys.insert(addr.clone(), pub_hex);
        new_keys.insert(addr, hex::encode(ent));
    }
    if primary_address.is_empty() {
        primary_address = new_keys.keys().next().cloned().unwrap_or_default();
    }

    ws.keys = new_keys;
    ws.pubkeys = new_pubkeys;
    ws.seed_hex = Some(hex::encode(seed));
    ws.next_index = next_index;
    ws.primary_address = primary_address;
    ws.locked = false;
    ws.db_passphrase = Some(passphrase.to_string());
    Ok(())
}

fn derive_next_address_for_wallet(
    ws: &mut super::WalletState,
    passphrase: Option<&str>,
) -> Result<(String, String, String), String> {
    if ws.is_db {
        if ws.locked {
            return Err("wallet_locked".to_string());
        }
        let chosen_passphrase = passphrase
            .filter(|p| !p.is_empty())
            .map(|p| p.to_string())
            .or_else(|| ws.db_passphrase.clone())
            .ok_or_else(|| "missing_passphrase".to_string())?;
        let seed_hex = ws
            .seed_hex
            .as_ref()
            .ok_or_else(|| "wallet_seed_missing".to_string())?;
        let seed_b = hex::decode(seed_hex).map_err(|_| "wallet_seed_invalid".to_string())?;
        let i = ws.next_index;
        let net = wallet_state_network(ws);
        let (addr, sk_hex, pub_hex) = new_wallet_keypair_from_seed(net, &seed_b, i);
        let sk_b = hex::decode(&sk_hex).map_err(|_| "wallet_key_invalid".to_string())?;
        if sk_b.len() != 32 {
            return Err("wallet_key_invalid".to_string());
        }
        let mut ent = [0u8; 32];
        ent.copy_from_slice(&sk_b);
        let db = super::walletdb::WalletDb::open(&ws.wallet_path)?;
        let next_index = i.saturating_add(1);
        let primary_address = if ws.primary_address.is_empty() {
            Some(addr.as_str())
        } else {
            None
        };
        db.insert_key_with_meta_atomic(
            &addr,
            &pub_hex,
            &ent,
            &chosen_passphrase,
            Some(next_index as i64),
            primary_address,
            None,
        )?;
        ws.next_index = next_index;
        ws.keys.insert(addr.clone(), sk_hex.clone());
        ws.pubkeys.insert(addr.clone(), pub_hex.clone());
        if ws.primary_address.is_empty() {
            ws.primary_address = addr.clone();
            db.update_primary_address(&ws.primary_address)?;
        }
        Ok((addr, pub_hex, sk_hex))
    } else {
        let (addr, sk_hex, pub_hex) = if let Some(seed_hex) = ws.seed_hex.as_ref() {
            let seed_b = hex::decode(seed_hex).map_err(|_| "wallet_seed_invalid".to_string())?;
            let i = ws.next_index;
            let net = wallet_state_network(ws);
            let (a, s, p) = new_wallet_keypair_from_seed(net, &seed_b, i);
            ws.next_index = ws.next_index.saturating_add(1);
            (a, s, p)
        } else {
            new_wallet_keypair_random(wallet_state_network(ws))
        };
        ws.keys.insert(addr.clone(), sk_hex.clone());
        ws.pubkeys.insert(addr.clone(), pub_hex.clone());
        Ok((addr, pub_hex, sk_hex))
    }
}

/// Snapshot + (optional) auto-sync wallet UTXOs if empty by scanning daemon /blocks_from.
fn daemon_tip_height_with_retry(daemon_rpc_port: u16, fallback_height: i64) -> Result<i64, String> {
    for attempt in 0..3 {
        let tip_body = super::http_get_local("127.0.0.1", daemon_rpc_port, "/tip")?;
        let tip_v: serde_json::Value =
            serde_json::from_str(&tip_body).map_err(|e| format!("tip_invalid_json: {}", e))?;

        if let Some(height) = tip_v.get("height").and_then(|x| x.as_i64()) {
            return Ok(height);
        }

        let is_rate_limited =
            tip_v.get("error").and_then(|x| x.as_str()) == Some("rate_limited");
        if is_rate_limited {
            let retry_secs = tip_v
                .get("retry_after_secs")
                .and_then(|x| x.as_u64())
                .unwrap_or(1)
                .min(2);
            wwlog!(
                "wallet_rpc: daemon_tip_rate_limited port={} retry_after_secs={} attempt={}",
                daemon_rpc_port,
                retry_secs,
                attempt + 1
            );
            std::thread::sleep(std::time::Duration::from_secs(retry_secs));
            continue;
        }

        return Err(format!("tip_missing_height: {}", tip_body));
    }

    if fallback_height > 0 {
        wwlog!(
            "wallet_rpc: daemon_tip_fallback_using_cached_height port={} cached_height={}",
            daemon_rpc_port,
            fallback_height
        );
        return Ok(fallback_height);
    }

    Err("tip_height_unavailable".to_string())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct WalletBalanceSnapshot {
    balance_dut: i64,
    spendable_dut: i64,
    immature_dut: i64,
    reserved_dut: i64,
    pending_send_dut: i64,
    pending_change_dut: i64,
    height: i64,
    utxos: usize,
    pending_txs: usize,
}

fn wallet_balance_snapshot(daemon_rpc_port: u16) -> Result<WalletBalanceSnapshot, String> {
    // Snapshot wallet state (avoid holding lock during daemon RPC).
    let (wallet_path, addrs, mut utxos, pending_txs, mut reserved_inputs, last_sync_height) = {
        let g = super::wallet_lock_or_recover();
        let ws = g.as_ref().ok_or_else(|| "wallet_not_open".to_string())?;
        (
            ws.wallet_path.clone(),
            if !ws.pubkeys.is_empty() {
                ws.pubkeys.keys().cloned().collect::<Vec<String>>()
            } else {
                    ws.keys.keys().cloned().collect::<Vec<String>>()
            },
            ws.utxos.clone(),
            ws.pending_txs.clone(),
            ws.reserved_inputs.clone(),
            ws.last_sync_height,
        )
    };

    let cur_h = daemon_tip_height_with_retry(daemon_rpc_port, last_sync_height)?;
    let mut active_pending = pending_txs.clone();
    sync_pending_txs_with_chain_and_mempool(
        &wallet_path,
        &addrs,
        &mut active_pending,
        &[],
        daemon_rpc_port,
    );
    let reserved_outpoints = reconcile_pending_and_reserved_state(
        &wallet_path,
        &utxos,
        cur_h,
        &mut active_pending,
        &mut reserved_inputs,
        daemon_rpc_port,
    )?;

    let missing_tracked_utxo = utxos.iter().any(|u| {
        if !should_probe_daemon_utxo_presence(u, cur_h) {
            return false;
        }
        let path = format!("/utxo?txid={}&vout={}", u.txid, u.vout);
        match super::http_get_local("127.0.0.1", daemon_rpc_port, &path)
            .ok()
            .and_then(|b| serde_json::from_str::<serde_json::Value>(&b).ok())
            .and_then(|v| v.get("found").and_then(|x| x.as_bool()))
        {
            Some(found) => !found,
            None => false,
        }
    });

    let should_refresh = !addrs.is_empty()
        && (wallet_needs_full_utxo_rebuild(&utxos, cur_h, last_sync_height)
            || (cur_h > last_sync_height && last_sync_height > 0)
            || missing_tracked_utxo);

    if should_refresh {
        match refresh_wallet_utxos_runtime(&addrs, daemon_rpc_port, &utxos, last_sync_height) {
            Ok((new_h, new_utxos)) => {
                utxos = new_utxos;

                let mut g = super::wallet_lock_or_recover();
                if let Some(ws) = g.as_mut() {
                    ws.utxos = utxos.clone();
                    ws.last_sync_height = new_h;
                    ws.reserved_inputs = reserved_inputs.clone();
                }

                if let Err(e) =
                    super::save_wallet_sync_state(&wallet_path, &utxos, new_h, &reserved_inputs)
                {
                    wwlog!(
                        "wallet_rpc: balance_sync_persist_failed wallet={} err={}",
                        wallet_public_name(&wallet_path),
                        e
                    );
                }
            }
            Err(e) => {
                wwlog!(
                    "wallet_rpc: balance_refresh_failed wallet={} err={}",
                    wallet_public_name(&wallet_path),
                    e
                );
                return Err(e);
            }
        }
    }

    let mut balance: i64 = 0;
    let mut spendable: i64 = 0;
    let mut immature: i64 = 0;

    for u in utxos.iter() {
        let v = u.value;
        balance += v;
        let reserved = reserved_outpoints.contains(&(u.txid.clone(), u.vout));

        if u.coinbase {
            if reserved {
                continue;
            }
            if is_wallet_utxo_spendable(u, cur_h) {
                spendable += v;
            } else {
                immature += v;
            }
        } else if !reserved && is_wallet_utxo_spendable(u, cur_h) {
            spendable += v;
        }
    }

    let pending_stats = pending_balance_stats(&utxos, &reserved_outpoints, &active_pending);

    Ok(WalletBalanceSnapshot {
        balance_dut: balance,
        spendable_dut: spendable,
        immature_dut: immature,
        reserved_dut: pending_stats.reserved_dut,
        pending_send_dut: pending_stats.pending_send_dut,
        pending_change_dut: pending_stats.pending_change_dut,
        height: cur_h,
        utxos: utxos.len(),
        pending_txs: pending_stats.pending_txs,
    })
}

pub(crate) fn handle_request(
    mut request: tiny_http::Request,
    rpc_addr: &str,
    daemon_rpc_port: u16,
    net: &str,
) {
    let url = request.url().to_string();
    if url.len() > super::MAX_RPC_URL_BYTES {
        super::respond_json(
            request,
            tiny_http::StatusCode(414),
            json!({"error":"uri_too_long","max_bytes":super::MAX_RPC_URL_BYTES}).to_string(),
        );
        return;
    }
    let path = url.split('?').next().unwrap_or(&url);

    // Wallet launch policy is localhost-only for every endpoint, including read-only ones.
    if !super::request_is_loopback(&request) {
        super::respond_json(
            request,
            tiny_http::StatusCode(404),
            json!({"error":"not_found"}).to_string(),
        );
        return;
    }

    if request.method() == &tiny_http::Method::Post {
        let expects_json = matches!(
            path,
            "/rpc"
                | "/open"
                | "/createwallet"
                | "/import_mnemonic"
                | "/getnewaddress"
                | "/sync"
                | "/lock"
                | "/unlock"
                | "/migrate"
                | "/export_mnemonic"
                | "/change_passphrase"
                | "/export_seed"
                | "/send"
                | "/sendmany"
                | "/sendmany_plan"
                | "/send_plan"
        );
        if expects_json && !super::request_content_type_is_json(&request) {
            super::respond_415(request);
            return;
        }
    }

    match path {
        "/health" => {
            if request.method() != &tiny_http::Method::Get {
                respond_method_not_allowed(request);
            } else {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(200),
                    json!({"ok":true}).to_string(),
                );
            }
        }

        "/info" => {
            if request.method() != &tiny_http::Method::Get {
                respond_method_not_allowed(request);
            } else {
                let g = super::wallet_lock_or_recover();
                let wallet_open = g.is_some();
                drop(g);

                let mut body = json!({
                    "net": net,
                    "wallet_rpc": format!("http://{}", rpc_addr),
                    "version": env!("CARGO_PKG_VERSION"),
                    "wallet_open": wallet_open,
                });

                if wallet_open {
                    match wallet_balance_snapshot(daemon_rpc_port) {
                        Ok(snapshot) => {
                            body["balance"] = amount_json_value(snapshot.balance_dut);
                            body["balance_dut"] = json!(snapshot.balance_dut);
                            body["spendable"] = amount_json_value(snapshot.spendable_dut);
                            body["spendable_dut"] = json!(snapshot.spendable_dut);
                            body["immature"] = amount_json_value(snapshot.immature_dut);
                            body["immature_dut"] = json!(snapshot.immature_dut);
                            body["reserved"] = amount_json_value(snapshot.reserved_dut);
                            body["reserved_dut"] = json!(snapshot.reserved_dut);
                            body["pending_send"] = amount_json_value(snapshot.pending_send_dut);
                            body["pending_send_dut"] = json!(snapshot.pending_send_dut);
                            body["pending_change"] = amount_json_value(snapshot.pending_change_dut);
                            body["pending_change_dut"] = json!(snapshot.pending_change_dut);
                            body["pending_txs"] = json!(snapshot.pending_txs);
                            body["height"] = json!(snapshot.height);
                            body["utxos"] = json!(snapshot.utxos);
                            body["unit"] = json!(DISPLAY_UNIT);
                            body["display_unit"] = json!(DISPLAY_UNIT);
                            body["base_unit"] = json!(BASE_UNIT);
                            body["decimals"] = json!(DUTA_DECIMALS);
                        }
                        Err(e) => {
                            body["wallet_state_refresh_error"] =
                                json!(wallet_refresh_error_code(&e));
                            body["wallet_state_refresh_detail"] = json!(e);
                        }
                    }
                }

                super::respond_json(request, tiny_http::StatusCode(200), body.to_string());
            }
        }

        "/daemon_tip" => {
            if request.method() != &tiny_http::Method::Get {
                respond_method_not_allowed(request);
            } else {
                match super::http_get_local("127.0.0.1", daemon_rpc_port, "/tip") {
                    Ok(body) => super::respond_json(request, tiny_http::StatusCode(200), body),
                    Err(e) => {
                        wwlog!("wallet_rpc: daemon_tip_failed err={}", e);
                        respond_http_error_detail(
                            request,
                            tiny_http::StatusCode(502),
                            "daemon_unreachable",
                            e,
                        );
                    }
                }
            }
        }

        // Compatibility surface for older clients.
        // Public/mobile canonical surface is the explicit HTTP endpoints below.
        "/rpc" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }

            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        rpc_response_err(serde_json::Value::Null, -32700, &e),
                    );
                    return;
                }
            };

            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        rpc_response_err(
                            serde_json::Value::Null,
                            -32700,
                            &format!("invalid_json: {}", e),
                        ),
                    );
                    return;
                }
            };

            let id = v.get("id").cloned().unwrap_or(serde_json::Value::Null);
            let method = v.get("method").and_then(|x| x.as_str()).unwrap_or("");
            let params = v
                .get("params")
                .and_then(|x| x.as_array())
                .cloned()
                .unwrap_or_default();

            // Compatibility JSON-RPC surface only. Canonical/public wallet API is the HTTP route set for mobile and integrations.
            match method {
                "help" => {
                    let methods = json!([
                        "help",
                        "createwallet(wallet_path, overwrite=false)",
                        "loadwallet(wallet_path)",
                        "getwalletinfo",
                        "getbalance",
                        "getaddress",
                        "getnewaddress",
                        "listunspent(minconf=0)",
                        "listtransactions(count=10, skip=0)",
                        "gettransaction(txid)",
                        "estimatebatchsend(amount, fee=0.0001, outputs=null)",
                        "estimatebatchsendmany(outputs, fee=null)",
                        "sendtoaddress(address, amount, fee=1)",
                        "walletpassphrase(passphrase, timeout=0)",
                        "walletlock"
                    ]);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, methods),
                    );
                }

                "createwallet" => {
                    // compatibility RPC alias for HTTP POST /createwallet
                    // params: [wallet_path, overwrite?]
                    let wallet_path = match params.get(0).and_then(|x| x.as_str()) {
                        Some(p) if !p.is_empty() => p.to_string(),
                        _ => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, "missing_wallet_path"),
                            );
                            return;
                        }
                    };
                    if !wallet_path.ends_with(".db") {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            rpc_response_err(
                                id,
                                -32602,
                                "legacy_plaintext_wallet_disabled_use_db_wallet",
                            ),
                        );
                        return;
                    }
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        rpc_response_err(
                            id,
                            -32602,
                            "db_wallet_create_via_json_rpc_not_supported_use_http_createwallet",
                        ),
                    );
                }

                "loadwallet" | "openwallet" => {
                    let wallet_path = match params.get(0).and_then(|x| x.as_str()) {
                        Some(p) if !p.is_empty() => p.to_string(),
                        _ => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, "missing_wallet_path"),
                            );
                            return;
                        }
                    };
                    let _passphrase = params
                        .get(1)
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    match super::load_wallet_from_path(&wallet_path) {
                        Ok(mut ws) => {
                            let passphrase =
                                params.get(1).and_then(|x| x.as_str()).unwrap_or("");
                            if !passphrase.is_empty() {
                                if let Err(e) = unlock_db_wallet_state(&mut ws, passphrase) {
                                    super::respond_json(
                                        request,
                                        tiny_http::StatusCode(400),
                                        rpc_response_err(id, -14, &e),
                                    );
                                    return;
                                }
                            }
                            let unlocked = !(ws.is_db && ws.locked);
                            let mut guard = super::wallet_lock_or_recover();
                            if let Some(existing) = guard.as_mut() {
                                super::clear_wallet_sensitive_state(existing);
                            }
                            *guard = Some(ws);
                            super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        json!({"ok":true,"wallet":wallet_public_name(&wallet_path),"unlocked":unlocked}).to_string(),
                    );
                        }
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -18, &e),
                            );
                        }
                    }
                }

                "getwalletinfo" => {
                    let snapshot =
                        match wallet_balance_snapshot(daemon_rpc_port) {
                            Ok(t) => t,
                            Err(e) => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -18, &e),
                                );
                                return;
                            }
                        };

                    let (wallet_path, unlocked, keypoolsize, addrs, pending_txs) = {
                        let g = super::wallet_lock_or_recover();
                        let ws = match g.as_ref() {
                            Some(w) => w,
                            None => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -18, "wallet_not_open"),
                                );
                                return;
                            }
                        };
                        (
                            ws.wallet_path.clone(),
                            !(ws.is_db && ws.locked),
                            ws.pubkeys.len(),
                            if !ws.pubkeys.is_empty() {
                                ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                            } else {
                                ws.keys.keys().cloned().collect::<Vec<String>>()
                            },
                            ws.pending_txs.clone(),
                        )
                    };

                    let txcount = match scan_wallet_txs_via_blocks_from(&addrs, daemon_rpc_port) {
                        Ok((_cur_h, txs)) => {
                            let merged = merge_pending_wallet_txs(txs, &pending_txs);
                            merged.len()
                        }
                        Err(_) => pending_txs.len(),
                    };

                    let result = json!({
                        "walletname": wallet_public_name(&wallet_path),
                        "walletversion": 1,
                        "balance": format_dut_i64(snapshot.balance_dut),
                        "balance_dut": snapshot.balance_dut,
                        "spendable_balance": format_dut_i64(snapshot.spendable_dut),
                        "spendable_balance_dut": snapshot.spendable_dut,
                        "immature_balance": format_dut_i64(snapshot.immature_dut),
                        "immature_balance_dut": snapshot.immature_dut,
                        "reserved_balance": format_dut_i64(snapshot.reserved_dut),
                        "reserved_balance_dut": snapshot.reserved_dut,
                        "pending_send": format_dut_i64(snapshot.pending_send_dut),
                        "pending_send_dut": snapshot.pending_send_dut,
                        "pending_change": format_dut_i64(snapshot.pending_change_dut),
                        "pending_change_dut": snapshot.pending_change_dut,
                        "txcount": txcount,
                        "keypoolsize": keypoolsize,
                        "unlocked": unlocked,
                        "height": snapshot.height,
                        "utxos": snapshot.utxos,
                        "pending_txs": snapshot.pending_txs,
                        "unit": DISPLAY_UNIT,
                        "display_unit": DISPLAY_UNIT,
                        "base_unit": BASE_UNIT,
                        "decimals": DUTA_DECIMALS
                    });

                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, result),
                    );
                }

                "getbalance" => {
                    let snapshot =
                        match wallet_balance_snapshot(daemon_rpc_port) {
                            Ok(t) => t,
                            Err(e) => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -18, &e),
                                );
                                return;
                            }
                        };
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, json!({
                            "amount": format_dut_i64(snapshot.spendable_dut),
                            "amount_dut": snapshot.spendable_dut,
                            "reserved": format_dut_i64(snapshot.reserved_dut),
                            "reserved_dut": snapshot.reserved_dut,
                            "pending_send": format_dut_i64(snapshot.pending_send_dut),
                            "pending_send_dut": snapshot.pending_send_dut,
                            "pending_change": format_dut_i64(snapshot.pending_change_dut),
                            "pending_change_dut": snapshot.pending_change_dut,
                            "pending_txs": snapshot.pending_txs,
                            "unit": DISPLAY_UNIT,
                            "display_unit": DISPLAY_UNIT,
                            "base_unit": BASE_UNIT,
                            "decimals": DUTA_DECIMALS
                        })),
                    );
                }

                "getaddress" => {
                    let g = super::wallet_lock_or_recover();
                    let ws = match g.as_ref() {
                        Some(w) => w,
                        None => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -18, "wallet_not_open"),
                            );
                            return;
                        }
                    };
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, json!(ws.primary_address)),
                    );
                }

                // canonical HTTP endpoint: POST /getnewaddress
                // RPC methods remain for compatibility with older clients.
                "getnewaddress" | "getaddressnew" => {
                    // Reuse the same derivation as /getnewaddress. Works for JSON wallets.
                    let mut g = super::wallet_lock_or_recover();
                    let ws = match g.as_mut() {
                        Some(w) => w,
                        None => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -18, "wallet_not_open"),
                            );
                            return;
                        }
                    };

                    let passphrase = params.get(0).and_then(|x| x.as_str()).unwrap_or("");
                    let (addr, _pub_hex, _sk_hex) =
                        match derive_next_address_for_wallet(ws, Some(passphrase)) {
                            Ok(v) => v,
                            Err(e) => {
                                let code = if e == "wallet_locked" {
                                    -13
                                } else if e == "missing_passphrase" {
                                    -32602
                                } else {
                                    -32603
                                };
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, code, &e),
                                );
                                return;
                            }
                        };

                    debug_assert!(ws.is_db, "wallet state should always be db-backed");

                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, json!(addr)),
                    );
                }

                "listunspent" => {
                    // params: [minconf] (optional)
                    let minconf: i64 = params.get(0).and_then(|x| x.as_i64()).unwrap_or(0);

                    let snapshot =
                        match wallet_balance_snapshot(daemon_rpc_port) {
                            Ok(t) => t,
                            Err(e) => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -18, &e),
                                );
                                return;
                            }
                        };
                    let (wallet_path, cur_h, utxos, pending_txs) = {
                        let g = super::wallet_lock_or_recover();
                        let ws = match g.as_ref() {
                            Some(w) => w,
                            None => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -18, "wallet_not_open"),
                                );
                                return;
                            }
                        };
                        (
                            ws.wallet_path.clone(),
                            snapshot.height,
                            ws.utxos.clone(),
                            ws.pending_txs.clone(),
                        )
                    };
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    let mut active_pending = pending_txs;
                    let mut reserved_outpoints = pending_reserved_outpoints(&active_pending);
                    match pending_mempool_state_or_err(&wallet_path, &active_pending, daemon_rpc_port) {
                        Ok(Some((mempool_txids, mempool_reserved))) => {
                            if prune_inactive_pending_txs(&mut active_pending, &mempool_txids, now_secs) {
                                {
                                    let mut g = super::wallet_lock_or_recover();
                                    if let Some(ws) = g.as_mut() {
                                        ws.pending_txs = active_pending.clone();
                                    }
                                }
                                if let Err(e) = super::save_wallet_pending_txs(&wallet_path, &active_pending) {
                                    wwlog!(
                                        "wallet_rpc: pending_txs_mempool_prune_persist_failed wallet={} err={}",
                                        wallet_public_name(&wallet_path),
                                        e
                                    );
                                }
                            }
                            reserved_outpoints = pending_reserved_outpoints(&active_pending);
                            reserved_outpoints.extend(mempool_reserved);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                rpc_response_err(id, -32603, &e),
                            );
                            return;
                        }
                    }

                    let mut out: Vec<serde_json::Value> = Vec::new();
                    for u in utxos.iter() {
                        let conf = if u.height > 0 && cur_h >= u.height {
                            cur_h - u.height + 1
                        } else {
                            0
                        };
                        if conf < minconf {
                            continue;
                        }
                        out.push(json!({
                            "txid": u.txid,
                            "vout": u.vout,
                            "address": u.address,
                            "amount": format_dut_i64(u.value),
                            "amount_dut": u.value,
                            "confirmations": conf,
                            "spendable": is_wallet_utxo_spendable(u, cur_h)
                                && !reserved_outpoints.contains(&(u.txid.clone(), u.vout)),
                            "coinbase": u.coinbase,
                            "unit": DISPLAY_UNIT,
                            "display_unit": DISPLAY_UNIT,
                            "base_unit": BASE_UNIT,
                            "decimals": DUTA_DECIMALS
                        }));
                    }

                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, json!(out)),
                    );
                }

                "listtransactions" => {
                    let count_in = params.get(0).and_then(|x| x.as_i64()).unwrap_or(10);
                    let skip_in = params.get(1).and_then(|x| x.as_i64()).unwrap_or(0);

                    let count: usize = if count_in <= 0 {
                        0
                    } else {
                        (count_in as usize).min(1000)
                    };
                    let skip: usize = if skip_in <= 0 {
                        0
                    } else {
                        (skip_in as usize).min(1_000_000)
                    };

                    let (wallet_path, addrs, pending_txs) = {
                        let g = super::wallet_lock_or_recover();
                        let ws = match g.as_ref() {
                            Some(w) => w,
                            None => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -18, "wallet_not_open"),
                                );
                                return;
                            }
                        };
                        let addrs = if !ws.pubkeys.is_empty() {
                            ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                        } else {
                            ws.keys.keys().cloned().collect::<Vec<String>>()
                        };
                        if addrs.is_empty() {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -18, "wallet_no_address"),
                            );
                            return;
                        }
                        (ws.wallet_path.clone(), addrs, ws.pending_txs.clone())
                    };

                    let (cur_h, txs) =
                        match scan_wallet_txs_via_blocks_from(&addrs, daemon_rpc_port) {
                            Ok(t) => t,
                            Err(e) => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(502),
                                    rpc_response_err(id, -32603, &e),
                                );
                                return;
                            }
                        };
                    let mut pending_txs = pending_txs;
                    sync_pending_txs_with_chain_and_mempool(
                        &wallet_path,
                        &addrs,
                        &mut pending_txs,
                        &txs,
                        daemon_rpc_port,
                    );
                    let txs = merge_pending_wallet_txs(txs, &pending_txs);

                    let mut out: Vec<serde_json::Value> = Vec::new();
                    for (i, (txid, h, block_time, category, amt, fee, coinbase, details)) in
                        txs.into_iter().enumerate()
                    {
                        if i < skip {
                            continue;
                        }
                        if out.len() >= count {
                            break;
                        }
                        let conf = if h > 0 && cur_h >= h {
                            cur_h - h + 1
                        } else {
                            0
                        };
                        out.push(json!({
                            "category": category,
                            "txid": txid,
                            "amount": format_dut_i64(amt),
                            "amount_dut": amt,
                            "fee": format_dut_i64(fee),
                            "fee_dut": fee,
                            "confirmations": conf,
                            "blockheight": h,
                            "time": block_time,
                            "timereceived": block_time,
                            "blocktime": block_time,
                            "coinbase": coinbase,
                            "details": normalize_history_details(&details),
                            "unit": DISPLAY_UNIT,
                            "display_unit": DISPLAY_UNIT,
                            "base_unit": BASE_UNIT,
                            "decimals": DUTA_DECIMALS
                        }));
                    }

                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, json!(out)),
                    );
                }

                "gettransaction" => {
                    let txid = match params.get(0).and_then(|x| x.as_str()) {
                        Some(t) if !t.trim().is_empty() => t.trim().to_string(),
                        _ => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, "missing_txid"),
                            );
                            return;
                        }
                    };

                    let (wallet_path, addrs, pending_txs) = {
                        let g = super::wallet_lock_or_recover();
                        let ws = match g.as_ref() {
                            Some(w) => w,
                            None => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -18, "wallet_not_open"),
                                );
                                return;
                            }
                        };
                        let addrs = if !ws.pubkeys.is_empty() {
                            ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                        } else {
                            ws.keys.keys().cloned().collect::<Vec<String>>()
                        };
                        if addrs.is_empty() {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -18, "wallet_no_address"),
                            );
                            return;
                        }
                        (ws.wallet_path.clone(), addrs, ws.pending_txs.clone())
                    };

                    let (cur_h, txs) =
                        match scan_wallet_txs_via_blocks_from(&addrs, daemon_rpc_port) {
                            Ok(t) => t,
                            Err(e) => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(502),
                                    rpc_response_err(id, -32603, &e),
                                );
                                return;
                            }
                        };
                    let mut pending_txs = pending_txs;
                    sync_pending_txs_with_chain_and_mempool(
                        &wallet_path,
                        &addrs,
                        &mut pending_txs,
                        &txs,
                        daemon_rpc_port,
                    );
                    let txs = merge_pending_wallet_txs(txs, &pending_txs);

                    let mut found: Option<(i64, i64, String, i64, i64, bool, Vec<serde_json::Value>)> =
                        None;
                    for (t, h, block_time, category, amt, fee, cb, details) in txs.into_iter() {
                        if t == txid {
                            found = Some((h, block_time, category, amt, fee, cb, details));
                            break;
                        }
                    }

                    let (h, block_time, category, amt, fee, coinbase, details) = match found {
                        Some(x) => x,
                        None => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(404),
                                rpc_response_err(id, -5, "tx_not_found_for_wallet"),
                            );
                            return;
                        }
                    };

                    let conf = if h > 0 && cur_h >= h {
                        cur_h - h + 1
                    } else {
                        0
                    };

                    let result = json!({
                        "txid": txid,
                        "category": category,
                        "amount": format_dut_i64(amt),
                        "amount_dut": amt,
                        "fee": format_dut_i64(fee),
                        "fee_dut": fee,
                        "confirmations": conf,
                        "blockheight": h,
                        "time": block_time,
                        "timereceived": block_time,
                        "blocktime": block_time,
                        "coinbase": coinbase,
                        "details": normalize_history_details(&details),
                        "unit": DISPLAY_UNIT,
                        "display_unit": DISPLAY_UNIT,
                        "base_unit": BASE_UNIT,
                        "decimals": DUTA_DECIMALS
                    });

                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, result),
                    );
                }

                "walletlock" => {
                    let mut g = super::wallet_lock_or_recover();
                    let ws = match g.as_mut() {
                        Some(w) => w,
                        None => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -18, "wallet_not_open"),
                            );
                            return;
                        }
                    };
                    debug_assert!(ws.is_db, "wallet state should always be db-backed");
                    super::clear_wallet_sensitive_state(ws);
                    ws.locked = true;
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, json!(true)),
                    );
                }

                "walletpassphrase" => {
                    // params: [passphrase, timeout] (timeout ignored for now)
                    let _passphrase = match params.get(0).and_then(|x| x.as_str()) {
                        Some(p) if !p.is_empty() => p.to_string(),
                        _ => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, "missing_passphrase"),
                            );
                            return;
                        }
                    };

                    let mut g = super::wallet_lock_or_recover();
                    let ws = match g.as_mut() {
                        Some(w) => w,
                        None => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -18, "wallet_not_open"),
                            );
                            return;
                        }
                    };
                    debug_assert!(ws.is_db, "wallet state should always be db-backed");
                    if let Err(e) = unlock_db_wallet_state(ws, &_passphrase) {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            rpc_response_err(id, -14, &e),
                        );
                        return;
                    }
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, json!(true)),
                    );
                }

                "estimatebatchsend" => {
                    let amount = match parse_required_amount_param(params.get(0), None) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, &format!("invalid_amount:{e}")),
                            );
                            return;
                        }
                    };
                    if amount <= 0 {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            rpc_response_err(id, -32602, "amount_must_be_positive"),
                        );
                        return;
                    }
                    let fee = match parse_optional_fee_param(params.get(1), None) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, &format!("invalid_fee:{e}")),
                            );
                            return;
                        }
                    };
                    if fee < 0 {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            rpc_response_err(id, -32602, "fee_must_be_non_negative"),
                        );
                        return;
                    }
                    let requested_outputs = params
                        .get(2)
                        .and_then(|x| x.as_u64())
                        .map(|v| v as usize)
                        .filter(|v| *v > 0);
                    let need = match amount.checked_add(fee) {
                        Some(v) => v,
                        None => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, "amount_overflow"),
                            );
                            return;
                        }
                    };

                    let (
                        wallet_path,
                        addrs,
                        mut utxos,
                        pending_txs,
                        reserved_inputs,
                        last_sync_height,
                    ) = {
                        let g = super::wallet_lock_or_recover();
                        let ws = match g.as_ref() {
                            Some(w) => w,
                            None => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -18, "wallet_not_open"),
                                );
                                return;
                            }
                        };
                        (
                            ws.wallet_path.clone(),
                            if !ws.pubkeys.is_empty() {
                                ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                            } else {
                                ws.keys.keys().cloned().collect::<Vec<String>>()
                            },
                            ws.utxos.clone(),
                            ws.pending_txs.clone(),
                            ws.reserved_inputs.clone(),
                            ws.last_sync_height,
                        )
                    };

                    let (cur_h, new_utxos) = match refresh_wallet_utxos_runtime(
                        &addrs,
                        daemon_rpc_port,
                        &utxos,
                        last_sync_height,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                rpc_response_err(id, -18, &e),
                            );
                            return;
                        }
                    };
                    utxos = new_utxos;
                    let _ =
                        persist_runtime_reserved_inputs(&wallet_path, &utxos, cur_h, &reserved_inputs);

                    let mut active_pending = pending_txs.clone();
                    let mut reserved_inputs = reserved_inputs;
                    let reserved_outpoints = match reconcile_pending_and_reserved_state(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &mut active_pending,
                        &mut reserved_inputs,
                        daemon_rpc_port,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                rpc_response_err(id, -18, &e),
                            );
                            return;
                        }
                    };

                    let spendable_values: Vec<i64> = utxos
                        .iter()
                        .filter(|u| u.value > 0)
                        .filter(|u| !u.txid.is_empty())
                        .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                        .filter(|u| is_wallet_utxo_spendable(u, cur_h))
                        .map(|u| u.value)
                        .collect();
                    let spendable_balance_dut: i64 = spendable_values.iter().sum();
                    let pending_stats =
                        pending_balance_stats(&utxos, &reserved_outpoints, &active_pending);
                    let plan = simulate_send_plan(
                        &spendable_values,
                        need,
                        MAX_WALLET_SEND_INPUTS,
                        requested_outputs,
                    );
                    let failure = if plan.requested_outputs_fit {
                        serde_json::Value::Null
                    } else {
                        insufficient_funds_body(
                            need,
                            plan.last_total_in,
                            fee,
                            cur_h,
                            spendable_values.len(),
                            reserved_outpoints.len(),
                            &pending_stats,
                        )
                    };

                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(
                            id,
                            json!({
                                "amount": format_dut_i64(amount),
                                "amount_dut": amount,
                                "fee": format_dut_i64(fee),
                                "fee_dut": fee,
                                "need": format_dut_i64(need),
                                "need_dut": need,
                                "requested_outputs": requested_outputs,
                                "max_outputs": plan.max_outputs,
                                "requested_outputs_fit": plan.requested_outputs_fit,
                                "spendable_utxos": spendable_values.len(),
                                "spendable_balance": format_dut_i64(spendable_balance_dut),
                                "spendable_balance_dut": spendable_balance_dut,
                                "reserved_outpoints": reserved_outpoints.len(),
                                "reserved_dut": pending_stats.reserved_dut,
                                "pending_send_dut": pending_stats.pending_send_dut,
                                "pending_change_dut": pending_stats.pending_change_dut,
                                "selected_input_count_for_failure": plan.selected_input_count_for_failure,
                                "failure": failure,
                                "unit": DISPLAY_UNIT,
                                "display_unit": DISPLAY_UNIT,
                                "base_unit": BASE_UNIT,
                                "decimals": DUTA_DECIMALS
                            }),
                        ),
                    );
                }

                "estimatebatchsendmany" => {
                    let outputs = match params.get(0).and_then(|x| x.as_array()) {
                        Some(v) if !v.is_empty() => v,
                        _ => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, "missing_outputs"),
                            );
                            return;
                        }
                    };
                    let req_outputs: Vec<SendManyRecipient> = match outputs
                        .iter()
                        .cloned()
                        .map(serde_json::from_value::<SendManyRecipient>)
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, &format!("invalid_outputs:{e}")),
                            );
                            return;
                        }
                    };
                    let fee_supplied = params.get(1).is_some() && !params[1].is_null();
                    let requested_fee = match parse_optional_fee_param(params.get(1), None) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, &format!("invalid_fee:{e}")),
                            );
                            return;
                        }
                    };
                    if requested_fee < 0 {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            rpc_response_err(id, -32602, "fee_must_be_non_negative"),
                        );
                        return;
                    }
                    if requested_fee > DEFAULT_MAX_WALLET_FEE_DUT {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            rpc_response_err(id, -32602, "fee_too_high"),
                        );
                        return;
                    }
                    let (recipients, total_amount) =
                        match parse_sendmany_recipients(&req_outputs, net) {
                            Ok(v) => v,
                            Err((error, detail)) => {
                                let message = if detail.is_empty() {
                                    error
                                } else {
                                    format!("{error}:{detail}")
                                };
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -32602, &message),
                                );
                                return;
                            }
                        };
                    let (
                        wallet_path,
                        change_addr,
                        addrs,
                        signers_by_pkh,
                        signers_by_addr,
                        mut utxos,
                        pending_txs,
                        reserved_inputs,
                        last_sync_height,
                    ) = {
                        let g = super::wallet_lock_or_recover();
                        let ws = match g.as_ref() {
                            Some(w) => w,
                            None => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -18, "wallet_not_open"),
                                );
                                return;
                            }
                        };
                        if ws.is_db && ws.locked {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -13, "wallet_locked"),
                            );
                            return;
                        }
                        (
                            ws.wallet_path.clone(),
                            if !ws.primary_address.is_empty() {
                                ws.primary_address.clone()
                            } else {
                                ws.keys.keys().next().cloned().unwrap_or_default()
                            },
                            if !ws.pubkeys.is_empty() {
                                ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                            } else {
                                ws.keys.keys().cloned().collect::<Vec<String>>()
                            },
                            match wallet_signers_by_pkh(ws) {
                                Ok(v) => v,
                                Err(e) => {
                                    super::respond_json(
                                        request,
                                        tiny_http::StatusCode(500),
                                        rpc_response_err(id, -18, &e),
                                    );
                                    return;
                                }
                            },
                            match wallet_signers_by_addr(ws) {
                                Ok(v) => v,
                                Err(e) => {
                                    super::respond_json(
                                        request,
                                        tiny_http::StatusCode(500),
                                        rpc_response_err(id, -18, &e),
                                    );
                                    return;
                                }
                            },
                            ws.utxos.clone(),
                            ws.pending_txs.clone(),
                            ws.reserved_inputs.clone(),
                            ws.last_sync_height,
                        )
                    };
                    if change_addr.is_empty() {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            rpc_response_err(id, -18, "wallet_no_address"),
                        );
                        return;
                    }
                    let (cur_h, new_utxos) = match refresh_wallet_utxos_runtime(
                        &addrs,
                        daemon_rpc_port,
                        &utxos,
                        last_sync_height,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                rpc_response_err(id, -18, &e),
                            );
                            return;
                        }
                    };
                    utxos = new_utxos;
                    let _ = persist_runtime_reserved_inputs(&wallet_path, &utxos, cur_h, &reserved_inputs);
                    let mut active_pending = pending_txs.clone();
                    let mut reserved_inputs = reserved_inputs;
                    let reserved_outpoints = match reconcile_pending_and_reserved_state(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &mut active_pending,
                        &mut reserved_inputs,
                        daemon_rpc_port,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                rpc_response_err(id, -18, &e),
                            );
                            return;
                        }
                    };
                    let mut spendable_utxos: Vec<OwnedInput> = Vec::new();
                    for utxo in utxos
                        .iter()
                        .filter(|u| u.value > 0)
                        .filter(|u| !u.txid.is_empty())
                        .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                        .filter(|u| is_wallet_utxo_spendable(u, cur_h))
                    {
                        match resolve_owned_input(
                            daemon_rpc_port,
                            utxo,
                            &signers_by_pkh,
                            &signers_by_addr,
                        ) {
                            Ok(Some(owned)) => spendable_utxos.push(owned),
                            Ok(None) => {}
                            Err(e) => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(502),
                                    rpc_response_err(id, -18, &e),
                                );
                                return;
                            }
                        }
                    }
                    let pending_stats =
                        pending_balance_stats(&utxos, &reserved_outpoints, &active_pending);
                    let spendable_utxo_count = spendable_utxos.len();
                    let spendable_balance_dut: i64 =
                        spendable_utxos.iter().map(|u| u.utxo.value).sum();
                    let mut target_fee = requested_fee;
                    let mut selected_inputs = 0usize;
                    let mut final_fee = 0i64;
                    let mut final_change = 0i64;
                    let mut min_relay_fee = 0i64;
                    let mut tx_size = 0usize;
                    let mut can_send = false;
                    let mut failure = serde_json::Value::Null;
                    for _ in 0..8 {
                        let need = match total_amount.checked_add(target_fee) {
                            Some(v) => v,
                            None => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    rpc_response_err(id, -32602, "amount_overflow"),
                                );
                                return;
                            }
                        };
                        let (selected, total_in) =
                            select_inputs_for_need(&spendable_utxos, need, MAX_WALLET_SEND_INPUTS);
                        if total_in < need {
                            failure = insufficient_funds_body(
                                need,
                                total_in,
                                target_fee,
                                cur_h,
                                spendable_utxo_count,
                                reserved_outpoints.len(),
                                &pending_stats,
                            );
                            break;
                        }
                        let (tx, next_final_fee, next_final_change, _change_vout) =
                            match sign_send_tx(&selected, &recipients, &change_addr, target_fee) {
                                Ok(v) => v,
                                Err(e) => {
                                    super::respond_json(
                                        request,
                                        tiny_http::StatusCode(500),
                                        rpc_response_err(id, -18, &e),
                                    );
                                    return;
                                }
                            };
                        let next_tx_size = match serde_json::to_vec(&tx) {
                            Ok(b) => b.len(),
                            Err(e) => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(500),
                                    rpc_response_err(id, -18, &format!("json_encode_failed:{e}")),
                                );
                                return;
                            }
                        };
                        selected_inputs = selected.len();
                        final_fee = next_final_fee;
                        final_change = next_final_change;
                        min_relay_fee = relay_fee_for_tx_bytes(next_tx_size);
                        tx_size = next_tx_size;
                        if fee_supplied {
                            if final_fee < min_relay_fee {
                                failure = fee_too_low_body(final_fee, min_relay_fee, tx_size);
                            } else {
                                can_send = true;
                            }
                            break;
                        }
                        if final_fee >= min_relay_fee {
                            can_send = true;
                            break;
                        }
                        target_fee = min_relay_fee;
                    }
                    let effective_fee = final_fee.max(target_fee);
                    let need_dut = total_amount.checked_add(effective_fee).unwrap_or(i64::MAX);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(
                            id,
                            json!({
                                "can_send": can_send,
                                "outputs": recipients.len(),
                                "inputs": selected_inputs,
                                "amount": format_dut_i64(total_amount),
                                "amount_dut": total_amount,
                                "fee": format_dut_i64(effective_fee),
                                "fee_dut": effective_fee,
                                "fee_auto": !fee_supplied,
                                "min_relay_fee": format_dut_i64(min_relay_fee),
                                "min_relay_fee_dut": min_relay_fee,
                                "need": format_dut_i64(need_dut),
                                "need_dut": need_dut,
                                "change": format_dut_i64(final_change),
                                "change_dut": final_change,
                                "size": tx_size,
                                "spendable_utxos": spendable_utxo_count,
                                "spendable_balance": format_dut_i64(spendable_balance_dut),
                                "spendable_balance_dut": spendable_balance_dut,
                                "reserved_outpoints": reserved_outpoints.len(),
                                "reserved_dut": pending_stats.reserved_dut,
                                "pending_send_dut": pending_stats.pending_send_dut,
                                "pending_change_dut": pending_stats.pending_change_dut,
                                "failure": failure,
                                "unit": DISPLAY_UNIT,
                                "display_unit": DISPLAY_UNIT,
                                "base_unit": BASE_UNIT,
                                "decimals": DUTA_DECIMALS
                            }),
                        ),
                    );
                }

                "sendtoaddress" => {
                    let to = match params.get(0).and_then(|x| x.as_str()) {
                        Some(a) if !a.is_empty() => a.to_string(),
                        _ => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, "missing_address"),
                            );
                            return;
                        }
                    };
                    let req = SendRequest {
                        to,
                        amount: params.get(1).cloned(),
                        amount_dut: None,
                        fee: params.get(2).cloned(),
                        fee_dut: None,
                    };
                    // Validate destination address (minimal sanity).
                    let to = req.to.trim();
                    if duta_core::address::parse_address_for_network(net_from_name(net), to)
                        .is_none()
                    {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"invalid_address","detail":"to"}).to_string(),
                        );
                        return;
                    }
                    let to_addr = to.to_string();

                    // Validate amount / fee.
                    let amount = match parse_required_amount_param(req.amount.as_ref(), req.amount_dut) {
                        Ok(v) => v,
                        Err(e) => {
                            let message = if e == "missing_amount" {
                                "missing_amount"
                            } else {
                                "invalid_amount"
                            };
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, message),
                            );
                            return;
                        }
                    };
                    if amount <= 0 {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"invalid_amount","detail":"amount_must_be_positive"})
                                .to_string(),
                        );
                        return;
                    }
                    let fee_in: i64 = match parse_optional_fee_param(req.fee.as_ref(), req.fee_dut) {
                        Ok(v) => v,
                        Err(_) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, "invalid_fee"),
                            );
                            return;
                        }
                    };
                    if fee_in < 0 {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"invalid_fee","detail":"fee_must_be_non_negative"})
                                .to_string(),
                        );
                        return;
                    }
                    let fee: i64 = fee_in;

                    const MAX_FEE: i64 = DEFAULT_MAX_WALLET_FEE_DUT;
                    if fee > MAX_FEE {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                    fee_too_high_body(fee, MAX_FEE).to_string(),
                        );
                        return;
                    }

                    let _send_guard = wallet_send_lock_or_recover();

                    // Snapshot wallet state.
            let (
                wallet_path,
                change_addr,
                addrs,
                signers_by_pkh,
                signers_by_addr,
                utxos,
                pending_txs,
                reserved_inputs,
                last_sync_height,
            ) = {
                let g = super::wallet_lock_or_recover();
                let ws = match g.as_ref() {
                            Some(w) => w,
                            None => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    json!({"error":"wallet_not_open"}).to_string(),
                                );
                                return;
                            }
                        };
                        if ws.is_db && ws.locked {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                json!({"error":"wallet_locked"}).to_string(),
                            );
                            return;
                        }
                        let change_addr = if !ws.primary_address.is_empty() {
                            ws.primary_address.clone()
                        } else {
                            ws.keys.keys().next().cloned().unwrap_or_default()
                        };
                        let addrs = if !ws.pubkeys.is_empty() {
                            ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                        } else {
                            ws.keys.keys().cloned().collect::<Vec<String>>()
                        };
                        (
                            ws.wallet_path.clone(),
                            change_addr,
                            addrs,
                            match wallet_signers_by_pkh(ws) {
                                Ok(v) => v,
                                Err(e) => {
                                    super::respond_json(
                                        request,
                                        tiny_http::StatusCode(500),
                                        json!({"error":"wallet_key_missing","detail":e}).to_string(),
                                    );
                                    return;
                                }
                            },
                            match wallet_signers_by_addr(ws) {
                                Ok(v) => v,
                                Err(e) => {
                                    super::respond_json(
                                        request,
                                        tiny_http::StatusCode(500),
                                        json!({"error":"wallet_key_missing","detail":e}).to_string(),
                                    );
                                    return;
                                }
                            },
                            ws.utxos.clone(),
                            ws.pending_txs.clone(),
                            ws.reserved_inputs.clone(),
                            ws.last_sync_height,
                        )
                    };

                    if change_addr.is_empty() {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"wallet_no_address"}).to_string(),
                        );
                        return;
                    }
                    // Need daemon height for maturity calculation.
                    let (cur_h, utxos) = match refresh_wallet_utxos_runtime(
                        &addrs,
                        daemon_rpc_port,
                        &utxos,
                        last_sync_height,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                json!({"error":"wallet_state_refresh_failed","detail":e}).to_string(),
                            );
                            return;
                        }
                    };

                    if let Err(e) = persist_runtime_reserved_inputs(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &reserved_inputs,
                    ) {
                        wwlog!(
                            "wallet_rpc: send_refresh_persist_failed wallet={} err={}",
                            wallet_public_name(&wallet_path),
                            e
                        );
                    }

                    // Select spendable inputs.
                    const DUST_CHANGE: i64 = DEFAULT_DUST_CHANGE_DUT;
                    let need: i64 = match amount.checked_add(fee) {
                        Some(v) => v,
                        None => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                json!({"error":"bad_request","detail":"amount_overflow"})
                                    .to_string(),
                            );
                            return;
                        }
                    };
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    let mut active_pending = pending_txs.clone();
                    let mut reserved_inputs = reserved_inputs;
                    let reserved_outpoints = match reconcile_pending_and_reserved_state(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &mut active_pending,
                        &mut reserved_inputs,
                        daemon_rpc_port,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                json!({"error":"daemon_mempool_unreachable","detail":e}).to_string(),
                            );
                            return;
                        }
                    };
                    // Filter out stale UTXOs (wallet view may lag behind daemon UTXO set).
                    let mut spendable_utxos: Vec<OwnedInput> = Vec::new();
                    for utxo in utxos
                        .iter()
                        .filter(|u| u.value > 0)
                        .filter(|u| !u.txid.is_empty())
                        .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                        .filter(|u| is_wallet_utxo_spendable(u, cur_h))
                    {
                        match resolve_owned_input(
                            daemon_rpc_port,
                            utxo,
                            &signers_by_pkh,
                            &signers_by_addr,
                        ) {
                            Ok(Some(owned)) => spendable_utxos.push(owned),
                            Ok(None) => {}
                            Err(e) => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(502),
                                    json!({"error":"daemon_bad_response","detail":e}).to_string(),
                                );
                                return;
                            }
                        }
                    }

                    // Prefer exact match to minimize inputs.
                    let (selected, total_in) =
                        select_inputs_for_need(&spendable_utxos, need, MAX_WALLET_SEND_INPUTS);

                    if total_in < need {
                        let pending_stats =
                            pending_balance_stats(&utxos, &reserved_outpoints, &active_pending);
                        let spendable_utxo_count = utxos
                            .iter()
                            .filter(|u| u.value > 0)
                            .filter(|u| !u.txid.is_empty())
                            .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                            .filter(|u| is_wallet_utxo_spendable(u, cur_h))
                            .count();

                        if selected.len() >= MAX_WALLET_SEND_INPUTS {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                too_many_inputs_body(
                                    need,
                                    total_in,
                                    fee,
                                    cur_h,
                                    spendable_utxo_count,
                                    reserved_outpoints.len(),
                                    MAX_WALLET_SEND_INPUTS,
                                    &pending_stats,
                                )
                                .to_string(),
                            );
                            return;
                        }

                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            insufficient_funds_body(
                                need,
                                total_in,
                                fee,
                                cur_h,
                                spendable_utxo_count,
                                reserved_outpoints.len(),
                                &pending_stats,
                            )
                            .to_string(),
                        );
                        return;
                    }

                    let (_mempool_txids_pre_submit, mempool_reserved_pre_submit) =
                        match send_mempool_state_or_err(&wallet_path, daemon_rpc_port) {
                            Ok(state) => state,
                            Err(e) => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(502),
                                    json!({"error":"daemon_mempool_unreachable","detail":e})
                                        .to_string(),
                                );
                                return;
                            }
                        };
                    let pre_submit_conflicts =
                        selected_inputs_conflict_with_reserved(&selected, &mempool_reserved_pre_submit);
                    if !pre_submit_conflicts.is_empty() {
                        if let Err(e) = refresh_wallet_utxos_after_submit_conflict(
                            &wallet_path,
                            &addrs,
                            daemon_rpc_port,
                            &utxos,
                            last_sync_height.max(cur_h),
                        ) {
                            wwlog!(
                                "wallet_rpc: send_presubmit_refresh_failed wallet={} err={}",
                                wallet_public_name(&wallet_path),
                                e
                            );
                        }
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(409),
                            json!({
                                "error":"input_conflict_before_submit",
                                "detail":"selected_inputs_already_reserved_in_mempool",
                                "conflicts": pre_submit_conflicts,
                            })
                            .to_string(),
                        );
                        return;
                    }

                    let change = total_in - need;
                    let mut final_fee = fee;
                    let mut final_change = change;
                    if final_change > 0 && final_change <= DUST_CHANGE {
                        // Avoid creating tiny change output: add it to fee instead.
                        final_fee = match final_fee.checked_add(final_change) {
                            Some(v) => v,
                            None => {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(400),
                                    json!({"error":"invalid_fee","detail":"fee_overflow"})
                                        .to_string(),
                                );
                                return;
                            }
                        };
                        final_change = 0;
                    }

                    // Build tx object compatible with legacy python shape.
                    let vin: Vec<serde_json::Value> = selected
                        .iter()
                        .map(|u| {
                            json!({
                                "txid": u.utxo.txid,
                                "vout": u.utxo.vout,
                                "pubkey": "",
                                "sig": "",
                                "prev_addr": u.signer.addr,
                            })
                        })
                        .collect();

                    let mut vout =
                        vec![json!({"addr": to_addr, "address": to_addr, "value": amount})];
                    if final_change > 0 {
                        vout.push(json!({"addr": change_addr, "address": change_addr, "value": final_change}));
                    }

                    let mut tx = json!({"vin": vin, "vout": vout, "fee": final_fee});

                    let msg = match sighash(&tx) {
                        Ok(h) => h,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_sign_failed","detail":e}).to_string(),
                            );
                            return;
                        }
                    };

                    if let Some(vins) = tx.get_mut("vin").and_then(|x| x.as_array_mut()) {
                        for (vin, input) in vins.iter_mut().zip(selected.iter()) {
                            let sk_b = match hex::decode(&input.signer.sk_hex) {
                                Ok(b) => b,
                                Err(_) => {
                                    super::respond_json(
                                        request,
                                        tiny_http::StatusCode(500),
                                        json!({"error":"wallet_key_invalid","detail":"sk_hex"}).to_string(),
                                    );
                                    return;
                                }
                            };
                            if sk_b.len() != 32 {
                                super::respond_json(
                                    request,
                                    tiny_http::StatusCode(500),
                                    json!({"error":"wallet_key_invalid","detail":"sk_len"}).to_string(),
                                );
                                return;
                            }
                            let mut ent = [0u8; 32];
                            ent.copy_from_slice(&sk_b);
                            let sk = ed25519_dalek::SigningKey::from_bytes(&ent);
                            let sig = ed25519_dalek::Signer::sign(&sk, &msg);
                            if let Some(o) = vin.as_object_mut() {
                                o.insert(
                                    "pubkey".to_string(),
                                    serde_json::Value::String(input.signer.pub_hex.clone()),
                                );
                                o.insert(
                                    "sig".to_string(),
                                    serde_json::Value::String(hex::encode(sig.to_bytes())),
                                );
                            }
                        }
                    }

                    let submit = json!({"tx": tx});
                    let submit_body = match serde_json::to_vec(&submit) {
                        Ok(b) => b,
                        Err(e) => {
                            let d = format!("json_encode_failed: {}", e);
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"internal","detail":d}).to_string(),
                            );
                            return;
                        }
                    };

                    let mut reserved_inputs_after_select = reserved_inputs.clone();
                    append_selected_reserved_inputs(
                        &mut reserved_inputs_after_select,
                        &selected,
                        now_secs,
                    );
                    if let Err(e) = persist_runtime_reserved_inputs(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &reserved_inputs_after_select,
                    ) {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(500),
                            json!({"error":"wallet_state_persist_failed","detail":e}).to_string(),
                        );
                        return;
                    }

                    let resp_body = match super::http_post_local(
                        "127.0.0.1",
                        daemon_rpc_port,
                        "/submit_tx",
                        "application/json",
                        &submit_body,
                    ) {
                        Ok(b) => b,
                        Err(e) => {
                            release_selected_reserved_inputs(
                                &mut reserved_inputs_after_select,
                                &selected,
                            );
                            let _ = persist_runtime_reserved_inputs(
                                &wallet_path,
                                &utxos,
                                cur_h,
                                &reserved_inputs_after_select,
                            );
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                json!({"error":"daemon_unreachable","detail":e}).to_string(),
                            );
                            return;
                        }
                    };

                    let resp_v: serde_json::Value = match serde_json::from_str(&resp_body) {
                        Ok(v) => v,
                        Err(e) => {
                            release_selected_reserved_inputs(
                                &mut reserved_inputs_after_select,
                                &selected,
                            );
                            let _ = persist_runtime_reserved_inputs(
                                &wallet_path,
                                &utxos,
                                cur_h,
                                &reserved_inputs_after_select,
                            );
                            let d = format!("daemon_invalid_json: {}", e);
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                json!({"error":"daemon_bad_response","detail":d}).to_string(),
                            );
                            return;
                        }
                    };

                    if resp_v.get("ok").and_then(|x| x.as_bool()) != Some(true) {
                        // Pass through fee-floor errors as 422 so clients don't treat it as a gateway failure.
                        if resp_v.get("error").and_then(|x| x.as_str()) == Some("fee_too_low") {
                            release_selected_reserved_inputs(
                                &mut reserved_inputs_after_select,
                                &selected,
                            );
                            let _ = persist_runtime_reserved_inputs(
                                &wallet_path,
                                &utxos,
                                cur_h,
                                &reserved_inputs_after_select,
                            );
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(422),
                                resp_v.to_string(),
                            );
                            return;
                        }
                        if matches!(
                            resp_v.get("error").and_then(|x| x.as_str()),
                            Some("input_not_found" | "double_spend")
                        ) {
                            release_selected_reserved_inputs(
                                &mut reserved_inputs_after_select,
                                &selected,
                            );
                            let _ = persist_runtime_reserved_inputs(
                                &wallet_path,
                                &utxos,
                                cur_h,
                                &reserved_inputs_after_select,
                            );
                            if let Err(e) = refresh_wallet_utxos_after_submit_conflict(
                                &wallet_path,
                                &addrs,
                                daemon_rpc_port,
                                &utxos,
                                last_sync_height.max(cur_h),
                            ) {
                                wwlog!(
                                    "wallet_rpc: send_conflict_refresh_failed wallet={} err={}",
                                    wallet_public_name(&wallet_path),
                                    e
                                );
                            }
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(409),
                                resp_v.to_string(),
                            );
                            return;
                        }
                        release_selected_reserved_inputs(
                            &mut reserved_inputs_after_select,
                            &selected,
                        );
                        let _ = persist_runtime_reserved_inputs(
                            &wallet_path,
                            &utxos,
                            cur_h,
                            &reserved_inputs_after_select,
                        );
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(502),
                            json!({"error":"daemon_submit_failed","daemon":resp_v}).to_string(),
                        );
                        return;
                    }

                    let txid = resp_v
                        .get("txid")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let spent_inputs: Vec<super::PendingInput> = selected
                        .iter()
                        .map(|s| super::PendingInput {
                            txid: s.utxo.txid.clone(),
                            vout: s.utxo.vout,
                        })
                        .collect();

                    // Update wallet utxos: remove spent, add change UTXO (unconfirmed).
                    let mut new_utxos: Vec<super::Utxo> = utxos
                        .into_iter()
                        .filter(|u| {
                            !selected
                                .iter()
                                .any(|s| s.utxo.txid == u.txid && s.utxo.vout == u.vout)
                        })
                        .collect();

                    if final_change > 0 && !txid.is_empty() {
                        new_utxos.push(super::Utxo {
                            value: final_change,
                            height: 0,
                            coinbase: false,
                            address: change_addr.clone(),
                            txid: txid.clone(),
                            vout: 1,
                        });
                    }

                    let cur_h = match daemon_tip_height_with_retry(daemon_rpc_port, cur_h) {
                        Ok(h) => h,
                        Err(e) => {
                            wwlog!(
                                "wallet_rpc: send_tip_refresh_failed wallet={} txid={} fallback_height={} err={}",
                                wallet_public_name(&wallet_path),
                                txid,
                                cur_h,
                                e
                            );
                            cur_h
                        }
                    };

                    let pending_after = {
                        let g = super::wallet_lock_or_recover();
                        let mut pending = g
                            .as_ref()
                            .map(|ws| ws.pending_txs.clone())
                            .unwrap_or_default();
                        record_pending_send(
                            &mut pending,
                            &txid,
                            &to_addr,
                            amount,
                            final_fee,
                            final_change,
                            &spent_inputs,
                        );
                        pending
                    };
                    release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);

                    // Persist to disk and update in-memory together.
                    let persist_result = super::save_wallet_full_state(
                        &wallet_path,
                        &new_utxos,
                        cur_h,
                        &pending_after,
                        &reserved_inputs_after_select,
                    );

                    {
                        let mut g = super::wallet_lock_or_recover();
                        if let Some(ws) = g.as_mut() {
                            ws.utxos = new_utxos.clone();
                            ws.last_sync_height = cur_h;
                            ws.pending_txs = pending_after.clone();
                            ws.reserved_inputs = reserved_inputs_after_select.clone();
                        }
                    }

                    let body = send_success_body(
                        &txid,
                        amount,
                        final_fee,
                        final_change,
                        selected.len(),
                        cur_h,
                        persist_result,
                    );
                    if let Some(e) = body
                        .get("wallet_state_persist_error")
                        .and_then(|x| x.as_str())
                    {
                        wwlog!(
                            "wallet_rpc: send_state_persist_failed wallet={} txid={} err={}",
                            wallet_public_name(&wallet_path),
                            body.get("txid").and_then(|x| x.as_str()).unwrap_or("-"),
                            e
                        );
                    }
                    super::respond_json(request, tiny_http::StatusCode(200), body.to_string());

                    // Success path above responded with plain JSON.
                    // Convert to JSON-RPC response wrapper is non-trivial without refactor;
                    // keep HTTP-style send response under JSON-RPC for now.
                }

                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(404),
                        rpc_response_err(id, -32601, "method_not_found"),
                    );
                }
            }
        }

        "/open" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }

            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    wwlog!("wallet_rpc: open_request_rejected detail={}", e);
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };

            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    let d = format!("invalid_json: {}", e);
                    wwlog!("wallet_rpc: open_request_invalid_json detail={}", d);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":d}).to_string(),
                    );
                    return;
                }
            };

            let wallet_path = match v.get("wallet_path").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_wallet_path"}).to_string(),
                    );
                    return;
                }
            };

            let _passphrase = v
                .get("passphrase")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();

            match super::load_wallet_from_path(&wallet_path) {
                Ok(mut ws) => {
                    let passphrase = v.get("passphrase").and_then(|x| x.as_str()).unwrap_or("");
                    if !passphrase.is_empty() {
                        if let Err(e) = unlock_db_wallet_state(&mut ws, passphrase) {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                json!({"ok":false,"error":"wallet_unlock_failed","detail":e})
                                    .to_string(),
                            );
                            return;
                        }
                    }
                    let unlocked = !(ws.is_db && ws.locked);
                    let mut guard = super::wallet_lock_or_recover();
                    if let Some(existing) = guard.as_mut() {
                        super::clear_wallet_sensitive_state(existing);
                    }
                    *guard = Some(ws);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        json!({"ok":true,"wallet":wallet_public_name(&wallet_path),"unlocked":unlocked}).to_string(),
                    );
                }
                Err(e) => {
                    wedlog!("wallet_rpc: open_wallet_failed detail={}", e);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"ok":false,"error":"wallet_open_failed","detail":e}).to_string(),
                    );
                }
            }
        }

        // Bitcoin-ish convenience endpoints (HTTP, not JSON-RPC)
        "/createwallet" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }

            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };

            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    let d = format!("invalid_json: {}", e);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":d}).to_string(),
                    );
                    return;
                }
            };

            let wallet_path = match v.get("wallet_path").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_wallet_path"}).to_string(),
                    );
                    return;
                }
            };
            let overwrite = v
                .get("overwrite")
                .and_then(|x| x.as_bool())
                .unwrap_or(false);

            if fs::metadata(&wallet_path).is_ok() && !overwrite {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"wallet_exists","wallet":wallet_public_name(&wallet_path)})
                        .to_string(),
                );
                return;
            }

            if let Some(parent) = std::path::Path::new(&wallet_path).parent() {
                if let Err(e) = fs::create_dir_all(parent) {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"error":"wallet_create_failed","detail":format!("mkdir_failed: {}", e)}).to_string(),
                    );
                    return;
                }
            }

            if !db_wallet_path(&wallet_path) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"bad_request","detail":"wallet_path_must_end_with_.db_or_.dat"})
                        .to_string(),
                );
                return;
            }
            let passphrase = v
                .get("passphrase")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if let Err(code) = require_non_empty_passphrase(&passphrase) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"bad_request","detail":code}).to_string(),
                );
                return;
            }
            let mnemonic = match bip39::Mnemonic::generate_in(bip39::Language::English, 24) {
                Ok(m) => m,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"error":"wallet_create_failed","detail":format!("mnemonic_generate_failed: {}", e)}).to_string(),
                    );
                    return;
                }
            };
            let seed = mnemonic.to_seed("");
            let mnemonic_entropy = mnemonic.to_entropy();
            let (addr, sk_hex, pub_hex) =
                new_wallet_keypair_from_seed(net_from_name(net), &seed, 0);

            // Store seed + keys encrypted in SQLite wallet.db (locked-by-default at open).
            let db = match super::walletdb::WalletDb::create_new(
                &wallet_path,
                &passphrase,
                &seed,
                1,
            ) {
                Ok(d) => d,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"wallet_create_failed","detail":e})
                            .to_string(),
                    );
                    return;
                }
            };

            let sk_b = match hex::decode(&sk_hex) {
                Ok(b) => b,
                Err(_) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"error":"wallet_create_failed","detail":"sk_hex_invalid"})
                            .to_string(),
                    );
                    return;
                }
            };
            if sk_b.len() != 32 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(500),
                    json!({"error":"wallet_create_failed","detail":"sk_len_invalid"})
                        .to_string(),
                );
                return;
            }
            let mut ent = [0u8; 32];
            ent.copy_from_slice(&sk_b);
            if let Err(e) = db.insert_key_with_meta_atomic(
                &addr,
                &pub_hex,
                &ent,
                &passphrase,
                None,
                Some(&addr),
                Some(&mnemonic_entropy),
            ) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(500),
                    json!({"ok":false,"error":"wallet_create_failed","detail":e}).to_string(),
                );
                return;
            }

            // Auto-open wallet (locked state). Return mnemonic once.
            let ws = match super::load_wallet_from_path(&wallet_path) {
                Ok(ws) => ws,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"wallet_open_failed","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let mut guard = super::wallet_lock_or_recover();
            if let Some(existing) = guard.as_mut() {
                super::clear_wallet_sensitive_state(existing);
            }
            *guard = Some(ws);

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"ok":true,"wallet":wallet_public_name(&wallet_path),"mnemonic":mnemonic.to_string(),"pubkey":pub_hex,"address":addr}).to_string(),
            );
            return;
        }

        "/import_mnemonic" => {
            if request.method() != &tiny_http::Method::Post {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(405),
                    json!({"error":"method_not_allowed"}).to_string(),
                );
                return;
            }

            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };

            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    let d = format!("invalid_json: {}", e);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":d}).to_string(),
                    );
                    return;
                }
            };

            let wallet_path = match v.get("wallet_path").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_wallet_path"}).to_string(),
                    );
                    return;
                }
            };
            let passphrase = v
                .get("passphrase")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let mnemonic_s = match v.get("mnemonic").and_then(|x| x.as_str()) {
                Some(m) if !m.trim().is_empty() => m.trim().to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_mnemonic"}).to_string(),
                    );
                    return;
                }
            };
            let overwrite = v
                .get("overwrite")
                .and_then(|x| x.as_bool())
                .unwrap_or(false);

            if fs::metadata(&wallet_path).is_ok() && !overwrite {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"wallet_exists","wallet":wallet_public_name(&wallet_path)})
                        .to_string(),
                );
                return;
            }

            if let Some(parent) = std::path::Path::new(&wallet_path).parent() {
                if let Err(e) = fs::create_dir_all(parent) {
                    super::respond_json(
                request,
                tiny_http::StatusCode(500),
                json!({"error":"wallet_import_failed","detail":format!("mkdir_failed: {}", e)}).to_string(),
            );
                    return;
                }
            }

            // Parse mnemonic -> seed64 (BIP39 seed).
            use bip39::{Language, Mnemonic};
            let m = match Mnemonic::parse_in(Language::English, &mnemonic_s) {
                Ok(m) => m,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"mnemonic_invalid","detail":format!("{}", e)}).to_string(),
                    );
                    return;
                }
            };

            let seed = m.to_seed("");
            let mnemonic_entropy = m.to_entropy();
            let (addr, sk_hex, pub_hex) =
                new_wallet_keypair_from_seed(net_from_name(net), &seed, 0);

            // Import creates a DB wallet (SQLite) only.
            if !db_wallet_path(&wallet_path) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"bad_request","detail":"wallet_path_must_end_with_.db_or_.dat"})
                        .to_string(),
                );
                return;
            }
            if let Err(code) = require_non_empty_passphrase(&passphrase) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"bad_request","detail":code}).to_string(),
                );
                return;
            }

            let db =
                match super::walletdb::WalletDb::create_new(&wallet_path, &passphrase, &seed, 1) {
                    Ok(d) => d,
                    Err(e) => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(500),
                            json!({"error":"wallet_import_failed","detail":e}).to_string(),
                        );
                        return;
                    }
                };

            let sk_b = match hex::decode(&sk_hex) {
                Ok(b) => b,
                Err(_) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"error":"wallet_import_failed","detail":"sk_hex_invalid"})
                            .to_string(),
                    );
                    return;
                }
            };
            if sk_b.len() != 32 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(500),
                    json!({"error":"wallet_import_failed","detail":"sk_len_invalid"}).to_string(),
                );
                return;
            }
            let mut ent = [0u8; 32];
            ent.copy_from_slice(&sk_b);
            if let Err(e) = db.insert_key_with_meta_atomic(
                &addr,
                &pub_hex,
                &ent,
                &passphrase,
                None,
                Some(&addr),
                Some(&mnemonic_entropy),
            ) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(500),
                    json!({"error":"wallet_import_failed","detail":e}).to_string(),
                );
                return;
            }

            // Auto-open wallet (locked).
            {
                let mut g = super::wallet_lock_or_recover();
                *g = Some(super::WalletState {
                    wallet_path: wallet_path.clone(),
                    primary_address: addr.clone(),
                    keys: std::collections::BTreeMap::new(),
                    pubkeys: {
                        let mut m = std::collections::BTreeMap::new();
                        m.insert(addr.clone(), pub_hex.clone());
                        m
                    },
                    utxos: Vec::new(),
                    pending_txs: Vec::new(),
                    reserved_inputs: Vec::new(),
                    last_sync_height: 0,
                    seed_hex: None,
                    next_index: 1,
                    is_db: true,
                    locked: true,
                    db_passphrase: None,
                });
            }

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"ok":true,"wallet":wallet_public_name(&wallet_path),"address":addr})
                    .to_string(),
            );
            return;
        }

        // Canonical: GET /address
        // Legacy compatibility alias: GET /getaddress
        // Canonical HTTP wallet read endpoint: /address. /getaddress is legacy compatibility alias.
        "/address" | "/getaddress" => {
            if request.method() != &tiny_http::Method::Get {
                respond_method_not_allowed(request);
                return;
            }

            let g = super::wallet_lock_or_recover();
            let ws = match g.as_ref() {
                Some(w) => w,
                None => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"wallet_not_open"}).to_string(),
                    );
                    return;
                }
            };

            let addr = ws.primary_address.clone();
            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"address":addr}).to_string(),
            );
        }

        "/listaddresses" => {
            if request.method() != &tiny_http::Method::Get {
                respond_method_not_allowed(request);
                return;
            }

            let g = super::wallet_lock_or_recover();
            let ws = match g.as_ref() {
                Some(w) => w,
                None => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"wallet_not_open"}).to_string(),
                    );
                    return;
                }
            };

            let mut addrs: Vec<String> = ws.pubkeys.keys().cloned().collect();
            if addrs.is_empty() {
                addrs = ws.keys.keys().cloned().collect();
            }
            addrs.sort();
            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"primary": ws.primary_address, "addresses": addrs}).to_string(),
            );
            return;
        }

        "/getnewaddress" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }

            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let v: serde_json::Value = if body.is_empty() {
                json!({})
            } else {
                match serde_json::from_slice(&body) {
                    Ok(v) => v,
                    Err(e) => {
                        super::respond_json(request, tiny_http::StatusCode(400), json!({"ok":false,"error":"invalid_json","detail":format!("invalid_json: {}", e)}).to_string());
                        return;
                    }
                }
            };

            let mut g = super::wallet_lock_or_recover();
            let ws = match g.as_mut() {
                Some(w) => w,
                None => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"wallet_not_open"}).to_string(),
                    );
                    return;
                }
            };

            let passphrase = v.get("passphrase").and_then(|x| x.as_str());
            let (addr, _pub_hex, _sk_hex) = match derive_next_address_for_wallet(ws, passphrase) {
                Ok(v) => v,
                Err(e) => {
                    let status = if e == "wallet_locked" || e == "missing_passphrase" {
                        tiny_http::StatusCode(400)
                    } else {
                        tiny_http::StatusCode(500)
                    };
                    super::respond_json(
                        request,
                        status,
                        json!({"error":"wallet_new_address_failed","detail":e}).to_string(),
                    );
                    return;
                }
            };

            debug_assert!(ws.is_db, "wallet state should always be db-backed");

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"address":addr}).to_string(),
            );
        }

        // Canonical: GET /balance
        // Legacy compatibility alias: GET /getbalance
        // Canonical HTTP wallet read endpoint: /balance. /getbalance is legacy compatibility alias.
        "/balance" | "/getbalance" => {
            if request.method() != &tiny_http::Method::Get {
                respond_method_not_allowed(request);
                return;
            }
            let snapshot =
                match wallet_balance_snapshot(daemon_rpc_port) {
                    Ok(t) => t,
                    Err(e) => {
                        respond_http_error_detail(
                            request,
                            tiny_http::StatusCode(502),
                            wallet_refresh_error_code(&e),
                            e,
                        );
                        return;
                    }
                };
            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({
                    "balance": format_dut_i64(snapshot.balance_dut),
                    "balance_dut": snapshot.balance_dut,
                    "spendable": format_dut_i64(snapshot.spendable_dut),
                    "spendable_dut": snapshot.spendable_dut,
                    "reserved": format_dut_i64(snapshot.reserved_dut),
                    "reserved_dut": snapshot.reserved_dut,
                    "pending_send": format_dut_i64(snapshot.pending_send_dut),
                    "pending_send_dut": snapshot.pending_send_dut,
                    "pending_change": format_dut_i64(snapshot.pending_change_dut),
                    "pending_change_dut": snapshot.pending_change_dut,
                    "pending_txs": snapshot.pending_txs,
                    "unit": DISPLAY_UNIT,
                    "display_unit": DISPLAY_UNIT,
                    "base_unit": BASE_UNIT,
                    "decimals": DUTA_DECIMALS,
                    "utxos": snapshot.utxos,
                    "height": snapshot.height
                })
                .to_string(),
            );
        }

        "/getaddressbalance" => {
            if request.method() != &tiny_http::Method::Get {
                respond_method_not_allowed(request);
                return;
            }

            let (wallet_addrs, primary_address, wallet_path, wallet_utxos, last_sync_height) = {
                let g = super::wallet_lock_or_recover();
                let ws = match g.as_ref() {
                    Some(w) => w,
                    None => {
                        respond_wallet_not_open(request);
                        return;
                    }
                };
                (
                    if !ws.pubkeys.is_empty() {
                        ws.pubkeys.keys().cloned().collect::<HashSet<String>>()
                    } else {
                        ws.keys.keys().cloned().collect::<HashSet<String>>()
                    },
                    ws.primary_address.clone(),
                    ws.wallet_path.clone(),
                    ws.utxos.clone(),
                    ws.last_sync_height,
                )
            };
            if wallet_addrs.is_empty() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"wallet_no_address"}).to_string(),
                );
                return;
            }

            let addr = query_param(&url, "address").unwrap_or(primary_address.clone());
            if addr.is_empty() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"wallet_no_address"}).to_string(),
                );
                return;
            }
            if !wallet_addrs.contains(&addr) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({
                        "error":"unsupported_address",
                        "detail":"address_not_in_wallet",
                        "wallet_primary_address": primary_address,
                    })
                    .to_string(),
                );
                return;
            }

            let wallet_addrs_vec = wallet_addrs.iter().cloned().collect::<Vec<String>>();
            let (cur_h, refreshed_utxos) = match refresh_wallet_utxos_runtime(
                &wallet_addrs_vec,
                daemon_rpc_port,
                &wallet_utxos,
                last_sync_height,
            ) {
                Ok(v) => v,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        wallet_refresh_error_code(&e),
                        e,
                    );
                    return;
                }
            };

            let reserved_inputs = {
                let mut g = super::wallet_lock_or_recover();
                if let Some(ws) = g.as_mut() {
                    ws.utxos = refreshed_utxos.clone();
                    ws.last_sync_height = cur_h;
                    ws.reserved_inputs.clone()
                } else {
                    Vec::new()
                }
            };
            if let Err(e) = super::save_wallet_sync_state(
                &wallet_path,
                &refreshed_utxos,
                cur_h,
                &reserved_inputs,
            ) {
                wwlog!(
                    "wallet_rpc: address_balance_refresh_persist_failed wallet={} err={}",
                    wallet_public_name(&wallet_path),
                    e
                );
            }

            let utxos: Vec<super::Utxo> = refreshed_utxos
                .into_iter()
                .filter(|u| u.address == addr)
                .collect();

            let mut balance: i64 = 0;
            let mut spendable: i64 = 0;
            for u in utxos.iter() {
                let v = u.value;
                balance += v;
                if is_wallet_utxo_spendable(u, cur_h) {
                    spendable += v;
                }
            }

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({
                    "address": addr,
                    "balance": format_dut_i64(balance),
                    "balance_dut": balance,
                    "spendable": format_dut_i64(spendable),
                    "spendable_dut": spendable,
                    "unit": DISPLAY_UNIT,
                    "display_unit": DISPLAY_UNIT,
                    "base_unit": BASE_UNIT,
                    "decimals": DUTA_DECIMALS,
                    "utxos": utxos.len(),
                    "height": cur_h
                })
                .to_string(),
            );
        }

        "/sync" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }

            // Snapshot wallet state.
            let (wallet_path, addrs, utxos, pending_txs, reserved_inputs, last_sync_height) = {
                let g = super::wallet_lock_or_recover();
                let ws = match g.as_ref() {
                    Some(w) => w,
                    None => {
                        respond_wallet_not_open(request);
                        return;
                    }
                };
                (
                    ws.wallet_path.clone(),
                    if !ws.pubkeys.is_empty() {
                        ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                    } else {
                        ws.keys.keys().cloned().collect::<Vec<String>>()
                    },
                    ws.utxos.clone(),
                    ws.pending_txs.clone(),
                    ws.reserved_inputs.clone(),
                    ws.last_sync_height,
                )
            };

            if addrs.is_empty() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"wallet_no_address"}).to_string(),
                );
                return;
            }

            let (cur_h, utxos) = match refresh_wallet_utxos_runtime(
                &addrs,
                daemon_rpc_port,
                &utxos,
                last_sync_height,
            ) {
                Ok(x) => x,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        wallet_refresh_error_code(&e),
                        e,
                    );
                    return;
                }
            };

            let mut pending_txs = pending_txs;
            let reserved_inputs = reserved_inputs;
            sync_pending_txs_with_chain_and_mempool(
                &wallet_path,
                &addrs,
                &mut pending_txs,
                &[],
                daemon_rpc_port,
            );

            if let Err(e) = super::save_wallet_full_state(
                &wallet_path,
                &utxos,
                cur_h,
                &pending_txs,
                &reserved_inputs,
            ) {
                wwlog!(
                    "wallet_rpc: manual_sync_persist_failed wallet={} err={}",
                    wallet_public_name(&wallet_path),
                    e
                );
                respond_http_error_detail(
                    request,
                    tiny_http::StatusCode(500),
                    "wallet_state_persist_failed",
                    e,
                );
                return;
            }

            {
                let mut g = super::wallet_lock_or_recover();
                if let Some(ws) = g.as_mut() {
                    ws.utxos = utxos.clone();
                    ws.last_sync_height = cur_h;
                    ws.pending_txs = pending_txs.clone();
                    ws.reserved_inputs = reserved_inputs.clone();
                }
            }

            // Compute balance + spendable.
            let mut balance: i64 = 0;
            let mut spendable: i64 = 0;
            let mut reserved_outpoints = pending_reserved_outpoints(&pending_txs);
            reserved_outpoints.extend(explicit_reserved_outpoints(&reserved_inputs));
            for u in utxos.iter() {
                balance += u.value;
                if is_wallet_utxo_spendable(u, cur_h)
                    && !reserved_outpoints.contains(&(u.txid.clone(), u.vout))
                {
                    spendable += u.value;
                }
            }
            let pending_stats = pending_balance_stats(&utxos, &reserved_outpoints, &pending_txs);

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({
                    "ok": true,
                    "wallet": wallet_public_name(&wallet_path),
                    "height": cur_h,
                    "balance": format_dut_i64(balance),
                    "balance_dut": balance,
                    "spendable": format_dut_i64(spendable),
                    "spendable_dut": spendable,
                    "reserved": format_dut_i64(pending_stats.reserved_dut),
                    "reserved_dut": pending_stats.reserved_dut,
                    "pending_send": format_dut_i64(pending_stats.pending_send_dut),
                    "pending_send_dut": pending_stats.pending_send_dut,
                    "pending_change": format_dut_i64(pending_stats.pending_change_dut),
                    "pending_change_dut": pending_stats.pending_change_dut,
                    "pending_txs": pending_txs.len(),
                    "sync_scope": "confirmed_chain_plus_pending_reconcile",
                    "unit": DISPLAY_UNIT,
                    "display_unit": DISPLAY_UNIT,
                    "base_unit": BASE_UNIT,
                    "decimals": DUTA_DECIMALS,
                    "utxos": utxos.len()
                })
                .to_string(),
            );
        }

        "/lock" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }
            let mut g = super::wallet_lock_or_recover();
            let ws = match g.as_mut() {
                Some(w) => w,
                None => {
                    respond_wallet_not_open(request);
                    return;
                }
            };
            debug_assert!(ws.is_db, "wallet state should always be db-backed");
            super::clear_wallet_sensitive_state(ws);
            ws.locked = true;
            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"ok":true,"locked":true}).to_string(),
            );
            return;
        }

        "/unlock" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }
            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(request, tiny_http::StatusCode(400), json!({"ok":false,"error":"invalid_json","detail":format!("invalid_json: {}", e)}).to_string());
                    return;
                }
            };
            let passphrase = v
                .get("passphrase")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();

            let mut g = super::wallet_lock_or_recover();
            let ws = match g.as_mut() {
                Some(w) => w,
                None => {
                    respond_wallet_not_open(request);
                    return;
                }
            };
            debug_assert!(ws.is_db, "wallet state should always be db-backed");
            if let Err(e) = unlock_db_wallet_state(ws, &passphrase) {
                let mut passphrase = passphrase;
                passphrase.zeroize();
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"ok":false,"error":"wallet_unlock_failed","detail":e}).to_string(),
                );
                return;
            }
            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"ok":true,"locked":false}).to_string(),
            );
            let mut passphrase = passphrase;
            passphrase.zeroize();
            return;
        }
        "/migrate" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }
            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(request, tiny_http::StatusCode(400), json!({"ok":false,"error":"invalid_json","detail":format!("invalid_json: {}", e)}).to_string());
                    return;
                }
            };

            let json_wallet_path = match v.get("json_wallet_path").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_json_wallet_path"})
                            .to_string(),
                    );
                    return;
                }
            };
            let db_wallet_path = match v.get("db_wallet_path").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_db_wallet_path"})
                            .to_string(),
                    );
                    return;
                }
            };
            let passphrase = match v.get("passphrase").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_passphrase"}).to_string(),
                    );
                    return;
                }
            };

            let s = match fs::read_to_string(&json_wallet_path) {
                Ok(s) => s,
                Err(e) => {
                    super::respond_json(request, tiny_http::StatusCode(400), json!({"ok":false,"error":"wallet_read_failed","detail":format!("wallet_read_failed: {}", e)}).to_string());
                    return;
                }
            };
            let jf: serde_json::Value = match serde_json::from_str(&s) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(request, tiny_http::StatusCode(400), json!({"ok":false,"error":"wallet_json_invalid","detail":format!("wallet_json_invalid: {}", e)}).to_string());
                    return;
                }
            };

            let keys_obj = match jf.get("keys").and_then(|x| x.as_object()) {
                Some(o) if !o.is_empty() => o,
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"wallet_keys_empty"}).to_string(),
                    );
                    return;
                }
            };
            let next_index = jf.get("next_index").and_then(|x| x.as_u64()).unwrap_or(0) as i64;

            let seed_bytes = match decode_seed_hex_for_migration(&jf) {
                Ok(seed) => seed,
                Err(code) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"ok":false,"error":code}).to_string(),
                    );
                    return;
                }
            };

            let db = match super::walletdb::WalletDb::create_new(
                &db_wallet_path,
                &passphrase,
                &seed_bytes,
                next_index,
            ) {
                Ok(d) => d,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"wallet_create_failed","detail":e}).to_string(),
                    );
                    return;
                }
            };

            // migrate each key: addr -> sk_hex
            for (addr, skv) in keys_obj.iter() {
                let sk_hex = match skv.as_str() {
                    Some(s) => s,
                    None => continue,
                };
                let sk_b = match hex::decode(sk_hex) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                if sk_b.len() != 32 {
                    continue;
                }
                let mut ent = [0u8; 32];
                ent.copy_from_slice(&sk_b);
                let sk = ed25519_dalek::SigningKey::from_bytes(&ent);
                let pub_hex = hex::encode(sk.verifying_key().to_bytes());
                if let Err(e) = db.insert_key_encrypted(addr, &pub_hex, &ent, &passphrase) {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"wallet_migrate_failed","detail":e}).to_string(),
                    );
                    return;
                }
            }

            let ws = match super::load_wallet_from_path(&db_wallet_path) {
                Ok(ws) => ws,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"wallet_open_failed","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let mut guard = super::wallet_lock_or_recover();
            if let Some(existing) = guard.as_mut() {
                super::clear_wallet_sensitive_state(existing);
            }
            *guard = Some(ws);

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"ok":true,"wallet":wallet_public_name(&db_wallet_path)}).to_string(),
            );
            return;
        }

        "/export_mnemonic" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }
            if !secret_export_enabled() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(403),
                    json!({"ok":false,"error":"secret_export_disabled","detail":"set DUTA_WALLET_ENABLE_SECRET_EXPORT=1 only for offline recovery workflows"}).to_string(),
                );
                return;
            }
            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(request, tiny_http::StatusCode(400), json!({"ok":false,"error":"invalid_json","detail":format!("invalid_json: {}", e)}).to_string());
                    return;
                }
            };
            let passphrase = match v.get("passphrase").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_passphrase"}).to_string(),
                    );
                    return;
                }
            };

            let g = super::wallet_lock_or_recover();
            let ws = match g.as_ref() {
                Some(w) => w,
                None => {
                    respond_wallet_not_open(request);
                    return;
                }
            };
            debug_assert!(ws.is_db, "wallet state should always be db-backed");
            let db = match super::walletdb::WalletDb::open(&ws.wallet_path) {
                Ok(d) => d,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"wallet_open_failed","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let entropy = match db.read_mnemonic_entropy() {
                Ok(Some(v)) => v,
                Ok(None) => {
                    let seed = match db.decrypt_seed(&passphrase) {
                        Ok(s) => s,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                json!({"ok":false,"error":"wallet_unlock_failed","detail":e})
                                    .to_string(),
                            );
                            return;
                        }
                    };
                    if seed.is_empty() {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"no_seed"}).to_string(),
                        );
                        return;
                    }
                    let seed_len = seed.len();
                    if !matches!(seed_len, 16 | 20 | 24 | 28 | 32) {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"mnemonic_unavailable","detail":"mnemonic_entropy_missing_for_wallet"}).to_string(),
                        );
                        return;
                    }
                    seed
                }
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"wallet_open_failed","detail":e}).to_string(),
                    );
                    return;
                }
            };

            use bip39::{Language, Mnemonic};
            let mnemonic = match Mnemonic::from_entropy_in(Language::English, &entropy) {
                Ok(m) => m,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"mnemonic_failed","detail":format!("{}", e)})
                            .to_string(),
                    );
                    return;
                }
            };
            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"ok":true,"mnemonic":mnemonic.to_string()}).to_string(),
            );
            return;
        }

        "/change_passphrase" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }
            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"ok":false,"error":"invalid_json","detail":format!("invalid_json: {}", e)}).to_string(),
                    );
                    return;
                }
            };

            let wallet_path = match v.get("wallet_path").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_wallet_path"}).to_string(),
                    );
                    return;
                }
            };
            let old_passphrase = match v.get("old_passphrase").and_then(|x| x.as_str()) {
                Some(p) => p.to_string(),
                _ => String::new(),
            };
            let new_passphrase = match v.get("new_passphrase").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_new_passphrase"})
                            .to_string(),
                    );
                    return;
                }
            };

            let db = match super::walletdb::WalletDb::open(&wallet_path) {
                Ok(d) => d,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"wallet_open_failed","detail":e}).to_string(),
                    );
                    return;
                }
            };
            if let Err(e) = db.change_passphrase(&old_passphrase, &new_passphrase) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"ok":false,"error":"change_passphrase_failed","detail":e}).to_string(),
                );
                return;
            }

            let mut g = super::wallet_lock_or_recover();
            if let Some(ws) = g.as_mut() {
                if ws.is_db && ws.wallet_path == wallet_path {
                    ws.db_passphrase = Some(new_passphrase.clone());
                }
            }

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"ok":true,"wallet":wallet_public_name(&wallet_path)}).to_string(),
            );
            return;
        }

        "/export_seed" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }
            if !secret_export_enabled() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(403),
                    json!({"ok":false,"error":"secret_export_disabled","detail":"set DUTA_WALLET_ENABLE_SECRET_EXPORT=1 only for offline recovery workflows"}).to_string(),
                );
                return;
            }
            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"ok":false,"error":"invalid_json","detail":format!("invalid_json: {}", e)}).to_string(),
                    );
                    return;
                }
            };
            let passphrase = match v.get("passphrase").and_then(|x| x.as_str()) {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"missing_passphrase"}).to_string(),
                    );
                    return;
                }
            };

            let g = super::wallet_lock_or_recover();
            let ws = match g.as_ref() {
                Some(w) => w,
                None => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"wallet_not_open"}).to_string(),
                    );
                    return;
                }
            };

            debug_assert!(ws.is_db, "wallet state should always be db-backed");
            let db = match super::walletdb::WalletDb::open(&ws.wallet_path) {
                Ok(d) => d,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"ok":false,"error":"wallet_open_failed","detail":e}).to_string(),
                    );
                    return;
                }
            };
            let seed = match db.decrypt_seed(&passphrase) {
                Ok(s) => s,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"ok":false,"error":"wallet_unlock_failed","detail":e})
                            .to_string(),
                    );
                    return;
                }
            };
            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({"ok":true,"seed_hex":hex::encode(seed)}).to_string(),
            );
            return;
        }

        "/send_plan" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }

            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };

            let req: SendPlanRequest = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    let d = format!("invalid_json: {}", e);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_json","detail":d}).to_string(),
                    );
                    return;
                }
            };

            let amount = match parse_required_amount_param(req.amount.as_ref(), req.amount_dut) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_amount","detail":e}).to_string(),
                    );
                    return;
                }
            };
            if amount <= 0 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"invalid_amount","detail":"amount_must_be_positive"})
                        .to_string(),
                );
                return;
            }
            let requested_fee = match parse_optional_fee_param(req.fee.as_ref(), req.fee_dut) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_fee","detail":e}).to_string(),
                    );
                    return;
                }
            };
            if requested_fee < 0 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"invalid_fee","detail":"fee_must_be_non_negative"}).to_string(),
                );
                return;
            }
            if requested_fee > DEFAULT_MAX_WALLET_FEE_DUT {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    fee_too_high_body(requested_fee, DEFAULT_MAX_WALLET_FEE_DUT).to_string(),
                );
                return;
            }
            let fee = requested_fee;

            let requested_outputs = req.outputs.filter(|count| *count > 0);
            let need = match amount.checked_add(fee) {
                Some(v) => v,
                None => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"bad_request","detail":"amount_overflow"}).to_string(),
                    );
                    return;
                }
            };

            let (wallet_path, addrs, mut utxos, pending_txs, reserved_inputs, last_sync_height) = {
                let g = super::wallet_lock_or_recover();
                let ws = match g.as_ref() {
                    Some(w) => w,
                    None => {
                        respond_wallet_not_open(request);
                        return;
                    }
                };
                (
                    ws.wallet_path.clone(),
                    if !ws.pubkeys.is_empty() {
                        ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                    } else {
                        ws.keys.keys().cloned().collect::<Vec<String>>()
                    },
                    ws.utxos.clone(),
                    ws.pending_txs.clone(),
                    ws.reserved_inputs.clone(),
                    ws.last_sync_height,
                )
            };

            let (cur_h, new_utxos) = match refresh_wallet_utxos_runtime(
                &addrs,
                daemon_rpc_port,
                &utxos,
                last_sync_height,
            ) {
                Ok(v) => v,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "wallet_state_refresh_failed",
                        e,
                    );
                    return;
                }
            };
            utxos = new_utxos;
            if let Err(e) =
                persist_runtime_reserved_inputs(&wallet_path, &utxos, cur_h, &reserved_inputs)
            {
                wwlog!(
                    "wallet_rpc: send_plan_refresh_persist_failed wallet={} err={}",
                    wallet_public_name(&wallet_path),
                    e
                );
            }

            let mut active_pending = pending_txs.clone();
            let mut reserved_inputs = reserved_inputs;
            let reserved_outpoints = match reconcile_pending_and_reserved_state(
                &wallet_path,
                &utxos,
                cur_h,
                &mut active_pending,
                &mut reserved_inputs,
                daemon_rpc_port,
            ) {
                Ok(v) => v,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "daemon_mempool_unreachable",
                        e,
                    );
                    return;
                }
            };

            let spendable_values: Vec<i64> = utxos
                .iter()
                .filter(|u| u.value > 0)
                .filter(|u| !u.txid.is_empty())
                .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                .filter(|u| is_wallet_utxo_spendable(u, cur_h))
                .map(|u| u.value)
                .collect();
            let spendable_balance_dut: i64 = spendable_values.iter().sum();
            let pending_stats =
                pending_balance_stats(&utxos, &reserved_outpoints, &active_pending);
            let plan = simulate_send_plan(
                &spendable_values,
                need,
                MAX_WALLET_SEND_INPUTS,
                requested_outputs,
            );
            let failure = if plan.requested_outputs_fit {
                serde_json::Value::Null
            } else {
                insufficient_funds_body(
                    need,
                    plan.last_total_in,
                    fee,
                    cur_h,
                    spendable_values.len(),
                    reserved_outpoints.len(),
                    &pending_stats,
                )
            };

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({
                    "ok": true,
                    "amount": format_dut_i64(amount),
                    "amount_dut": amount,
                    "fee": format_dut_i64(fee),
                    "fee_dut": fee,
                    "need": format_dut_i64(need),
                    "need_dut": need,
                    "requested_outputs": requested_outputs,
                    "max_outputs": plan.max_outputs,
                    "requested_outputs_fit": plan.requested_outputs_fit,
                    "spendable_utxos": spendable_values.len(),
                    "spendable_balance": format_dut_i64(spendable_balance_dut),
                    "spendable_balance_dut": spendable_balance_dut,
                    "reserved_outpoints": reserved_outpoints.len(),
                    "reserved_dut": pending_stats.reserved_dut,
                    "pending_send_dut": pending_stats.pending_send_dut,
                    "pending_change_dut": pending_stats.pending_change_dut,
                    "selected_input_count_for_failure": plan.selected_input_count_for_failure,
                    "failure": failure,
                    "unit": DISPLAY_UNIT,
                    "display_unit": DISPLAY_UNIT,
                    "base_unit": BASE_UNIT,
                    "decimals": DUTA_DECIMALS
                }).to_string(),
            );
        }

        "/sendmany_plan" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }

            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };

            let req: SendManyPlanRequest = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    let d = format!("invalid_json: {}", e);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_json","detail":d}).to_string(),
                    );
                    return;
                }
            };

            let fee_supplied = req.fee.is_some() || req.fee_dut.is_some();
            let requested_fee = match parse_optional_fee_param(req.fee.as_ref(), req.fee_dut) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_fee","detail":e}).to_string(),
                    );
                    return;
                }
            };
            if requested_fee < 0 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"invalid_fee","detail":"fee_must_be_non_negative"}).to_string(),
                );
                return;
            }
            if requested_fee > DEFAULT_MAX_WALLET_FEE_DUT {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    fee_too_high_body(requested_fee, DEFAULT_MAX_WALLET_FEE_DUT).to_string(),
                );
                return;
            }

            let (recipients, total_amount) = match parse_sendmany_recipients(&req.outputs, net) {
                Ok(v) => v,
                Err((error, detail)) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        if detail.is_empty() {
                            json!({"error":error}).to_string()
                        } else {
                            json!({"error":error,"detail":detail}).to_string()
                        },
                    );
                    return;
                }
            };

            let (wallet_path, change_addr, addrs, signers_by_pkh, signers_by_addr, mut utxos, pending_txs, reserved_inputs, last_sync_height) = {
                let g = super::wallet_lock_or_recover();
                let ws = match g.as_ref() {
                    Some(w) => w,
                    None => {
                        respond_wallet_not_open(request);
                        return;
                    }
                };
                if ws.is_db && ws.locked {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"wallet_locked"}).to_string(),
                    );
                    return;
                }
                let change_addr = if !ws.primary_address.is_empty() {
                    ws.primary_address.clone()
                } else {
                    ws.keys.keys().next().cloned().unwrap_or_default()
                };
                let addrs = if !ws.pubkeys.is_empty() {
                    ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                } else {
                    ws.keys.keys().cloned().collect::<Vec<String>>()
                };
                (
                    ws.wallet_path.clone(),
                    change_addr,
                    addrs,
                    match wallet_signers_by_pkh(ws) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_key_missing","detail":e}).to_string(),
                            );
                            return;
                        }
                    },
                    match wallet_signers_by_addr(ws) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_key_missing","detail":e}).to_string(),
                            );
                            return;
                        }
                    },
                    ws.utxos.clone(),
                    ws.pending_txs.clone(),
                    ws.reserved_inputs.clone(),
                    ws.last_sync_height,
                )
            };

            if change_addr.is_empty() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"wallet_no_address"}).to_string(),
                );
                return;
            }

            let (cur_h, new_utxos) = match refresh_wallet_utxos_runtime(
                &addrs,
                daemon_rpc_port,
                &utxos,
                last_sync_height,
            ) {
                Ok(v) => v,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "wallet_state_refresh_failed",
                        e,
                    );
                    return;
                }
            };
            utxos = new_utxos;
            if let Err(e) =
                persist_runtime_reserved_inputs(&wallet_path, &utxos, cur_h, &reserved_inputs)
            {
                wwlog!(
                    "wallet_rpc: sendmany_plan_refresh_persist_failed wallet={} err={}",
                    wallet_public_name(&wallet_path),
                    e
                );
            }

            let mut active_pending = pending_txs.clone();
            let mut reserved_inputs = reserved_inputs;
            let reserved_outpoints = match reconcile_pending_and_reserved_state(
                &wallet_path,
                &utxos,
                cur_h,
                &mut active_pending,
                &mut reserved_inputs,
                daemon_rpc_port,
            ) {
                Ok(v) => v,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "daemon_mempool_unreachable",
                        e,
                    );
                    return;
                }
            };

            let mut spendable_utxos: Vec<OwnedInput> = Vec::new();
            for utxo in utxos
                .iter()
                .filter(|u| u.value > 0)
                .filter(|u| !u.txid.is_empty())
                .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                .filter(|u| is_wallet_utxo_spendable(u, cur_h))
            {
                match resolve_owned_input(
                    daemon_rpc_port,
                    utxo,
                    &signers_by_pkh,
                    &signers_by_addr,
                ) {
                    Ok(Some(owned)) => spendable_utxos.push(owned),
                    Ok(None) => {}
                    Err(e) => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(502),
                            json!({"error":"daemon_bad_response","detail":e}).to_string(),
                        );
                        return;
                    }
                }
            }

            let pending_stats = pending_balance_stats(&utxos, &reserved_outpoints, &active_pending);
            let spendable_utxo_count = spendable_utxos.len();
            let spendable_balance_dut: i64 = spendable_utxos.iter().map(|u| u.utxo.value).sum();
            let mut target_fee = requested_fee;
            let mut selected: Vec<OwnedInput> = Vec::new();
            let mut final_fee = 0i64;
            let mut final_change = 0i64;
            let mut min_relay_fee = 0i64;
            let mut tx_size = 0usize;
            let mut failure = serde_json::Value::Null;
            let mut can_send = false;

            for _ in 0..8 {
                let need = match total_amount.checked_add(target_fee) {
                    Some(v) => v,
                    None => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"bad_request","detail":"amount_overflow"}).to_string(),
                        );
                        return;
                    }
                };
                let (next_selected, next_total_in) =
                    select_inputs_for_need(&spendable_utxos, need, MAX_WALLET_SEND_INPUTS);
                if next_total_in < need {
                    failure = insufficient_funds_body(
                        need,
                        next_total_in,
                        target_fee,
                        cur_h,
                        spendable_utxo_count,
                        reserved_outpoints.len(),
                        &pending_stats,
                    );
                    break;
                }

                let (next_tx, next_final_fee, next_final_change, _next_change_vout) =
                    match sign_send_tx(&next_selected, &recipients, &change_addr, target_fee) {
                        Ok(v) => v,
                        Err(e) if e.starts_with("wallet_key_invalid:") => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_key_invalid","detail":e}).to_string(),
                            );
                            return;
                        }
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_sign_failed","detail":e}).to_string(),
                            );
                            return;
                        }
                    };
                let next_tx_size = match serde_json::to_vec(&next_tx) {
                    Ok(b) => b.len(),
                    Err(e) => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(500),
                            json!({"error":"internal","detail":format!("json_encode_failed: {}", e)}).to_string(),
                        );
                        return;
                    }
                };
                let next_min_relay_fee = relay_fee_for_tx_bytes(next_tx_size);
                selected = next_selected;
                final_fee = next_final_fee;
                final_change = next_final_change;
                min_relay_fee = next_min_relay_fee;
                tx_size = next_tx_size;

                if fee_supplied {
                    if final_fee < min_relay_fee {
                        failure = fee_too_low_body(final_fee, min_relay_fee, tx_size);
                    } else {
                        can_send = true;
                    }
                    break;
                }

                if final_fee >= min_relay_fee {
                    can_send = true;
                    break;
                }
                target_fee = min_relay_fee;
                if target_fee > DEFAULT_MAX_WALLET_FEE_DUT {
                    failure = fee_too_high_body(target_fee, DEFAULT_MAX_WALLET_FEE_DUT);
                    break;
                }
            }

            let need_dut = total_amount.checked_add(final_fee.max(target_fee)).unwrap_or(i64::MAX);
            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({
                    "ok": true,
                    "can_send": can_send,
                    "outputs": recipients.len(),
                    "inputs": selected.len(),
                    "amount": format_dut_i64(total_amount),
                    "amount_dut": total_amount,
                    "fee": format_dut_i64(final_fee.max(target_fee)),
                    "fee_dut": final_fee.max(target_fee),
                    "fee_auto": !fee_supplied,
                    "min_relay_fee": format_dut_i64(min_relay_fee),
                    "min_relay_fee_dut": min_relay_fee,
                    "need": format_dut_i64(need_dut),
                    "need_dut": need_dut,
                    "change": format_dut_i64(final_change),
                    "change_dut": final_change,
                    "size": tx_size,
                    "spendable_utxos": spendable_utxo_count,
                    "spendable_balance": format_dut_i64(spendable_balance_dut),
                    "spendable_balance_dut": spendable_balance_dut,
                    "reserved_outpoints": reserved_outpoints.len(),
                    "reserved_dut": pending_stats.reserved_dut,
                    "pending_send_dut": pending_stats.pending_send_dut,
                    "pending_change_dut": pending_stats.pending_change_dut,
                    "failure": failure,
                    "unit": DISPLAY_UNIT,
                    "display_unit": DISPLAY_UNIT,
                    "base_unit": BASE_UNIT,
                    "decimals": DUTA_DECIMALS
                }).to_string(),
            );
            return;
        }

        "/sendmany" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }

            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };

            let req: SendManyRequest = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    let d = format!("invalid_json: {}", e);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_json","detail":d}).to_string(),
                    );
                    return;
                }
            };

            if req.outputs.is_empty() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"missing_outputs"}).to_string(),
                );
                return;
            }

            let mut recipients: Vec<(String, i64)> = Vec::with_capacity(req.outputs.len());
            let mut total_amount = 0i64;
            for output in req.outputs.iter() {
                let to = output.to.trim();
                if duta_core::address::parse_address_for_network(net_from_name(net), to).is_none() {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_address","detail":"outputs.to"}).to_string(),
                    );
                    return;
                }
                let amount =
                    match parse_required_amount_param(output.amount.as_ref(), output.amount_dut) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                json!({"error":"invalid_amount","detail":e}).to_string(),
                            );
                            return;
                        }
                    };
                if amount <= 0 {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_amount","detail":"amount_must_be_positive"})
                            .to_string(),
                    );
                    return;
                }
                total_amount = match total_amount.checked_add(amount) {
                    Some(v) => v,
                    None => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"bad_request","detail":"amount_overflow"}).to_string(),
                        );
                        return;
                    }
                };
                recipients.push((to.to_string(), amount));
            }

            let fee_supplied = req.fee.is_some() || req.fee_dut.is_some();
            let requested_fee = match parse_optional_fee_param(req.fee.as_ref(), req.fee_dut) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_fee","detail":e}).to_string(),
                    );
                    return;
                }
            };
            if requested_fee < 0 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"invalid_fee","detail":"fee_must_be_non_negative"}).to_string(),
                );
                return;
            }
            if requested_fee > DEFAULT_MAX_WALLET_FEE_DUT {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    fee_too_high_body(requested_fee, DEFAULT_MAX_WALLET_FEE_DUT).to_string(),
                );
                return;
            }

            let _send_guard = wallet_send_lock_or_recover();
            let (
                wallet_path,
                change_addr,
                addrs,
                signers_by_pkh,
                signers_by_addr,
                mut utxos,
                pending_txs,
                reserved_inputs,
                last_sync_height,
            ) = {
                let g = super::wallet_lock_or_recover();
                let ws = match g.as_ref() {
                    Some(w) => w,
                    None => {
                        respond_wallet_not_open(request);
                        return;
                    }
                };
                if ws.is_db && ws.locked {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"wallet_locked"}).to_string(),
                    );
                    return;
                }
                let change_addr = if !ws.primary_address.is_empty() {
                    ws.primary_address.clone()
                } else {
                    ws.keys.keys().next().cloned().unwrap_or_default()
                };
                let addrs = if !ws.pubkeys.is_empty() {
                    ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                } else {
                    ws.keys.keys().cloned().collect::<Vec<String>>()
                };
                (
                    ws.wallet_path.clone(),
                    change_addr,
                    addrs,
                    match wallet_signers_by_pkh(ws) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_key_missing","detail":e}).to_string(),
                            );
                            return;
                        }
                    },
                    match wallet_signers_by_addr(ws) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_key_missing","detail":e}).to_string(),
                            );
                            return;
                        }
                    },
                    ws.utxos.clone(),
                    ws.pending_txs.clone(),
                    ws.reserved_inputs.clone(),
                    ws.last_sync_height,
                )
            };

            if change_addr.is_empty() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"wallet_no_address"}).to_string(),
                );
                return;
            }

            let (cur_h, new_utxos) = match refresh_wallet_utxos_runtime(
                &addrs,
                daemon_rpc_port,
                &utxos,
                last_sync_height,
            ) {
                Ok(v) => v,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "wallet_state_refresh_failed",
                        e,
                    );
                    return;
                }
            };
            utxos = new_utxos;
            if let Err(e) =
                persist_runtime_reserved_inputs(&wallet_path, &utxos, cur_h, &reserved_inputs)
            {
                wwlog!(
                    "wallet_rpc: sendmany_refresh_persist_failed wallet={} err={}",
                    wallet_public_name(&wallet_path),
                    e
                );
            }

            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let mut active_pending = pending_txs.clone();
            let mut reserved_inputs = reserved_inputs;
            let reserved_outpoints = match reconcile_pending_and_reserved_state(
                &wallet_path,
                &utxos,
                cur_h,
                &mut active_pending,
                &mut reserved_inputs,
                daemon_rpc_port,
            ) {
                Ok(v) => v,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "daemon_mempool_unreachable",
                        e,
                    );
                    return;
                }
            };

            let mut spendable_utxos: Vec<OwnedInput> = Vec::new();
            for utxo in utxos
                .iter()
                .filter(|u| u.value > 0)
                .filter(|u| !u.txid.is_empty())
                .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                .filter(|u| is_wallet_utxo_spendable(u, cur_h))
            {
                match resolve_owned_input(
                    daemon_rpc_port,
                    utxo,
                    &signers_by_pkh,
                    &signers_by_addr,
                ) {
                    Ok(Some(owned)) => spendable_utxos.push(owned),
                    Ok(None) => {}
                    Err(e) => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(502),
                            json!({"error":"daemon_bad_response","detail":e}).to_string(),
                        );
                        return;
                    }
                }
            }

            let mut target_fee = requested_fee;
            let mut selected: Vec<OwnedInput> = Vec::new();
            let mut final_fee = 0i64;
            let mut final_change = 0i64;
            let mut min_relay_fee = 0i64;
            let mut tx_size = 0usize;
            let mut change_vout = recipients.len() as u32;
            let mut tx = serde_json::Value::Null;
            for _ in 0..8 {
                let need = match total_amount.checked_add(target_fee) {
                    Some(v) => v,
                    None => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"bad_request","detail":"amount_overflow"}).to_string(),
                        );
                        return;
                    }
                };
                let (next_selected, next_total_in) =
                    select_inputs_for_need(&spendable_utxos, need, MAX_WALLET_SEND_INPUTS);
                if next_total_in < need {
                    let pending_stats =
                        pending_balance_stats(&utxos, &reserved_outpoints, &active_pending);
                    let spendable_utxo_count = utxos
                        .iter()
                        .filter(|u| u.value > 0)
                        .filter(|u| !u.txid.is_empty())
                        .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                        .filter(|u| is_wallet_utxo_spendable(u, cur_h))
                        .count();

                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        insufficient_funds_body(
                            need,
                            next_total_in,
                            target_fee,
                            cur_h,
                            spendable_utxo_count,
                            reserved_outpoints.len(),
                            &pending_stats,
                        )
                        .to_string(),
                    );
                    return;
                }

                let (next_tx, next_final_fee, next_final_change, next_change_vout) =
                    match sign_send_tx(&next_selected, &recipients, &change_addr, target_fee) {
                        Ok(v) => v,
                        Err(e) if e.starts_with("wallet_key_invalid:") => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_key_invalid","detail":e}).to_string(),
                            );
                            return;
                        }
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_sign_failed","detail":e}).to_string(),
                            );
                            return;
                        }
                    };
                let next_tx_size = match serde_json::to_vec(&next_tx) {
                    Ok(b) => b.len(),
                    Err(e) => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(500),
                            json!({"error":"internal","detail":format!("json_encode_failed: {}", e)}).to_string(),
                        );
                        return;
                    }
                };
                let next_min_relay_fee = relay_fee_for_tx_bytes(next_tx_size);

                selected = next_selected;
                tx = next_tx;
                final_fee = next_final_fee;
                final_change = next_final_change;
                change_vout = next_change_vout;
                tx_size = next_tx_size;
                min_relay_fee = next_min_relay_fee;

                if fee_supplied {
                    if final_fee < min_relay_fee {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(422),
                            {
                                let mut body = fee_too_low_body(final_fee, min_relay_fee, tx_size);
                                if let Some(obj) = body.as_object_mut() {
                                    obj.insert("ok".to_string(), json!(false));
                                }
                                body.to_string()
                            },
                        );
                        return;
                    }
                    break;
                }

                if final_fee >= min_relay_fee {
                    break;
                }
                target_fee = min_relay_fee;
                if target_fee > DEFAULT_MAX_WALLET_FEE_DUT {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        fee_too_high_body(target_fee, DEFAULT_MAX_WALLET_FEE_DUT).to_string(),
                    );
                    return;
                }
            }
            if !fee_supplied && final_fee < min_relay_fee {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(500),
                    json!({"error":"wallet_fee_estimation_failed","detail":"relay_fee_not_stable"}).to_string(),
                );
                return;
            }

            let (_mempool_txids_pre_submit, mempool_reserved_pre_submit) =
                match send_mempool_state_or_err(&wallet_path, daemon_rpc_port) {
                    Ok(state) => state,
                    Err(e) => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(502),
                            json!({"error":"daemon_mempool_unreachable","detail":e}).to_string(),
                        );
                        return;
                    }
                };
            let pre_submit_conflicts =
                selected_inputs_conflict_with_reserved(&selected, &mempool_reserved_pre_submit);
            if !pre_submit_conflicts.is_empty() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(409),
                    json!({
                        "error":"input_conflict_before_submit",
                        "detail":"selected_inputs_already_reserved_in_mempool",
                        "conflicts": pre_submit_conflicts,
                    })
                    .to_string(),
                );
                return;
            }

            let submit_body = match serde_json::to_vec(&json!({"tx": tx})) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"error":"internal","detail":format!("json_encode_failed: {}", e)}).to_string(),
                    );
                    return;
                }
            };

            let mut reserved_inputs_after_select = reserved_inputs.clone();
            append_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected, now_secs);
            if let Err(e) = persist_runtime_reserved_inputs(
                &wallet_path,
                &utxos,
                cur_h,
                &reserved_inputs_after_select,
            ) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(500),
                    json!({"error":"wallet_state_persist_failed","detail":e}).to_string(),
                );
                return;
            }

            let resp_body = match super::http_post_local(
                "127.0.0.1",
                daemon_rpc_port,
                "/submit_tx",
                "application/json",
                &submit_body,
            ) {
                Ok(b) => b,
                Err(e) => {
                    release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);
                    let _ = persist_runtime_reserved_inputs(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &reserved_inputs_after_select,
                    );
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "daemon_unreachable",
                        e,
                    );
                    return;
                }
            };

            let resp_v: serde_json::Value = match serde_json::from_str(&resp_body) {
                Ok(v) => v,
                Err(e) => {
                    release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);
                    let _ = persist_runtime_reserved_inputs(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &reserved_inputs_after_select,
                    );
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(502),
                        json!({"error":"daemon_bad_response","detail":format!("daemon_invalid_json: {}", e)}).to_string(),
                    );
                    return;
                }
            };

            if resp_v.get("ok").and_then(|x| x.as_bool()) != Some(true) {
                release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);
                let _ = persist_runtime_reserved_inputs(
                    &wallet_path,
                    &utxos,
                    cur_h,
                    &reserved_inputs_after_select,
                );
                super::respond_json(
                    request,
                    if resp_v.get("error").and_then(|x| x.as_str()) == Some("fee_too_low") {
                        tiny_http::StatusCode(422)
                    } else if matches!(
                        resp_v.get("error").and_then(|x| x.as_str()),
                        Some("input_not_found" | "double_spend")
                    ) {
                        tiny_http::StatusCode(409)
                    } else {
                        tiny_http::StatusCode(502)
                    },
                    if matches!(
                        resp_v.get("error").and_then(|x| x.as_str()),
                        Some("fee_too_low" | "input_not_found" | "double_spend")
                    ) {
                        resp_v.to_string()
                    } else {
                        json!({"error":"daemon_submit_failed","daemon":resp_v}).to_string()
                    },
                );
                return;
            }

            let txid = resp_v
                .get("txid")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let spent_inputs: Vec<super::PendingInput> = selected
                .iter()
                .map(|s| super::PendingInput {
                    txid: s.utxo.txid.clone(),
                    vout: s.utxo.vout,
                })
                .collect();
            let mut new_utxos: Vec<super::Utxo> = utxos
                .into_iter()
                .filter(|u| {
                    !selected
                        .iter()
                        .any(|s| s.utxo.txid == u.txid && s.utxo.vout == u.vout)
                })
                .collect();
            if final_change > 0 && !txid.is_empty() {
                new_utxos.push(super::Utxo {
                    value: final_change,
                    height: 0,
                    coinbase: false,
                    address: change_addr.clone(),
                    txid: txid.clone(),
                    vout: change_vout,
                });
            }
            let cur_h = match daemon_tip_height_with_retry(daemon_rpc_port, cur_h) {
                Ok(h) => h,
                Err(_) => cur_h,
            };
            let pending_after = {
                let g = super::wallet_lock_or_recover();
                let mut pending = g
                    .as_ref()
                    .map(|ws| ws.pending_txs.clone())
                    .unwrap_or_default();
                record_pending_send_many(
                    &mut pending,
                    &txid,
                    &recipients,
                    final_fee,
                    final_change,
                    &spent_inputs,
                );
                pending
            };
            release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);
            let persist_result = super::save_wallet_full_state(
                &wallet_path,
                &new_utxos,
                cur_h,
                &pending_after,
                &reserved_inputs_after_select,
            );
            {
                let mut g = super::wallet_lock_or_recover();
                if let Some(ws) = g.as_mut() {
                    ws.utxos = new_utxos.clone();
                    ws.last_sync_height = cur_h;
                    ws.pending_txs = pending_after.clone();
                    ws.reserved_inputs = reserved_inputs_after_select.clone();
                }
            }
            if let Err(e) = persist_result {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(500),
                    json!({
                        "ok": false,
                        "error": "wallet_state_persist_failed",
                        "detail": e,
                        "txid": txid,
                        "amount": format_dut_i64(total_amount),
                        "amount_dut": total_amount,
                        "fee": format_dut_i64(final_fee),
                        "fee_dut": final_fee,
                        "fee_auto": !fee_supplied,
                        "min_relay_fee": format_dut_i64(min_relay_fee),
                        "min_relay_fee_dut": min_relay_fee,
                        "size": tx_size,
                        "change": format_dut_i64(final_change),
                        "change_dut": final_change,
                        "outputs": recipients.len(),
                    }).to_string(),
                );
                return;
            }
            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({
                    "ok": true,
                    "txid": txid,
                    "amount": format_dut_i64(total_amount),
                    "amount_dut": total_amount,
                    "fee": format_dut_i64(final_fee),
                    "fee_dut": final_fee,
                    "fee_auto": !fee_supplied,
                    "min_relay_fee": format_dut_i64(min_relay_fee),
                    "min_relay_fee_dut": min_relay_fee,
                    "size": tx_size,
                    "change": format_dut_i64(final_change),
                    "change_dut": final_change,
                    "outputs": recipients.len(),
                    "inputs": selected.len(),
                    "height": cur_h,
                    "wallet_state_persisted": true,
                    "unit": DISPLAY_UNIT,
                    "display_unit": DISPLAY_UNIT,
                    "base_unit": BASE_UNIT,
                    "decimals": DUTA_DECIMALS
                }).to_string(),
            );
        }

        "/send" => {
            if request.method() != &tiny_http::Method::Post {
                respond_method_not_allowed(request);
                return;
            }

            let body = match super::read_body(&mut request) {
                Ok(b) => b,
                Err(e) => {
                    super::respond_json(
                        request,
                        status_for_body_err(&e),
                        json!({"ok":false,"error":"bad_request","detail":e}).to_string(),
                    );
                    return;
                }
            };

            let req: SendRequest = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(e) => {
                    let d = format!("invalid_json: {}", e);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_json","detail":d}).to_string(),
                    );
                    return;
                }
            };

            // Validate destination address (minimal sanity).
            let to = req.to.trim();
            if duta_core::address::parse_address_for_network(net_from_name(net), to).is_none() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"invalid_address","detail":"to"}).to_string(),
                );
                return;
            }
            let to_addr = to.to_string();

            // Validate amount / fee.
            let amount = match parse_required_amount_param(req.amount.as_ref(), req.amount_dut) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_amount","detail":e}).to_string(),
                    );
                    return;
                }
            };
            if amount <= 0 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"invalid_amount","detail":"amount_must_be_positive"})
                        .to_string(),
                );
                return;
            }
            let fee_supplied = req.fee.is_some() || req.fee_dut.is_some();
            // Fee: optional (defaults to core wallet floor), allow 0, disallow negative.
            let fee_in: i64 = match parse_optional_fee_param(req.fee.as_ref(), req.fee_dut) {
                Ok(v) => v,
                Err(e) => {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"invalid_fee","detail":e}).to_string(),
                    );
                    return;
                }
            };
            if fee_in < 0 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"invalid_fee","detail":"fee_must_be_non_negative"}).to_string(),
                );
                return;
            }
            let fee: i64 = fee_in;

            const MAX_FEE: i64 = DEFAULT_MAX_WALLET_FEE_DUT;
            if fee > MAX_FEE {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    fee_too_high_body(fee, MAX_FEE).to_string(),
                );
                return;
            }

            let _send_guard = wallet_send_lock_or_recover();

            // Snapshot wallet state.
            let (
                wallet_path,
                change_addr,
                addrs,
                signers_by_pkh,
                signers_by_addr,
                mut utxos,
                pending_txs,
                reserved_inputs,
                last_sync_height,
            ) = {
                let g = super::wallet_lock_or_recover();
                let ws = match g.as_ref() {
                    Some(w) => w,
                    None => {
                        respond_wallet_not_open(request);
                        return;
                    }
                };
                if ws.is_db && ws.locked {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({"error":"wallet_locked"}).to_string(),
                    );
                    return;
                }
                let change_addr = if !ws.primary_address.is_empty() {
                    ws.primary_address.clone()
                } else {
                    ws.keys.keys().next().cloned().unwrap_or_default()
                };
                let addrs = if !ws.pubkeys.is_empty() {
                    ws.pubkeys.keys().cloned().collect::<Vec<String>>()
                } else {
                    ws.keys.keys().cloned().collect::<Vec<String>>()
                };
                (
                    ws.wallet_path.clone(),
                    change_addr,
                    addrs,
                    match wallet_signers_by_pkh(ws) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_key_missing","detail":e}).to_string(),
                            );
                            return;
                        }
                    },
                    match wallet_signers_by_addr(ws) {
                        Ok(v) => v,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_key_missing","detail":e}).to_string(),
                            );
                            return;
                        }
                    },
                    ws.utxos.clone(),
                    ws.pending_txs.clone(),
                    ws.reserved_inputs.clone(),
                    ws.last_sync_height,
                )
            };

            if change_addr.is_empty() {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"wallet_no_address"}).to_string(),
                );
                return;
            }
            // Need daemon height for maturity calculation.
            let (cur_h, new_utxos) = match refresh_wallet_utxos_runtime(
                &addrs,
                daemon_rpc_port,
                &utxos,
                last_sync_height,
            ) {
                Ok(v) => v,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "wallet_state_refresh_failed",
                        e,
                    );
                    return;
                }
            };

            utxos = new_utxos;
            if let Err(e) =
                persist_runtime_reserved_inputs(&wallet_path, &utxos, cur_h, &reserved_inputs)
            {
                wwlog!(
                    "wallet_rpc: send_refresh_persist_failed wallet={} err={}",
                    wallet_public_name(&wallet_path),
                    e
                );
            }

            // Select spendable inputs.
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let mut active_pending = pending_txs.clone();
            let mut reserved_inputs = reserved_inputs;
            let reserved_outpoints = match reconcile_pending_and_reserved_state(
                &wallet_path,
                &utxos,
                cur_h,
                &mut active_pending,
                &mut reserved_inputs,
                daemon_rpc_port,
            ) {
                Ok(v) => v,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "daemon_mempool_unreachable",
                        e,
                    );
                    return;
                }
            };
            // Filter out stale UTXOs (wallet view may lag behind daemon UTXO set).
            let mut spendable_utxos: Vec<OwnedInput> = Vec::new();
            for utxo in utxos
                .iter()
                .filter(|u| u.value > 0)
                .filter(|u| !u.txid.is_empty())
                .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                .filter(|u| is_wallet_utxo_spendable(u, cur_h))
            {
                match resolve_owned_input(
                    daemon_rpc_port,
                    utxo,
                    &signers_by_pkh,
                    &signers_by_addr,
                ) {
                    Ok(Some(owned)) => spendable_utxos.push(owned),
                    Ok(None) => {}
                    Err(e) => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(502),
                            json!({"error":"daemon_bad_response","detail":e}).to_string(),
                        );
                        return;
                    }
                }
            }

            let pending_stats =
                pending_balance_stats(&utxos, &reserved_outpoints, &active_pending);
            let spendable_utxo_count = utxos
                .iter()
                .filter(|u| u.value > 0)
                .filter(|u| !u.txid.is_empty())
                .filter(|u| !reserved_outpoints.contains(&(u.txid.clone(), u.vout)))
                .filter(|u| is_wallet_utxo_spendable(u, cur_h))
                .count();
            let recipients = vec![(to_addr.clone(), amount)];
            let mut target_fee = fee;
            let mut selected: Vec<OwnedInput> = Vec::new();
            let mut final_fee = 0i64;
            let mut final_change = 0i64;
            let mut min_relay_fee = 0i64;
            let mut tx_size = 0usize;
            let mut tx = serde_json::Value::Null;
            for _ in 0..8 {
                let need = match amount.checked_add(target_fee) {
                    Some(v) => v,
                    None => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"bad_request","detail":"amount_overflow"}).to_string(),
                        );
                        return;
                    }
                };
                let (next_selected, next_total_in) =
                    select_inputs_for_need(&spendable_utxos, need, MAX_WALLET_SEND_INPUTS);
                if next_total_in < need {
                    if next_selected.len() >= MAX_WALLET_SEND_INPUTS {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            too_many_inputs_body(
                                need,
                                next_total_in,
                                target_fee,
                                cur_h,
                                spendable_utxo_count,
                                reserved_outpoints.len(),
                                MAX_WALLET_SEND_INPUTS,
                                &pending_stats,
                            ).to_string(),
                        );
                        return;
                    }
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        insufficient_funds_body(
                            need,
                            next_total_in,
                            target_fee,
                            cur_h,
                            spendable_utxo_count,
                            reserved_outpoints.len(),
                            &pending_stats,
                        )
                        .to_string(),
                    );
                    return;
                }
                let (next_tx, next_final_fee, next_final_change, _change_vout) =
                    match sign_send_tx(&next_selected, &recipients, &change_addr, target_fee) {
                        Ok(v) => v,
                        Err(e) if e.starts_with("wallet_key_invalid:") => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_key_invalid","detail":e}).to_string(),
                            );
                            return;
                        }
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(500),
                                json!({"error":"wallet_sign_failed","detail":e}).to_string(),
                            );
                            return;
                        }
                    };
                let next_tx_size = match serde_json::to_vec(&next_tx) {
                    Ok(b) => b.len(),
                    Err(e) => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(500),
                            json!({"error":"internal","detail":format!("json_encode_failed: {}", e)}).to_string(),
                        );
                        return;
                    }
                };
                let next_min_relay_fee = relay_fee_for_tx_bytes(next_tx_size);
                selected = next_selected;
                tx = next_tx;
                final_fee = next_final_fee;
                final_change = next_final_change;
                min_relay_fee = next_min_relay_fee;
                tx_size = next_tx_size;
                if fee_supplied {
                    if final_fee < min_relay_fee {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(422),
                            {
                                let mut body = fee_too_low_body(final_fee, min_relay_fee, tx_size);
                                if let Some(obj) = body.as_object_mut() {
                                    obj.insert("ok".to_string(), json!(false));
                                }
                                body.to_string()
                            },
                        );
                        return;
                    }
                    break;
                }
                if final_fee >= min_relay_fee {
                    break;
                }
                target_fee = min_relay_fee;
                if target_fee > MAX_FEE {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                    fee_too_high_body(target_fee, MAX_FEE).to_string(),
                    );
                    return;
                }
            }

            let (_mempool_txids_pre_submit, mempool_reserved_pre_submit) =
                match send_mempool_state_or_err(&wallet_path, daemon_rpc_port) {
                    Ok(state) => state,
                    Err(e) => {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(502),
                            json!({"error":"daemon_mempool_unreachable","detail":e}).to_string(),
                        );
                        return;
                    }
                };
            let pre_submit_conflicts =
                selected_inputs_conflict_with_reserved(&selected, &mempool_reserved_pre_submit);
            if !pre_submit_conflicts.is_empty() {
                if let Err(e) = refresh_wallet_utxos_after_submit_conflict(
                    &wallet_path,
                    &addrs,
                    daemon_rpc_port,
                    &utxos,
                    last_sync_height.max(cur_h),
                ) {
                    wwlog!(
                        "wallet_rpc: send_presubmit_refresh_failed wallet={} err={}",
                        wallet_public_name(&wallet_path),
                        e
                    );
                }
                super::respond_json(
                    request,
                    tiny_http::StatusCode(409),
                    json!({
                        "error":"input_conflict_before_submit",
                        "detail":"selected_inputs_already_reserved_in_mempool",
                        "conflicts": pre_submit_conflicts,
                    })
                    .to_string(),
                );
                return;
            }

            let submit = json!({"tx": tx});
            let submit_body = match serde_json::to_vec(&submit) {
                Ok(b) => b,
                Err(e) => {
                    let d = format!("json_encode_failed: {}", e);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(500),
                        json!({"error":"internal","detail":d}).to_string(),
                    );
                    return;
                }
            };

            let mut reserved_inputs_after_select = reserved_inputs.clone();
            append_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected, now_secs);
            if let Err(e) = persist_runtime_reserved_inputs(
                &wallet_path,
                &utxos,
                cur_h,
                &reserved_inputs_after_select,
            ) {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(500),
                    json!({"error":"wallet_state_persist_failed","detail":e}).to_string(),
                );
                return;
            }

            let resp_body = match super::http_post_local(
                "127.0.0.1",
                daemon_rpc_port,
                "/submit_tx",
                "application/json",
                &submit_body,
            ) {
                Ok(b) => b,
                Err(e) => {
                    release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);
                    let _ = persist_runtime_reserved_inputs(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &reserved_inputs_after_select,
                    );
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "daemon_unreachable",
                        e,
                    );
                    return;
                }
            };

            let resp_v: serde_json::Value = match serde_json::from_str(&resp_body) {
                Ok(v) => v,
                Err(e) => {
                    release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);
                    let _ = persist_runtime_reserved_inputs(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &reserved_inputs_after_select,
                    );
                    let d = format!("daemon_invalid_json: {}", e);
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(502),
                        json!({"error":"daemon_bad_response","detail":d}).to_string(),
                    );
                    return;
                }
            };

            if resp_v.get("ok").and_then(|x| x.as_bool()) != Some(true) {
                // Pass through fee-floor errors as 422 so clients don't treat it as a gateway failure.
                if resp_v.get("error").and_then(|x| x.as_str()) == Some("fee_too_low") {
                    release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);
                    let _ = persist_runtime_reserved_inputs(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &reserved_inputs_after_select,
                    );
                    super::respond_json(request, tiny_http::StatusCode(422), resp_v.to_string());
                    return;
                }
                if matches!(
                    resp_v.get("error").and_then(|x| x.as_str()),
                    Some("input_not_found" | "double_spend")
                ) {
                    release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);
                    let _ = persist_runtime_reserved_inputs(
                        &wallet_path,
                        &utxos,
                        cur_h,
                        &reserved_inputs_after_select,
                    );
                    if let Err(e) = refresh_wallet_utxos_after_submit_conflict(
                        &wallet_path,
                        &addrs,
                        daemon_rpc_port,
                        &utxos,
                        last_sync_height.max(cur_h),
                    ) {
                        wwlog!(
                            "wallet_rpc: send_conflict_refresh_failed wallet={} err={}",
                            wallet_public_name(&wallet_path),
                            e
                        );
                    }
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(409),
                        resp_v.to_string(),
                    );
                    return;
                }
                release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);
                let _ = persist_runtime_reserved_inputs(
                    &wallet_path,
                    &utxos,
                    cur_h,
                    &reserved_inputs_after_select,
                );
                super::respond_json(
                    request,
                    tiny_http::StatusCode(502),
                    json!({"error":"daemon_submit_failed","daemon":resp_v}).to_string(),
                );
                return;
            }

            let txid = resp_v
                .get("txid")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let spent_inputs: Vec<super::PendingInput> = selected
                .iter()
                .map(|s| super::PendingInput {
                    txid: s.utxo.txid.clone(),
                    vout: s.utxo.vout,
                })
                .collect();

            // Update wallet utxos: remove spent, add change UTXO (unconfirmed).
            let mut new_utxos: Vec<super::Utxo> = utxos
                .into_iter()
                .filter(|u| {
                    !selected
                        .iter()
                        .any(|s| s.utxo.txid == u.txid && s.utxo.vout == u.vout)
                })
                .collect();

            if final_change > 0 && !txid.is_empty() {
                new_utxos.push(super::Utxo {
                    value: final_change,
                    height: 0,
                    coinbase: false,
                    address: change_addr.clone(),
                    txid: txid.clone(),
                    vout: 1,
                });
            }

            let cur_h = match daemon_tip_height_with_retry(daemon_rpc_port, cur_h) {
                Ok(h) => h,
                Err(e) => {
                    wwlog!(
                        "wallet_rpc: send_tip_refresh_failed wallet={} txid={} fallback_height={} err={}",
                        wallet_public_name(&wallet_path),
                        txid,
                        cur_h,
                        e
                    );
                    cur_h
                }
            };

            let pending_after = {
                let g = super::wallet_lock_or_recover();
                let mut pending = g
                    .as_ref()
                    .map(|ws| ws.pending_txs.clone())
                    .unwrap_or_default();
                record_pending_send(
                    &mut pending,
                    &txid,
                    &to_addr,
                    amount,
                    final_fee,
                    final_change,
                    &spent_inputs,
                );
                pending
            };
            release_selected_reserved_inputs(&mut reserved_inputs_after_select, &selected);

                    let persist_result = super::save_wallet_full_state(
                        &wallet_path,
                        &new_utxos,
                        cur_h,
                        &pending_after,
                        &reserved_inputs_after_select,
                    );

                    {
                        let mut g = super::wallet_lock_or_recover();
                        if let Some(ws) = g.as_mut() {
                            ws.utxos = new_utxos.clone();
                            ws.last_sync_height = cur_h;
                            ws.pending_txs = pending_after.clone();
                            ws.reserved_inputs = reserved_inputs_after_select.clone();
                        }
                    }

                    if let Err(e) = persist_result {
                        wwlog!(
                            "wallet_rpc: send_state_persist_failed wallet={} txid={} err={}",
                            wallet_public_name(&wallet_path),
                            txid,
                            e
                        );
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(500),
                            json!({
                                "ok": false,
                                "error": "wallet_state_persist_failed",
                                "detail": e,
                                "txid": txid,
                                "amount": format_dut_i64(amount),
                                "amount_dut": amount,
                                "fee": format_dut_i64(final_fee),
                                "fee_dut": final_fee,
                                "fee_auto": !fee_supplied,
                                "min_relay_fee": format_dut_i64(min_relay_fee),
                                "min_relay_fee_dut": min_relay_fee,
                                "size": tx_size,
                                "change": format_dut_i64(final_change),
                                "change_dut": final_change,
                                "height": cur_h
                            })
                            .to_string(),
                        );
                        return;
                    }
                    let body = send_success_body(
                        &txid,
                        amount,
                        final_fee,
                        final_change,
                        selected.len(),
                        cur_h,
                        Ok(()),
                    );
                    let mut body = body;
                    body["fee_auto"] = json!(!fee_supplied);
                    body["min_relay_fee"] = json!(format_dut_i64(min_relay_fee));
                    body["min_relay_fee_dut"] = json!(min_relay_fee);
                    body["size"] = json!(tx_size);
                    super::respond_json(request, tiny_http::StatusCode(200), body.to_string());
        }

        _ => super::respond_json(
            request,
            tiny_http::StatusCode(404),
            json!({"error":"not_found"}).to_string(),
        ),
    }
}
