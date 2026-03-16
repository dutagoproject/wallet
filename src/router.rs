use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs;
use zeroize::Zeroize;

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
        blocks_from_pruned_error, db_wallet_path, decode_seed_hex_for_migration, net_from_name,
        net_from_wallet_path, query_param, require_non_empty_passphrase, resolve_owned_input,
        send_success_body, should_probe_daemon_utxo_presence, status_for_body_err,
        wallet_public_name, wallet_refresh_error_code, wallet_state_network, OwnedInput,
        WalletSigner,
    };
    use duta_core::netparams::Network;
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};

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
    fn send_success_body_stays_ok_when_persist_fails() {
        let body = send_success_body("tx123", 50, 1, 9, 2, 123, Err("disk_full".to_string()));
        assert_eq!(body.get("ok").and_then(|x| x.as_bool()), Some(true));
        assert_eq!(
            body.get("wallet_state_persisted").and_then(|x| x.as_bool()),
            Some(false)
        );
        assert_eq!(
            body.get("wallet_state_persist_error").and_then(|x| x.as_str()),
            Some("disk_full")
        );
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
    amount: i64,
    fee: i64,
    change: i64,
    inputs: usize,
    height: i64,
    persist_result: Result<(), String>,
) -> serde_json::Value {
    let mut body = json!({
        "ok": true,
        "txid": txid,
        "amount": amount,
        "fee": fee,
        "change": change,
        "inputs": inputs,
        "height": height,
        "wallet_state_persisted": persist_result.is_ok()
    });
    if let Err(e) = persist_result {
        body["wallet_state_persist_error"] = json!(e);
    }
    body
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

fn rebuild_wallet_utxos_via_blocks_from(
    addrs: &[String],
    daemon_rpc_port: u16,
) -> Result<(i64, Vec<super::Utxo>), String> {
    // Current chain tip.
    let cur_h = daemon_tip_height_with_retry(daemon_rpc_port, 0)?;

    let addr_set: HashSet<&str> = addrs.iter().map(|s| s.as_str()).collect();
    if addr_set.is_empty() {
        return Ok((cur_h, Vec::new()));
    }

    // Rebuild wallet UTXOs by scanning blocks via daemon RPC (/blocks_from).
    let mut map: HashMap<(String, u32), super::Utxo> = HashMap::new();
    let mut from: i64 = 0;
    let limit: i64 = 256;

    loop {
        let v = daemon_blocks_from_with_retry(daemon_rpc_port, from, limit)?;
        // Daemon may return {"error":"chain_unavailable"} when polling beyond tip.
        // Treat that as empty result (no more blocks).
        if v.get("error").and_then(|x| x.as_str()) == Some("chain_unavailable") {
            break;
        }
        if v.get("error").and_then(|x| x.as_str()) == Some("pruned") {
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
                    let oaddr = ov.get("addr").and_then(|x| x.as_str()).unwrap_or("");
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

fn scan_wallet_txs_via_blocks_from(
    addrs: &[String],
    daemon_rpc_port: u16,
) -> Result<
    (
        i64,
        Vec<(String, i64, i64, String, i64, bool, Vec<serde_json::Value>)>,
    ),
    String,
> {
    let addr_set: HashSet<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let cur_h = daemon_tip_height_with_retry(daemon_rpc_port, 0)?;

    let mut out: Vec<(String, i64, i64, String, i64, bool, Vec<serde_json::Value>)> = Vec::new();
    let mut from: i64 = 0;
    let limit: i64 = 256;

    loop {
        let v = daemon_blocks_from_with_retry(daemon_rpc_port, from, limit)?;
        if v.get("error").and_then(|x| x.as_str()) == Some("chain_unavailable") {
            break;
        }
        if v.get("error").and_then(|x| x.as_str()) == Some("pruned") {
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
                    let oaddr = ov.get("addr").and_then(|x| x.as_str()).unwrap_or("");
                    let val = ov.get("value").and_then(|x| x.as_i64()).unwrap_or(0);
                    if val <= 0 {
                        continue;
                    }
                    if addr_set.contains(oaddr) {
                        recv_total = recv_total.saturating_add(val);
                        recv_details
                            .push(json!({"category":"receive","address":oaddr,"amount":val}));
                    } else {
                        external_total = external_total.saturating_add(val);
                        send_details.push(json!({"category":"send","address":oaddr,"amount":-val}));
                    }
                }

                let wallet_input = vin.iter().any(|iv| {
                    let prev_addr = iv.get("prev_addr").and_then(|x| x.as_str()).unwrap_or("");
                    addr_set.contains(prev_addr)
                });

                if !(wallet_input || recv_total > 0) {
                    continue;
                }

                let fee = txv.get("fee").and_then(|x| x.as_i64()).unwrap_or(0);
                let (category, amount, details) = if wallet_input {
                    if external_total > 0 {
                        let amt = -(external_total.saturating_add(fee));
                        ("send".to_string(), amt, send_details)
                    } else {
                        ("move".to_string(), 0, Vec::new())
                    }
                } else {
                    ("receive".to_string(), recv_total, recv_details)
                };

                out.push((
                    txid.clone(),
                    bh,
                    block_time,
                    category,
                    amount,
                    is_coinbase,
                    details,
                ));
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

#[derive(serde::Deserialize)]
struct SendRequest {
    to: String,
    amount: i64,
    // Fee is optional:
    // - if omitted, default to 1 (legacy behavior)
    // - if provided, allow 0 (daemon enforces relay fee floor)
    fee: Option<i64>,
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

fn wallet_balance_snapshot(daemon_rpc_port: u16) -> Result<(i64, i64, i64, i64, usize), String> {
    // Snapshot wallet state (avoid holding lock during daemon RPC).
    let (wallet_path, addrs, mut utxos, last_sync_height) = {
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
            ws.last_sync_height,
        )
    };

    let cur_h = daemon_tip_height_with_retry(daemon_rpc_port, last_sync_height)?;

    // Auto-sync for correctness: rebuild when wallet is empty, when a tracked UTXO is
    // impossible at current tip, or when daemon no longer has one of the tracked outpoints.
    let needs_rebuild = (!addrs.is_empty() && cur_h > 0)
        && (cur_h > last_sync_height
            || utxos.is_empty()
            || utxos.iter().any(|u| u.height > cur_h)
            || utxos.iter().any(|u| {
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
            }));
    if needs_rebuild {
        match rebuild_wallet_utxos_via_blocks_from(&addrs, daemon_rpc_port) {
            Ok((_h, new_utxos)) => {
                utxos = new_utxos;

                let mut g = super::wallet_lock_or_recover();
                if let Some(ws) = g.as_mut() {
                    ws.utxos = utxos.clone();
                    ws.last_sync_height = cur_h;
                }

                if let Err(e) = super::save_wallet_sync_state(&wallet_path, &utxos, cur_h) {
                    wwlog!(
                        "wallet_rpc: balance_sync_persist_failed wallet={} err={}",
                        wallet_public_name(&wallet_path),
                        e
                    );
                }
            }
            Err(e) => {
                wwlog!(
                    "wallet_rpc: balance_rebuild_failed wallet={} err={}",
                    wallet_public_name(&wallet_path),
                    e
                );
                return Err(e);
            }
        }
    }

    const COINBASE_MATURITY: i64 = 60;
    let mut balance: i64 = 0;
    let mut spendable: i64 = 0;
    let mut immature: i64 = 0;

    for u in utxos.iter() {
        let v = u.value;
        balance += v;

        if u.coinbase {
            if (cur_h - u.height) >= COINBASE_MATURITY {
                spendable += v;
            } else {
                immature += v;
            }
        } else {
            spendable += v;
        }
    }

    Ok((balance, spendable, immature, cur_h, utxos.len()))
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
                        Ok((balance, spendable, immature, height, utxos_n)) => {
                            body["balance"] = json!(balance);
                            body["spendable"] = json!(spendable);
                            body["immature"] = json!(immature);
                            body["height"] = json!(height);
                            body["utxos"] = json!(utxos_n);
                            body["unit"] = json!("DUTA");
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
                    let (balance, spendable, immature, height, utxos_n) =
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

                    let unlocked = !(ws.is_db && ws.locked);

                    let result = json!({
                        "walletname": wallet_public_name(&ws.wallet_path),
                        "walletversion": 1,
                        "balance": balance,
                        "spendable_balance": spendable,
                        "immature_balance": immature,
                        "txcount": 0,
                        "keypoolsize": ws.keys.len(),
                        "unlocked": unlocked,
                        "height": height,
                        "utxos": utxos_n,
                        "unit": "DUTA"
                    });

                    super::respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        rpc_response_ok(id, result),
                    );
                }

                "getbalance" => {
                    let (_balance, spendable, _immature, _height, _utxos_n) =
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
                        rpc_response_ok(id, json!(spendable)),
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

                    let (_balance, _spendable, _immature, height, _utxos_n) =
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
                    let (cur_h, utxos) = {
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
                        (height, ws.utxos.clone())
                    };

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
                            "amount": u.value,
                            "confirmations": conf,
                            "spendable": !(u.coinbase && conf < 60),
                            "coinbase": u.coinbase
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

                    let addrs = {
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
                        addrs
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

                    let mut out: Vec<serde_json::Value> = Vec::new();
                    for (i, (txid, h, block_time, category, amt, coinbase, details)) in
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
                            "amount": amt,
                            "confirmations": conf,
                            "blockheight": h,
                            "time": block_time,
                            "timereceived": block_time,
                            "blocktime": block_time,
                            "coinbase": coinbase,
                            "details": details
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

                    let addrs = {
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
                        addrs
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

                    let mut found: Option<(i64, i64, String, i64, bool, Vec<serde_json::Value>)> =
                        None;
                    for (t, h, block_time, category, amt, cb, details) in txs.into_iter() {
                        if t == txid {
                            found = Some((h, block_time, category, amt, cb, details));
                            break;
                        }
                    }

                    let (h, block_time, category, amt, coinbase, details) = match found {
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
                        "amount": amt,
                        "confirmations": conf,
                        "blockheight": h,
                        "time": block_time,
                        "timereceived": block_time,
                        "blocktime": block_time,
                        "coinbase": coinbase,
                        "details": details
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
                    let amount = match params.get(1).and_then(|x| x.as_i64()) {
                        Some(v) => v,
                        None => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                rpc_response_err(id, -32602, "missing_amount"),
                            );
                            return;
                        }
                    };
                    let fee = params.get(2).and_then(|x| x.as_i64());
                    let req = SendRequest { to, amount, fee };
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
                    if req.amount <= 0 {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"invalid_amount","detail":"amount_must_be_positive"})
                                .to_string(),
                        );
                        return;
                    }
                    // Fee: optional (defaults to 1), allow 0, disallow negative.
                    let fee_in: i64 = req.fee.unwrap_or(1);
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

                    const MAX_FEE: i64 = 10_000;
                    if fee > MAX_FEE {
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({"error":"fee_too_high","max_fee":MAX_FEE,"fee":fee}).to_string(),
                        );
                        return;
                    }

                    // Snapshot wallet state.
                    let (wallet_path, change_addr, signers_by_pkh, signers_by_addr, utxos) = {
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
                        (
                            ws.wallet_path.clone(),
                            change_addr,
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
                    let cur_h = match daemon_tip_height_with_retry(daemon_rpc_port, 0) {
                        Ok(h) => h,
                        Err(e) => {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(502),
                                json!({"error":"daemon_unreachable","detail":e}).to_string(),
                            );
                            return;
                        }
                    };

                    // Select spendable inputs.
                    const COINBASE_MATURITY: i64 = 60;
                    const MAX_INPUTS: usize = 64;
                    const DUST_CHANGE: i64 = 1;
                    let mut selected = Vec::new();
                    let mut total_in: i64 = 0;
                    let need: i64 = match req.amount.checked_add(fee) {
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
                    // Filter out stale UTXOs (wallet view may lag behind daemon UTXO set).
                    let mut spendable_utxos: Vec<OwnedInput> = Vec::new();
                    for utxo in utxos
                        .iter()
                        .filter(|u| u.value > 0)
                        .filter(|u| !u.txid.is_empty())
                        .filter(|u| !(u.coinbase && (cur_h - u.height) < COINBASE_MATURITY))
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
                    if let Some(u) = spendable_utxos
                        .iter()
                        .find(|u| u.utxo.value == need)
                        .cloned()
                    {
                        selected.push(u.clone());
                        total_in = u.utxo.value;
                    } else {
                        // Largest-first to minimize input count.
                        spendable_utxos.sort_by(|a, b| b.utxo.value.cmp(&a.utxo.value));
                        for u in spendable_utxos.iter() {
                            if selected.len() >= MAX_INPUTS {
                                break;
                            }
                            selected.push(u.clone());
                            total_in += u.utxo.value;
                            if total_in >= need {
                                break;
                            }
                        }
                    }

                    if total_in < need {
                        if selected.len() >= MAX_INPUTS {
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(400),
                                json!({
                                    "error":"too_many_inputs",
                                    "max_inputs": MAX_INPUTS,
                                    "need": need,
                                    "have": total_in,
                                    "fee": fee,
                                    "height": cur_h
                                })
                                .to_string(),
                            );
                            return;
                        }

                        super::respond_json(
                            request,
                            tiny_http::StatusCode(400),
                            json!({
                                "error":"insufficient_funds",
                                "need": need,
                                "have": total_in,
                                "fee": fee,
                                "height": cur_h
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
                        vec![json!({"addr": to_addr, "address": to_addr, "value": req.amount})];
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

                    let resp_body = match super::http_post_local(
                        "127.0.0.1",
                        daemon_rpc_port,
                        "/submit_tx",
                        "application/json",
                        &submit_body,
                    ) {
                        Ok(b) => b,
                        Err(e) => {
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
                            super::respond_json(
                                request,
                                tiny_http::StatusCode(422),
                                resp_v.to_string(),
                            );
                            return;
                        }
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

                    // Persist to disk and update in-memory.
                    let persist_result =
                        super::save_wallet_sync_state(&wallet_path, &new_utxos, cur_h);

                    {
                        let mut g = super::wallet_lock_or_recover();
                        if let Some(ws) = g.as_mut() {
                            ws.utxos = new_utxos.clone();
                            ws.last_sync_height = cur_h;
                        }
                    }

                    let body = send_success_body(
                        &txid,
                        req.amount,
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
            let (balance, spendable, _immature, cur_h, utxos_n) =
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
                    "balance": balance,
                    "spendable": spendable,
                    "unit": "DUTA",
                    "utxos": utxos_n,
                    "height": cur_h
                })
                .to_string(),
            );
        }

        "/getaddressbalance" => {
            if request.method() != &tiny_http::Method::Get {
                respond_method_not_allowed(request);
                return;
            }

            let (wallet_addrs, primary_address) = {
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

            let (cur_h, utxos) =
                match rebuild_wallet_utxos_via_blocks_from(&vec![addr.clone()], daemon_rpc_port) {
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

            const COINBASE_MATURITY: i64 = 60;
            let mut balance: i64 = 0;
            let mut spendable: i64 = 0;
            for u in utxos.iter() {
                let v = u.value;
                balance += v;
                if u.coinbase {
                    if (cur_h - u.height) >= COINBASE_MATURITY {
                        spendable += v;
                    }
                } else {
                    spendable += v;
                }
            }

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({
                    "address": addr,
                    "balance": balance,
                    "spendable": spendable,
                    "unit": "DUTA",
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
            let (wallet_path, addrs) = {
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

            let (cur_h, utxos) = match rebuild_wallet_utxos_via_blocks_from(&addrs, daemon_rpc_port)
            {
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

            {
                let mut g = super::wallet_lock_or_recover();
                if let Some(ws) = g.as_mut() {
                    ws.utxos = utxos.clone();
                    ws.last_sync_height = cur_h;
                }
            }

            if let Err(e) = super::save_wallet_sync_state(&wallet_path, &utxos, cur_h) {
                wwlog!(
                    "wallet_rpc: manual_sync_persist_failed wallet={} err={}",
                    wallet_public_name(&wallet_path),
                    e
                );
            }

            // Compute balance + spendable.
            let mut balance: i64 = 0;
            let mut spendable: i64 = 0;
            for u in utxos.iter() {
                balance += u.value;
                let mature = if u.coinbase {
                    (cur_h - u.height) >= 60
                } else {
                    true
                };
                if mature {
                    spendable += u.value;
                }
            }

            super::respond_json(
                request,
                tiny_http::StatusCode(200),
                json!({
                    "ok": true,
                    "wallet": wallet_public_name(&wallet_path),
                    "height": cur_h,
                    "balance": balance,
                    "spendable": spendable,
                    "unit": "DUTA",
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
            if req.amount <= 0 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"invalid_amount","detail":"amount_must_be_positive"})
                        .to_string(),
                );
                return;
            }
            // Fee: optional (defaults to 1), allow 0, disallow negative.
            let fee_in: i64 = req.fee.unwrap_or(1);
            if fee_in < 0 {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"invalid_fee","detail":"fee_must_be_non_negative"}).to_string(),
                );
                return;
            }
            let fee: i64 = fee_in;

            const MAX_FEE: i64 = 10_000;
            if fee > MAX_FEE {
                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({"error":"fee_too_high","max_fee":MAX_FEE,"fee":fee}).to_string(),
                );
                return;
            }

            // Snapshot wallet state.
            let (wallet_path, change_addr, addrs, signers_by_pkh, signers_by_addr, mut utxos) = {
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
            let cur_h = match daemon_tip_height_with_retry(daemon_rpc_port, 0) {
                Ok(h) => h,
                Err(e) => {
                    respond_http_error_detail(
                        request,
                        tiny_http::StatusCode(502),
                        "daemon_unreachable",
                        e,
                    );
                    return;
                }
            };

            if !addrs.is_empty() && cur_h > 0 {
                match rebuild_wallet_utxos_via_blocks_from(&addrs, daemon_rpc_port) {
                    Ok((_h, new_utxos)) => {
                        utxos = new_utxos;

                        {
                            let mut g = super::wallet_lock_or_recover();
                            if let Some(ws) = g.as_mut() {
                                ws.utxos = utxos.clone();
                            }
                        }

                        if let Err(e) = super::save_wallet_utxos(&wallet_path, &utxos) {
                            wwlog!(
                                "wallet_rpc: send_rebuild_persist_failed wallet={} err={}",
                                wallet_public_name(&wallet_path),
                                e
                            );
                        }
                    }
                    Err(e) => {
                        wwlog!(
                            "wallet_rpc: send_rebuild_failed wallet={} err={}",
                            wallet_public_name(&wallet_path),
                            e
                        );
                        super::respond_json(
                            request,
                            tiny_http::StatusCode(502),
                            json!({"error":"wallet_state_refresh_failed","detail":e}).to_string(),
                        );
                        return;
                    }
                }
            }

            // Select spendable inputs.
            const COINBASE_MATURITY: i64 = 60;
            const MAX_INPUTS: usize = 64;
            const DUST_CHANGE: i64 = 1;
            let mut selected = Vec::new();
            let mut total_in: i64 = 0;
            let need: i64 = match req.amount.checked_add(fee) {
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
            // Filter out stale UTXOs (wallet view may lag behind daemon UTXO set).
            let mut spendable_utxos: Vec<OwnedInput> = Vec::new();
            for utxo in utxos
                .iter()
                .filter(|u| u.value > 0)
                .filter(|u| !u.txid.is_empty())
                .filter(|u| !(u.coinbase && (cur_h - u.height) < COINBASE_MATURITY))
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
            if let Some(u) = spendable_utxos
                .iter()
                .find(|u| u.utxo.value == need)
                .cloned()
            {
                selected.push(u.clone());
                total_in = u.utxo.value;
            } else {
                // Largest-first to minimize input count.
                spendable_utxos.sort_by(|a, b| b.utxo.value.cmp(&a.utxo.value));
                for u in spendable_utxos.iter() {
                    if selected.len() >= MAX_INPUTS {
                        break;
                    }
                    selected.push(u.clone());
                    total_in += u.utxo.value;
                    if total_in >= need {
                        break;
                    }
                }
            }

            if total_in < need {
                if selected.len() >= MAX_INPUTS {
                    super::respond_json(
                        request,
                        tiny_http::StatusCode(400),
                        json!({
                            "error":"too_many_inputs",
                            "max_inputs": MAX_INPUTS,
                            "need": need,
                            "have": total_in,
                            "fee": fee,
                            "height": cur_h
                        })
                        .to_string(),
                    );
                    return;
                }

                super::respond_json(
                    request,
                    tiny_http::StatusCode(400),
                    json!({
                        "error":"insufficient_funds",
                        "need": need,
                        "have": total_in,
                        "fee": fee,
                        "height": cur_h
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
                            json!({"error":"invalid_fee","detail":"fee_overflow"}).to_string(),
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

            let mut vout = vec![json!({"addr": to_addr, "address": to_addr, "value": req.amount})];
            if final_change > 0 {
                vout.push(
                    json!({"addr": change_addr, "address": change_addr, "value": final_change}),
                );
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

            let resp_body = match super::http_post_local(
                "127.0.0.1",
                daemon_rpc_port,
                "/submit_tx",
                "application/json",
                &submit_body,
            ) {
                Ok(b) => b,
                Err(e) => {
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
                    super::respond_json(request, tiny_http::StatusCode(422), resp_v.to_string());
                    return;
                }
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

            // Persist to disk and update in-memory.
            let persist_result = super::save_wallet_sync_state(&wallet_path, &new_utxos, cur_h);

            {
                let mut g = super::wallet_lock_or_recover();
                if let Some(ws) = g.as_mut() {
                    ws.utxos = new_utxos.clone();
                    ws.last_sync_height = cur_h;
                }
            }

            let body = send_success_body(
                &txid,
                req.amount,
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
        }

        _ => super::respond_json(
            request,
            tiny_http::StatusCode(404),
            json!({"error":"not_found"}).to_string(),
        ),
    }
}
