//! duta-wallet-cli: lightweight wallet RPC client (bitcoin-cli style)
//!
//! Build example:
//!   RUSTFLAGS="-Dwarnings" cargo build --release -p dutawalletd --bin duta-wallet-cli

use clap::{Parser, Subcommand};
use duta_core::amount::DUTA_DECIMALS;
use duta_core::amount::{parse_duta_to_dut_i64, DISPLAY_UNIT};
use duta_core::netparams::Network;
use serde_json::json;
use std::io::{Read, Write};
use std::net::TcpStream;

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD: &str = "\x1b[1m";
const ANSI_BLACK: &str = "\x1b[30m";
const ANSI_WHITE: &str = "\x1b[37m";
const ANSI_BG_BLUE: &str = "\x1b[44m";
const ANSI_BG_GREEN: &str = "\x1b[42m";
const ANSI_BG_YELLOW: &str = "\x1b[43m";
const ANSI_BG_RED: &str = "\x1b[41m";

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

#[derive(Parser, Debug)]
#[command(name = "duta-wallet-cli", about = "DUTA wallet CLI (RPC client)")]
struct Args {
    /// Use testnet (default RPC: 127.0.0.1:18084)
    #[arg(long)]
    testnet: bool,

    /// Use stagenet (default RPC: 127.0.0.1:17084)
    #[arg(long)]
    stagenet: bool,

    /// Wallet RPC address (host:port). Overrides network defaults.
    #[arg(long, default_value = "")]
    rpc: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug, Clone)]
enum Cmd {
    /// GET /health
    Health,
    /// GET /info
    Info,

    /// POST /createwallet
    ///
    /// For encrypted SQLite wallet (.db), passphrase is REQUIRED.
    Createwallet {
        /// Wallet name (bitcoin-cli style) OR wallet file path.
        ///
        /// If a name is given (no "/" in it), it maps to <datadir>/<name>.db where <datadir>=~/.duta/<network>.
        wallet: String,
        /// Overwrite existing wallet file
        #[arg(long)]
        overwrite: bool,
        /// Passphrase (required for *.db)
        #[arg(long, default_value = "")]
        passphrase: String,
    },

    /// POST /open
    Open {
        /// Wallet name (bitcoin-cli style) OR wallet file path.
        ///
        /// If a name is given (no "/" in it), it maps to <datadir>/<name>.db where <datadir>=~/.duta/<network>.
        wallet: String,
    },

    /// POST /migrate (legacy JSON -> encrypted SQLite)
    Migrate {
        /// Legacy JSON wallet path
        json_wallet_path: String,
        /// Destination SQLite wallet path (*.db)
        db_wallet_path: String,
        /// Passphrase for new SQLite wallet
        passphrase: String,
    },

    /// GET /address (alias: /getaddress)
    Address,

    /// GET /balance (alias: /getbalance)
    Getbalance,

    /// POST /getnewaddress
    Getnewaddress,

    /// POST /sync
    Sync,

    /// POST /lock
    Lock,

    /// POST /unlock
    Unlock {
        /// Wallet passphrase
        passphrase: String,
    },

    /// POST /export_seed
    Exportseed {
        /// Wallet passphrase for encrypted SQLite wallets
        #[arg(long, default_value = "")]
        passphrase: String,
    },

    /// POST /export_mnemonic
    Exportmnemonic {
        /// Wallet passphrase for encrypted SQLite wallets
        #[arg(long, default_value = "")]
        passphrase: String,
    },

    /// POST /import_mnemonic
    ///
    /// For encrypted SQLite wallet (.db), passphrase is REQUIRED.
    Importmnemonic {
        /// Wallet name (bitcoin-cli style) OR wallet file path.
        wallet: String,
        /// Mnemonic (24 words)
        mnemonic: String,
        /// Overwrite existing wallet file
        #[arg(long)]
        overwrite: bool,
        /// Passphrase (required for *.db)
        #[arg(long, default_value = "")]
        passphrase: String,
    },

    /// GET /listaddresses
    Listaddresses,

    /// POST /send {"to":..., "amount":..., "fee":optional}
    Send {
        /// Destination address
        to: String,
        /// Amount in display-unit DUTA (stored on-chain as integer dut; example: 1.25)
        amount: String,
        /// Fee in display-unit DUTA (optional; example: 0.0001)
        #[arg(long)]
        fee: Option<String>,
    },

    /// POST /sendmany {"outputs":[...], "fee":optional}
    Sendmany {
        /// Repeatable destination=amount pair, example: addr1=0.1
        outputs: Vec<String>,
        /// Fee for the whole transaction in display-unit DUTA
        #[arg(long)]
        fee: Option<String>,
    },

    /// POST /sendmany_plan {"outputs":[...], "fee":optional}
    Sendmanyplan {
        /// Repeatable destination=amount pair, example: addr1=0.1
        outputs: Vec<String>,
        /// Optional fee for the whole transaction in display-unit DUTA
        #[arg(long)]
        fee: Option<String>,
    },

    /// POST /send_plan {"amount":..., "fee":optional, "outputs":optional}
    Sendplan {
        /// Amount per payout in display-unit DUTA
        amount: String,
        /// Fee per payout in display-unit DUTA
        #[arg(long)]
        fee: Option<String>,
        /// Optional number of payouts to evaluate
        #[arg(long)]
        outputs: Option<usize>,
    },

    /// POST /rpc {"method":"listunspent"}
    Listunspent,

    /// POST /rpc {"method":"getwalletinfo"}
    Getwalletinfo,

    /// POST /rpc {"method":"walletdoctor"}
    Doctor,

    /// POST /rpc {"method":"listtransactions","params":[count, skip]}
    #[command(visible_alias = "listtransactions")]
    History {
        /// Max number of entries (default 10)
        #[arg(long, default_value_t = 10)]
        count: i64,
        /// Skip first N entries (default 0)
        #[arg(long, default_value_t = 0)]
        skip: i64,
    },

    /// POST /rpc {"method":"gettransaction","params":[txid]}
    #[command(visible_alias = "gettransaction")]
    Tx { txid: String },
}

fn main() {
    let args = Args::parse();
    let (host, port) = resolve_rpc(&args);
    let cmd = args.cmd.clone();

    if matches!(cmd, Cmd::Health) {
        match http_get_response(&host, port, "/health") {
            Ok((status, body)) => {
                println!("{}", format_response(&cmd, &body));
                let ok = serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| json_bool(&v, &["ok"]))
                    .unwrap_or((200..300).contains(&status));
                if !ok {
                    std::process::exit(1);
                }
                return;
            }
            Err(e) => {
                eprintln!("{}", format_error(&e));
                std::process::exit(1);
            }
        }
    }

    let res = match args.cmd {
        Cmd::Health => unreachable!("health handled above"),
        Cmd::Info => http_get_json(&host, port, "/info"),

        Cmd::Createwallet {
            ref wallet,
            overwrite,
            ref passphrase,
        } => {
            let wallet_path = resolve_wallet_path(&args, wallet);
            let mut body = json!({"wallet_path": wallet_path, "overwrite": overwrite});
            if !passphrase.trim().is_empty() {
                body["passphrase"] = json!(passphrase);
            }
            http_post_json(&host, port, "/createwallet", body)
        }

        Cmd::Open { ref wallet } => {
            let wallet_path = resolve_wallet_path(&args, wallet);
            http_post_json(&host, port, "/open", json!({"wallet_path": wallet_path}))
        }

        Cmd::Migrate {
            json_wallet_path,
            db_wallet_path,
            passphrase,
        } => http_post_json(
            &host,
            port,
            "/migrate",
            json!({"json_wallet_path": json_wallet_path, "db_wallet_path": db_wallet_path, "passphrase": passphrase}),
        ),

        Cmd::Address => http_get_json(&host, port, "/address"),
        Cmd::Getbalance => http_get_json(&host, port, "/balance"),
        Cmd::Getnewaddress => http_post_json(&host, port, "/getnewaddress", json!({})),
        Cmd::Sync => http_post_json(&host, port, "/sync", json!({})),
        Cmd::Lock => http_post_json(&host, port, "/lock", json!({})),
        Cmd::Unlock { passphrase } => {
            http_post_json(&host, port, "/unlock", json!({"passphrase": passphrase}))
        }
        Cmd::Exportseed { ref passphrase } => {
            let mut body = json!({});
            if !passphrase.trim().is_empty() {
                body["passphrase"] = json!(passphrase);
            }
            http_post_json(&host, port, "/export_seed", body)
        }
        Cmd::Exportmnemonic { ref passphrase } => {
            let mut body = json!({});
            if !passphrase.trim().is_empty() {
                body["passphrase"] = json!(passphrase);
            }
            http_post_json(&host, port, "/export_mnemonic", body)
        }

        Cmd::Importmnemonic {
            ref wallet,
            ref mnemonic,
            overwrite,
            ref passphrase,
        } => {
            let wallet_path = resolve_wallet_path(&args, wallet);
            let mut body =
                json!({"wallet_path": wallet_path, "mnemonic": mnemonic, "overwrite": overwrite});
            if !passphrase.trim().is_empty() {
                body["passphrase"] = json!(passphrase);
            }
            http_post_json(&host, port, "/import_mnemonic", body)
        }

        Cmd::Listaddresses => http_get_json(&host, port, "/listaddresses"),

        Cmd::Send { to, amount, fee } => {
            let mut v = json!({"to": to, "amount": amount});
            if let Some(f) = fee {
                v["fee"] = json!(f);
            }
            http_post_json(&host, port, "/send", v)
        }

        Cmd::Sendmany { outputs, fee } => {
            let mut parsed = Vec::new();
            let mut parse_error = None;
            for item in outputs {
                let Some((to, amount)) = item.split_once('=') else {
                    parse_error = Some("invalid_output_pair_use_address_equals_amount".to_string());
                    break;
                };
                parsed.push(json!({"to": to, "amount": amount}));
            }
            if let Some(err) = parse_error {
                Err(err)
            } else {
                let mut v = json!({"outputs": parsed});
                if let Some(f) = fee {
                    v["fee"] = json!(f);
                }
                http_post_json(&host, port, "/sendmany", v)
            }
        }

        Cmd::Sendmanyplan { outputs, fee } => {
            let mut parsed = Vec::new();
            let mut parse_error = None;
            for item in outputs {
                let Some((to, amount)) = item.split_once('=') else {
                    parse_error = Some("invalid_output_pair_use_address_equals_amount".to_string());
                    break;
                };
                parsed.push(json!({"to": to, "amount": amount}));
            }
            if let Some(err) = parse_error {
                Err(err)
            } else {
                let mut v = json!({"outputs": parsed});
                if let Some(f) = fee {
                    v["fee"] = json!(f);
                }
                http_post_json(&host, port, "/sendmany_plan", v)
            }
        }

        Cmd::Sendplan {
            amount,
            fee,
            outputs,
        } => {
            let mut v = json!({"amount": amount});
            if let Some(f) = fee {
                v["fee"] = json!(f);
            }
            if let Some(count) = outputs {
                v["outputs"] = json!(count);
            }
            http_post_json(&host, port, "/send_plan", v)
        }

        Cmd::Listunspent => http_post_json(
            &host,
            port,
            "/rpc",
            json!({"jsonrpc":"2.0","id":1,"method":"listunspent","params":{}}),
        ),
        Cmd::Getwalletinfo => http_post_json(
            &host,
            port,
            "/rpc",
            json!({"jsonrpc":"2.0","id":1,"method":"getwalletinfo","params":{}}),
        ),
        Cmd::Doctor => http_post_json(
            &host,
            port,
            "/rpc",
            json!({"jsonrpc":"2.0","id":1,"method":"walletdoctor","params":{}}),
        ),

        Cmd::History { count, skip } => {
            let backend_count = count.saturating_mul(10).clamp(50, 500);
            http_post_json(
                &host,
                port,
                "/rpc",
                json!({"jsonrpc":"2.0","id":1,"method":"listtransactions","params":[backend_count, skip]}),
            )
        }

        Cmd::Tx { txid } => http_post_json(
            &host,
            port,
            "/rpc",
            json!({"jsonrpc":"2.0","id":1,"method":"gettransaction","params":[txid]}),
        ),
    };

    match res {
        Ok(body) => {
            println!("{}", format_response(&cmd, &body));
        }
        Err(e) => {
            eprintln!("{}", format_error(&e));
            std::process::exit(1);
        }
    }
}

fn tag(label: &str, bg: &str, fg: &str) -> String {
    format!("{ANSI_BOLD}{bg}{fg} {label} {ANSI_RESET}")
}

fn line(label: &str, bg: &str, fg: &str, message: impl AsRef<str>) -> String {
    format!("{} {}", tag(label, bg, fg), message.as_ref())
}

fn ok_line(message: impl AsRef<str>) -> String {
    line("OK", ANSI_BG_GREEN, ANSI_BLACK, message)
}

fn info_line(message: impl AsRef<str>) -> String {
    line("INFO", ANSI_BG_BLUE, ANSI_WHITE, message)
}

fn wait_line(message: impl AsRef<str>) -> String {
    line("WAIT", ANSI_BG_YELLOW, ANSI_BLACK, message)
}

fn err_line(message: impl AsRef<str>) -> String {
    line("ERROR", ANSI_BG_RED, ANSI_WHITE, message)
}

fn history_label(category: &str, amount: i64, height: i64) -> String {
    if category == "move" {
        if height > 0 {
            "internal transfer".to_string()
        } else {
            "pending internal transfer".to_string()
        }
    } else if category == "send" && height <= 0 {
        "pending send".to_string()
    } else if category == "receive" && height <= 0 {
        "pending receive".to_string()
    } else {
        let _ = amount;
        category.to_string()
    }
}

fn history_priority(category: &str, height: i64) -> i32 {
    if height <= 0 {
        return 0;
    }
    match category {
        "send" | "move" => 1,
        "receive" => 2,
        _ => 3,
    }
}

fn history_amount_and_fee(item: &serde_json::Value) -> (String, Option<String>) {
    let category = item
        .get("category")
        .and_then(|x| x.as_str())
        .unwrap_or("tx");
    let amount = amount_from_json(item, "amount", "amount_dut").unwrap_or(0);
    let fee = amount_from_json(item, "fee", "fee_dut").unwrap_or(0);

    if category == "send" {
        let sent = if amount >= 0 {
            amount
        } else if fee != 0 {
            amount.abs().saturating_sub(fee.abs())
        } else {
            amount.abs()
        };
        let fee_text = if fee != 0 {
            Some(format!(
                "fee {} {}",
                format_dut_i64(fee.abs()),
                DISPLAY_UNIT
            ))
        } else {
            None
        };
        (
            format!("{} {}", format_dut_i64(sent), DISPLAY_UNIT),
            fee_text,
        )
    } else if category == "move" {
        let fee_text = if fee != 0 {
            Some(format!(
                "fee {} {}",
                format_dut_i64(fee.abs()),
                DISPLAY_UNIT
            ))
        } else {
            None
        };
        (
            format!("{} {}", format_dut_i64(amount), DISPLAY_UNIT),
            fee_text,
        )
    } else {
        (format!("{} {}", format_dut_i64(amount), DISPLAY_UNIT), None)
    }
}

fn amount_from_json(v: &serde_json::Value, display_key: &str, raw_key: &str) -> Option<i64> {
    if let Some(raw) = v.get(raw_key).and_then(|x| x.as_i64()) {
        return Some(raw);
    }
    match v.get(display_key) {
        Some(serde_json::Value::String(s)) => parse_duta_to_dut_i64(s).ok(),
        Some(serde_json::Value::Number(n)) => parse_duta_to_dut_i64(&n.to_string()).ok(),
        _ => None,
    }
}

fn amount_text(v: &serde_json::Value, display_key: &str, raw_key: &str) -> String {
    amount_from_json(v, display_key, raw_key)
        .map(|dut| format!("{} {}", format_dut_i64(dut), DISPLAY_UNIT))
        .unwrap_or_else(|| format!("0 {}", DISPLAY_UNIT))
}

fn json_field<'a>(v: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut cur = v;
    for key in path {
        cur = cur.get(*key)?;
    }
    Some(cur)
}

fn json_i64(v: &serde_json::Value, path: &[&str]) -> Option<i64> {
    json_field(v, path)?.as_i64()
}

fn json_bool(v: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_field(v, path)?.as_bool()
}

fn json_str<'a>(v: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_field(v, path)?.as_str()
}

fn format_wallet_health(v: &serde_json::Value, body: &str) -> String {
    let wallet_open = json_bool(v, &["wallet_open"]).unwrap_or(false);
    let height = json_i64(v, &["height"]).unwrap_or(0);
    let wallet_state =
        json_str(v, &["wallet_state"]).unwrap_or(if wallet_open { "open" } else { "closed" });
    let pending = json_i64(v, &["pending_txs"]).unwrap_or(0);
    let last_sync_height = json_i64(v, &["last_sync_height"]).unwrap_or(0);
    if json_bool(v, &["ok"]) == Some(true) {
        if wallet_open {
            format!(
                "{}\n{}\n{}\n{}\n{}",
                ok_line("wallet service is online"),
                info_line(format!("wallet: {wallet_state}")),
                info_line(format!("height: {height}")),
                info_line(format!("last sync height: {last_sync_height}")),
                info_line(format!("pending txs: {pending}"))
            )
        } else {
            format!(
                "{}\n{}",
                wait_line("wallet service is online"),
                wait_line("wallet: closed")
            )
        }
    } else if wallet_open {
        let error = json_str(v, &["error"]).unwrap_or("wallet_unhealthy");
        let detail = json_str(v, &["detail"]).unwrap_or(body);
        let backend = json_str(v, &["backend_status"]).unwrap_or("unknown");
        format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            err_line("wallet service degraded"),
            wait_line(format!("wallet: {wallet_state}")),
            wait_line(format!("height: {height}")),
            wait_line(format!("last sync height: {last_sync_height}")),
            wait_line(format!("pending txs: {pending}")),
            wait_line(format!("backend: {backend}")),
            err_line(format!("error: {error}")),
            info_line(format!("detail: {detail}"))
        )
    } else {
        body.to_string()
    }
}

fn format_response(cmd: &Cmd, body: &str) -> String {
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return body.to_string(),
    };

    match cmd {
        Cmd::Health => format_wallet_health(&v, body),
        Cmd::Info => {
            let net = json_str(&v, &["net"]).unwrap_or("unknown");
            let rpc = json_str(&v, &["wallet_rpc"]).unwrap_or("-");
            let height = json_i64(&v, &["height"]).unwrap_or(0);
            format!(
                "{}\n{}\n{}\n{}\n{}",
                info_line(format!("network: {net}")),
                info_line(format!("wallet rpc: {rpc}")),
                info_line(format!(
                    "balance: {}",
                    amount_text(&v, "balance", "balance_dut")
                )),
                info_line(format!(
                    "spendable: {}",
                    amount_text(&v, "spendable", "spendable_dut")
                )),
                info_line(format!("height: {height}"))
            )
        }
        Cmd::Open { .. } => {
            if json_bool(&v, &["ok"]) == Some(true) {
                let wallet = json_str(&v, &["wallet"]).unwrap_or("wallet");
                let unlocked = json_bool(&v, &["unlocked"]).unwrap_or(false);
                ok_line(format!(
                    "opened {wallet} ({})",
                    if unlocked {
                        "already unlocked"
                    } else {
                        "locked"
                    }
                ))
            } else {
                body.to_string()
            }
        }
        Cmd::Unlock { .. } => {
            if json_bool(&v, &["ok"]) == Some(true) {
                ok_line("wallet unlocked")
            } else {
                body.to_string()
            }
        }
        Cmd::Lock => {
            if json_bool(&v, &["ok"]) == Some(true) {
                ok_line("wallet locked")
            } else {
                body.to_string()
            }
        }
        Cmd::Getnewaddress => {
            if let Some(addr) = json_str(&v, &["address"]) {
                ok_line(format!("new address: {addr}"))
            } else {
                body.to_string()
            }
        }
        Cmd::Getbalance => {
            let height = json_i64(&v, &["height"]).unwrap_or(0);
            let utxos = json_i64(&v, &["utxos"]).unwrap_or(0);
            let pending = json_i64(&v, &["pending_txs"]).unwrap_or(0);
            let balance = amount_text(&v, "balance", "balance_dut");
            let spendable = amount_text(&v, "spendable", "spendable_dut");
            format!(
                "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
                info_line(format!("balance: {balance}")),
                info_line(format!("spendable: {spendable}")),
                info_line(format!(
                    "reserved: {}",
                    amount_text(&v, "reserved", "reserved_dut")
                )),
                info_line(format!(
                    "pending send: {}",
                    amount_text(&v, "pending_send", "pending_send_dut")
                )),
                info_line(format!(
                    "pending change: {}",
                    amount_text(&v, "pending_change", "pending_change_dut")
                )),
                info_line(format!("height: {height}")),
                info_line(format!("utxos: {utxos}")),
                wait_line(format!("pending txs: {pending}"))
            )
        }
        Cmd::Getwalletinfo => {
            let height = json_i64(&v, &["result", "height"]).unwrap_or(0);
            let wallet = json_str(&v, &["result", "walletname"]).unwrap_or("wallet");
            let unlocked = json_bool(&v, &["result", "unlocked"]).unwrap_or(false);
            let result = json_field(&v, &["result"])
                .cloned()
                .unwrap_or_else(|| json!({}));
            format!(
                "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
                info_line(format!("wallet: {wallet}")),
                info_line(format!(
                    "balance: {}",
                    amount_text(&result, "balance", "balance_dut")
                )),
                info_line(format!(
                    "spendable: {}",
                    amount_text(&result, "spendable_balance", "spendable_balance_dut")
                )),
                info_line(format!(
                    "immature: {}",
                    amount_text(&result, "immature_balance", "immature_balance_dut")
                )),
                info_line(format!(
                    "reserved: {}",
                    amount_text(&result, "reserved_balance", "reserved_balance_dut")
                )),
                info_line(format!(
                    "pending send: {}",
                    amount_text(&result, "pending_send", "pending_send_dut")
                )),
                info_line(format!(
                    "pending change: {}",
                    amount_text(&result, "pending_change", "pending_change_dut")
                )),
                info_line(format!("height: {height}")),
                info_line(format!(
                    "status: {}",
                    if unlocked { "unlocked" } else { "locked" }
                )),
                wait_line(format!(
                    "pending txs: {}",
                    json_i64(&result, &["pending_txs"]).unwrap_or(0)
                ))
            )
        }
        Cmd::Doctor => {
            let result = json_field(&v, &["result"])
                .cloned()
                .unwrap_or_else(|| json!({}));
            let mut lines = vec![
                info_line(format!(
                    "wallet: {}",
                    json_str(&result, &["walletname"]).unwrap_or("wallet")
                )),
                info_line(format!(
                    "db health: {}",
                    json_str(&result, &["db_health"]).unwrap_or("unknown")
                )),
                info_line(format!(
                    "status: {}",
                    if json_bool(&result, &["wallet_unlocked"]).unwrap_or(false) {
                        "unlocked"
                    } else {
                        "locked"
                    }
                )),
                info_line(format!(
                    "balance: {}",
                    amount_text(&result, "balance", "balance_dut")
                )),
                info_line(format!(
                    "spendable: {}",
                    amount_text(&result, "spendable_balance", "spendable_balance_dut")
                )),
                info_line(format!(
                    "reserved balance: {}",
                    amount_text(&result, "reserved_balance", "reserved_balance_dut")
                )),
                info_line(format!(
                    "pending send/change: {} / {}",
                    amount_text(&result, "pending_send", "pending_send_dut"),
                    amount_text(&result, "pending_change", "pending_change_dut")
                )),
                info_line(format!(
                    "pending txs: {}",
                    json_i64(&result, &["pending_txs"]).unwrap_or(0)
                )),
                info_line(format!(
                    "reserved inputs: {}",
                    json_i64(&result, &["reserved_inputs"]).unwrap_or(0)
                )),
                info_line(format!(
                    "last sync height: {}",
                    json_i64(&result, &["last_sync_height"]).unwrap_or(0)
                )),
                info_line(format!(
                    "next index: {}",
                    json_i64(&result, &["next_index"]).unwrap_or(0)
                )),
                info_line(format!(
                    "backend: {}",
                    json_str(&result, &["backend_status"]).unwrap_or("unknown")
                )),
            ];
            if json_bool(&result, &["submitted_tx_recovery_present"]) == Some(true) {
                lines.push(wait_line("wallet state: partial_send_committed"));
                lines.push(wait_line(format!(
                    "recovery txid: {}",
                    json_str(&result, &["submitted_tx_recovery_txid"]).unwrap_or("-")
                )));
            }
            lines.join("\n")
        }
        Cmd::Sync => {
            if json_bool(&v, &["ok"]) == Some(true) {
                let height = json_i64(&v, &["height"]).unwrap_or(0);
                let pending = json_i64(&v, &["pending_txs"]).unwrap_or(0);
                format!(
                    "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
                    ok_line("wallet sync complete"),
                    info_line(format!(
                        "balance: {}",
                        amount_text(&v, "balance", "balance_dut")
                    )),
                    info_line(format!(
                        "spendable: {}",
                        amount_text(&v, "spendable", "spendable_dut")
                    )),
                    info_line(format!(
                        "reserved: {}",
                        amount_text(&v, "reserved", "reserved_dut")
                    )),
                    info_line(format!(
                        "pending send: {}",
                        amount_text(&v, "pending_send", "pending_send_dut")
                    )),
                    info_line(format!(
                        "pending change: {}",
                        amount_text(&v, "pending_change", "pending_change_dut")
                    )),
                    info_line(format!("height: {height}")),
                    wait_line(format!("pending sends: {pending}"))
                )
            } else {
                body.to_string()
            }
        }
        Cmd::Send { .. } => {
            if json_str(&v, &["error"]) == Some("wallet_state_partially_committed") {
                let txid = json_str(&v, &["txid"]).unwrap_or("-");
                vec![
                    wait_line("send submitted but wallet only saved recovery state"),
                    info_line(format!("txid: {txid}")),
                    info_line(format!(
                        "amount: {}",
                        amount_text(&v, "amount", "amount_dut")
                    )),
                    info_line(format!("fee: {}", amount_text(&v, "fee", "fee_dut"))),
                    info_line(format!(
                        "change: {}",
                        amount_text(&v, "change", "change_dut")
                    )),
                    wait_line("reserved inputs committed: yes"),
                    wait_line("submitted recovery committed: yes"),
                    wait_line("full wallet state committed: no"),
                    wait_line("next action: inspect walletdoctor before retrying send"),
                ]
                .join("\n")
            } else if json_bool(&v, &["ok"]) == Some(true) {
                let txid = json_str(&v, &["txid"]).unwrap_or("-");
                let persisted = json_bool(&v, &["wallet_state_persisted"]).unwrap_or(false);
                let fee_mode = if json_bool(&v, &["fee_auto"]).unwrap_or(false) {
                    "auto-min-relay"
                } else {
                    "manual"
                };
                vec![
                    ok_line("send accepted"),
                    info_line(format!("txid: {txid}")),
                    info_line(format!(
                        "amount: {}",
                        amount_text(&v, "amount", "amount_dut")
                    )),
                    info_line(format!("fee: {}", amount_text(&v, "fee", "fee_dut"))),
                    info_line(format!("fee mode: {fee_mode}")),
                    info_line(format!(
                        "min relay fee: {}",
                        amount_text(&v, "min_relay_fee", "min_relay_fee_dut")
                    )),
                    info_line(format!(
                        "change: {}",
                        amount_text(&v, "change", "change_dut")
                    )),
                    info_line(format!(
                        "size: {} bytes",
                        json_i64(&v, &["size"]).unwrap_or(0)
                    )),
                    info_line(format!(
                        "saved: {}",
                        if persisted { "yes" } else { "not yet" }
                    )),
                ]
                .join("\n")
            } else {
                body.to_string()
            }
        }
        Cmd::Sendmany { .. } => {
            if json_str(&v, &["error"]) == Some("wallet_state_partially_committed") {
                let txid = json_str(&v, &["txid"]).unwrap_or("-");
                vec![
                    wait_line("batch send submitted but wallet only saved recovery state"),
                    info_line(format!("txid: {txid}")),
                    info_line(format!(
                        "amount: {}",
                        amount_text(&v, "amount", "amount_dut")
                    )),
                    info_line(format!("fee: {}", amount_text(&v, "fee", "fee_dut"))),
                    info_line(format!(
                        "change: {}",
                        amount_text(&v, "change", "change_dut")
                    )),
                    wait_line("reserved inputs committed: yes"),
                    wait_line("submitted recovery committed: yes"),
                    wait_line("full wallet state committed: no"),
                    wait_line("next action: inspect walletdoctor before retrying send"),
                ]
                .join("\n")
            } else if json_bool(&v, &["ok"]) == Some(true) {
                let txid = json_str(&v, &["txid"]).unwrap_or("-");
                let fee_mode = if json_bool(&v, &["fee_auto"]).unwrap_or(false) {
                    "auto-min-relay"
                } else {
                    "manual"
                };
                vec![
                    ok_line("batch send accepted"),
                    info_line(format!("txid: {txid}")),
                    info_line(format!(
                        "outputs: {}",
                        json_i64(&v, &["outputs"]).unwrap_or(0)
                    )),
                    info_line(format!(
                        "inputs: {}",
                        json_i64(&v, &["inputs"]).unwrap_or(0)
                    )),
                    info_line(format!(
                        "amount: {}",
                        amount_text(&v, "amount", "amount_dut")
                    )),
                    info_line(format!("fee: {}", amount_text(&v, "fee", "fee_dut"))),
                    info_line(format!("fee mode: {fee_mode}")),
                    info_line(format!(
                        "min relay fee: {}",
                        amount_text(&v, "min_relay_fee", "min_relay_fee_dut")
                    )),
                    info_line(format!(
                        "change: {}",
                        amount_text(&v, "change", "change_dut")
                    )),
                    info_line(format!(
                        "size: {} bytes",
                        json_i64(&v, &["size"]).unwrap_or(0)
                    )),
                ]
                .join("\n")
            } else {
                body.to_string()
            }
        }
        Cmd::Sendmanyplan { .. } => {
            if json_bool(&v, &["ok"]) == Some(true) {
                let fits = json_bool(&v, &["can_send"]).unwrap_or(false);
                let mut lines = vec![
                    info_line(format!(
                        "batch outputs: {}",
                        json_i64(&v, &["outputs"]).unwrap_or(0)
                    )),
                    info_line(format!(
                        "estimated inputs: {}",
                        json_i64(&v, &["inputs"]).unwrap_or(0)
                    )),
                    info_line(format!(
                        "amount total: {}",
                        amount_text(&v, "amount", "amount_dut")
                    )),
                    info_line(format!(
                        "estimated fee: {}",
                        amount_text(&v, "fee", "fee_dut")
                    )),
                    info_line(format!(
                        "min relay fee: {}",
                        amount_text(&v, "min_relay_fee", "min_relay_fee_dut")
                    )),
                    info_line(format!(
                        "estimated size: {} bytes",
                        json_i64(&v, &["size"]).unwrap_or(0)
                    )),
                    info_line(format!(
                        "spendable now: {} across {} utxo(s)",
                        amount_text(&v, "spendable_balance", "spendable_balance_dut"),
                        json_i64(&v, &["spendable_utxos"]).unwrap_or(0)
                    )),
                    info_line(format!(
                        "batch viability: {}",
                        if fits { "ready_to_send" } else { "would_fail" }
                    )),
                ];
                if !fits {
                    let failure = json_field(&v, &["failure"])
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    let detail = json_str(&failure, &["detail"])
                        .or_else(|| json_str(&failure, &["error"]))
                        .unwrap_or("batch_not_viable");
                    lines.push(wait_line(format!("failure detail: {detail}")));
                    if let Some(min_fee) = json_i64(&failure, &["min_fee"]) {
                        lines.push(wait_line(format!(
                            "required relay fee: {} {}",
                            format_dut_i64(min_fee),
                            DISPLAY_UNIT
                        )));
                    }
                }
                lines.join("\n")
            } else {
                body.to_string()
            }
        }
        Cmd::Sendplan { .. } => {
            if json_bool(&v, &["ok"]) == Some(true) {
                let requested = json_i64(&v, &["requested_outputs"]);
                let fits = json_bool(&v, &["requested_outputs_fit"]).unwrap_or(true);
                let max_outputs = json_i64(&v, &["max_outputs"]).unwrap_or(0);
                let mut lines = vec![
                    info_line(format!(
                        "amount per payout: {}",
                        amount_text(&v, "amount", "amount_dut")
                    )),
                    info_line(format!(
                        "fee per payout: {}",
                        amount_text(&v, "fee", "fee_dut")
                    )),
                    info_line(format!(
                        "need per payout: {}",
                        amount_text(&v, "need", "need_dut")
                    )),
                    info_line(format!("max confirmed payouts now: {max_outputs}")),
                    info_line(format!(
                        "spendable now: {} across {} utxo(s)",
                        amount_text(&v, "spendable_balance", "spendable_balance_dut"),
                        json_i64(&v, &["spendable_utxos"]).unwrap_or(0)
                    )),
                ];
                if let Some(req) = requested {
                    lines.push(info_line(format!(
                        "requested payouts: {} ({})",
                        req,
                        if fits {
                            "fits"
                        } else {
                            "would exhaust confirmed utxos"
                        }
                    )));
                }
                if !fits {
                    let failure = json_field(&v, &["failure"])
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    lines.push(wait_line(format!(
                        "failure detail: {}",
                        json_str(&failure, &["detail"])
                            .unwrap_or("confirmed_spendable_utxos_exhausted_or_reserved")
                    )));
                    lines.push(wait_line(format!(
                        "pending send/change: {} / {}",
                        amount_text(&failure, "pending_send", "pending_send_dut"),
                        amount_text(&failure, "pending_change", "pending_change_dut")
                    )));
                }
                lines.join("\n")
            } else {
                body.to_string()
            }
        }
        Cmd::History { .. } => {
            if let Some(items) = json_field(&v, &["result"]).and_then(|x| x.as_array()) {
                if items.is_empty() {
                    return info_line("no wallet history yet");
                }
                let limit = match cmd {
                    Cmd::History { count, .. } => (*count).max(1) as usize,
                    _ => 10,
                };
                let mut ordered: Vec<&serde_json::Value> = items.iter().collect();
                ordered.sort_by(|a, b| {
                    let a_category = a.get("category").and_then(|x| x.as_str()).unwrap_or("tx");
                    let b_category = b.get("category").and_then(|x| x.as_str()).unwrap_or("tx");
                    let a_height = a.get("blockheight").and_then(|x| x.as_i64()).unwrap_or(0);
                    let b_height = b.get("blockheight").and_then(|x| x.as_i64()).unwrap_or(0);
                    let a_time = a
                        .get("time")
                        .or_else(|| a.get("timereceived"))
                        .and_then(|x| x.as_i64())
                        .unwrap_or(0);
                    let b_time = b
                        .get("time")
                        .or_else(|| b.get("timereceived"))
                        .and_then(|x| x.as_i64())
                        .unwrap_or(0);
                    history_priority(a_category, a_height)
                        .cmp(&history_priority(b_category, b_height))
                        .then_with(|| b_height.cmp(&a_height))
                        .then_with(|| b_time.cmp(&a_time))
                });
                let mut lines = Vec::new();
                lines.push(info_line(format!("history entries: {}", ordered.len())));
                for item in ordered.into_iter().take(limit) {
                    let category = item
                        .get("category")
                        .and_then(|x| x.as_str())
                        .unwrap_or("tx");
                    let txid = item.get("txid").and_then(|x| x.as_str()).unwrap_or("-");
                    let height = item
                        .get("blockheight")
                        .and_then(|x| x.as_i64())
                        .unwrap_or(0);
                    let amount = item.get("amount").and_then(|x| x.as_i64()).unwrap_or(0);
                    let label = history_label(category, amount, height);
                    let (amount_text, fee_text) = history_amount_and_fee(item);
                    let location = if height > 0 {
                        format!("at height {height}")
                    } else {
                        "waiting for confirmation".to_string()
                    };
                    let mut message = format!("{label}: {amount_text} {location} ({txid})");
                    if let Some(fee_text) = fee_text {
                        message.push_str(&format!(" [{fee_text}]"));
                    }
                    lines.push(info_line(message));
                }
                lines.join("\n")
            } else {
                body.to_string()
            }
        }
        Cmd::Address => {
            if let Some(addr) = json_str(&v, &["address"]) {
                info_line(format!("wallet address: {addr}"))
            } else {
                body.to_string()
            }
        }
        _ => body.to_string(),
    }
}

fn format_error(raw: &str) -> String {
    if let Some(msg) = human_error_message(raw) {
        return err_line(msg);
    }
    err_line(raw)
}

fn human_error_message(raw: &str) -> Option<String> {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("daemon_unreachable") {
        return Some("node is not reachable right now".to_string());
    }
    if lower.contains("wallet_state_refresh_failed") {
        return Some("wallet could not refresh its balance yet".to_string());
    }
    if lower.contains("connect_failed") {
        return Some("cannot reach wallet service".to_string());
    }
    if lower.contains("wallet_open_failed") {
        return Some("could not open wallet file".to_string());
    }
    if lower.contains("read_failed") || lower.contains("write_failed") {
        return Some("wallet service connection was interrupted".to_string());
    }
    if lower.contains("wallet_locked") {
        return Some("wallet is locked".to_string());
    }
    if lower.contains("missing_passphrase") {
        return Some("passphrase is required".to_string());
    }
    if lower.contains("passphrase_too_short") {
        return Some("passphrase is too short".to_string());
    }
    if lower.contains("insufficient_funds") {
        return Some("not enough spendable balance".to_string());
    }
    if lower.contains("too_many_inputs") {
        return Some("this payment needs too many small inputs".to_string());
    }
    if lower.contains("input_not_found") || lower.contains("double_spend") {
        return Some("that payment conflicts with a transaction already in progress".to_string());
    }
    if lower.contains("daemon_submit_failed") {
        return Some("the node did not accept the payment".to_string());
    }
    if lower.contains("wallet_state_partially_committed") {
        return Some(
            "payment was submitted but wallet only saved recovery state; inspect walletdoctor before retrying"
                .to_string(),
        );
    }
    None
}

fn resolve_rpc(args: &Args) -> (String, u16) {
    if !args.rpc.trim().is_empty() {
        return split_host_port(&args.rpc).unwrap_or_else(|| {
            eprintln!("Invalid --rpc, expected host:port");
            std::process::exit(2);
        });
    }

    let net = if args.stagenet {
        Network::Stagenet
    } else if args.testnet {
        Network::Testnet
    } else {
        Network::Mainnet
    };

    ("127.0.0.1".to_string(), net.default_wallet_rpc_port())
}

fn split_host_port(s: &str) -> Option<(String, u16)> {
    let (h, p) = s.rsplit_once(':')?;
    let port: u16 = p.parse().ok()?;
    Some((h.to_string(), port))
}

fn http_get_json(host: &str, port: u16, path: &str) -> Result<String, String> {
    http_request_json("GET", host, port, path, None)
}

fn http_get_response(host: &str, port: u16, path: &str) -> Result<(u16, String), String> {
    http_request_response("GET", host, port, path, None)
}

fn http_post_json(
    host: &str,
    port: u16,
    path: &str,
    body: serde_json::Value,
) -> Result<String, String> {
    http_request_json("POST", host, port, path, Some(body.to_string()))
}

fn resolve_wallet_path(args: &Args, wallet: &str) -> String {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());

    // Expand "~/" prefix early.
    let mut w = wallet.to_string();
    if w == "~" {
        w = home.clone();
    } else if let Some(rest) = w.strip_prefix("~/") {
        w = format!("{}/{}", home, rest);
    }

    // If caller provided an explicit path, keep it.
    let is_windows_abs =
        w.len() >= 3 && w.as_bytes()[1] == b':' && matches!(w.as_bytes()[2], b'\\' | b'/');
    let is_unc = w.starts_with("\\\\");
    if w.contains('/') || w.contains('\\') || is_windows_abs || is_unc {
        return w;
    }

    // Treat as wallet name. Map to $HOME/<default_datadir>/<name>.db
    let net = if args.stagenet {
        Network::Stagenet
    } else if args.testnet {
        Network::Testnet
    } else {
        Network::Mainnet
    };

    let mut name = w;
    if !name.ends_with(".db") {
        name.push_str(".db");
    }

    // default_data_dir_unix is relative (".duta/<network>")
    format!("{}/{}/{}", home, net.default_data_dir_unix(), name)
}

fn http_request_json(
    method: &str,
    host: &str,
    port: u16,
    path: &str,
    body: Option<String>,
) -> Result<String, String> {
    let (code, body) = http_request_response(method, host, port, path, body)?;
    if (200..300).contains(&code) {
        Ok(body)
    } else {
        Err(format!("HTTP {}: {}", code, body))
    }
}

fn http_request_response(
    method: &str,
    host: &str,
    port: u16,
    path: &str,
    body: Option<String>,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect((host, port))
        .map_err(|e| format!("connect_failed: {}:{}: {}", host, port, e))?;

    let body_s = body.unwrap_or_default();
    let content_len = body_s.as_bytes().len();

    let req = if method == "GET" {
        format!(
            "GET {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\nAccept: application/json\r\n\r\n",
            path, host, port
        )
    } else {
        format!(
            "POST {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            path, host, port, content_len, body_s
        )
    };

    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("write_failed: {}", e))?;

    let buf = read_http_response_bytes(&mut stream)?;

    let resp = String::from_utf8_lossy(&buf).to_string();
    parse_http_response_any_status(&resp)
}

fn read_http_response_bytes<R: Read>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e)
                if !buf.is_empty()
                    && matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::BrokenPipe
                    ) =>
            {
                break;
            }
            Err(e) => return Err(format!("read_failed: {}", e)),
        }
    }
    Ok(buf)
}

#[cfg(test)]
fn parse_http_response(resp: &str) -> Result<String, String> {
    let (code, body) = parse_http_response_any_status(resp)?;
    if (200..300).contains(&code) {
        Ok(body)
    } else {
        Err(format!("HTTP {}: {}", code, body))
    }
}

fn parse_http_response_any_status(resp: &str) -> Result<(u16, String), String> {
    let (head, body) = match resp.split_once("\r\n\r\n") {
        Some(x) => x,
        None => return Err("invalid_http_response".to_string()),
    };

    let status_line = head.lines().next().unwrap_or("");
    let code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|x| x.parse().ok())
        .unwrap_or(0);

    let chunked_header = head.lines().any(|line| {
        let mut parts = line.splitn(2, ':');
        let name = parts.next().unwrap_or("").trim();
        let value = parts.next().unwrap_or("").trim();
        name.eq_ignore_ascii_case("Transfer-Encoding") && value.eq_ignore_ascii_case("chunked")
    });
    let body = if chunked_header || looks_like_chunked_body(body) {
        decode_chunked_body(body)?
    } else {
        body.to_string()
    };
    Ok((code, body))
}

fn looks_like_chunked_body(body: &str) -> bool {
    let Some((size_line, rest)) = body.split_once("\r\n") else {
        return false;
    };
    let size_hex = size_line.split(';').next().unwrap_or("").trim();
    if size_hex.is_empty() || !size_hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    let Ok(size) = usize::from_str_radix(size_hex, 16) else {
        return false;
    };
    size > 0 && rest.len() >= size + 2
}

fn decode_chunked_body(body: &str) -> Result<String, String> {
    let mut rest = body;
    let mut decoded = String::new();

    loop {
        let (size_line, after_size) = match rest.split_once("\r\n") {
            Some(x) => x,
            None => return Err("invalid_chunked_response".to_string()),
        };
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size =
            usize::from_str_radix(size_hex, 16).map_err(|_| "invalid_chunk_size".to_string())?;
        if size == 0 {
            break;
        }
        if after_size.len() < size + 2 {
            return Err("truncated_chunked_response".to_string());
        }
        decoded.push_str(&after_size[..size]);
        rest = &after_size[size + 2..];
    }

    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::{
        amount_text, format_response, history_amount_and_fee, human_error_message,
        parse_http_response, parse_http_response_any_status, read_http_response_bytes,
        resolve_wallet_path, Args, Cmd,
    };
    use clap::Parser;
    use serde_json::json;

    fn args() -> Args {
        Args {
            testnet: true,
            stagenet: false,
            rpc: String::new(),
            cmd: Cmd::Health,
        }
    }

    #[test]
    fn resolve_wallet_path_keeps_windows_absolute_paths() {
        let args = args();
        let path = r"C:\Dutafinal\runtime-e2e\wallet-testnet\pooltest.db";
        assert_eq!(resolve_wallet_path(&args, path), path);
    }

    #[test]
    fn resolve_wallet_path_keeps_forward_slash_absolute_paths() {
        let args = args();
        let path = "C:/Dutafinal/runtime-e2e/wallet-testnet/pooltest.db";
        assert_eq!(resolve_wallet_path(&args, path), path);
    }

    #[test]
    fn history_amount_and_fee_shows_internal_move_fee() {
        let item = json!({
            "category": "move",
            "amount_dut": 0,
            "fee_dut": 10000
        });
        let (amount_text, fee_text) = history_amount_and_fee(&item);
        assert_eq!(amount_text, "0.00000000 DUTA");
        assert_eq!(fee_text.as_deref(), Some("fee 0.00010000 DUTA"));
    }

    #[test]
    fn amount_text_formats_raw_dut_as_display_unit() {
        let item = json!({
            "amount_dut": 1,
            "unit": "DUTA",
            "base_unit": "dut",
            "decimals": 8
        });
        assert_eq!(
            amount_text(&item, "amount", "amount_dut"),
            "0.00000001 DUTA"
        );
    }

    #[test]
    fn partial_http_response_still_surfaces_error_body_after_connection_reset() {
        struct ResetAfterBody {
            body: Vec<u8>,
            pos: usize,
        }
        impl std::io::Read for ResetAfterBody {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.pos >= self.body.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "connection reset by peer",
                    ));
                }
                let n = std::cmp::min(buf.len(), self.body.len() - self.pos);
                buf[..n].copy_from_slice(&self.body[self.pos..self.pos + n]);
                self.pos += n;
                Ok(n)
            }
        }

        let mut reader = ResetAfterBody {
            body: b"HTTP/1.1 400 Bad Request\r\nContent-Length: 75\r\n\r\n{\"ok\":false,\"error\":\"wallet_open_failed\",\"detail\":\"db_meta_read_failed\"}".to_vec(),
            pos: 0,
        };
        let resp = read_http_response_bytes(&mut reader).unwrap();
        let parsed = parse_http_response(&String::from_utf8_lossy(&resp)).unwrap_err();
        assert!(parsed.contains("HTTP 400"));
        assert!(parsed.contains("wallet_open_failed"));
        assert!(parsed.contains("db_meta_read_failed"));
        assert_eq!(
            human_error_message(&parsed),
            Some("could not open wallet file".to_string())
        );
    }

    #[test]
    fn parse_http_response_any_status_keeps_503_json_body() {
        let (code, body) = parse_http_response_any_status(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n\r\n{\"ok\":false,\"wallet_open\":true,\"wallet_state\":\"locked\",\"backend_status\":\"unreachable\",\"error\":\"daemon_unreachable\"}",
        )
        .expect("parsed");
        assert_eq!(code, 503);
        assert!(body.contains("\"wallet_state\":\"locked\""));
        assert!(body.contains("\"backend_status\":\"unreachable\""));
    }

    #[test]
    fn daemon_unreachable_beats_nested_connect_failed_detail() {
        let raw = r#"HTTP 400: {"error":{"code":-18,"message":"daemon_unreachable: connect_failed: Connection refused (os error 111)"}}"#;
        assert_eq!(
            human_error_message(raw),
            Some("node is not reachable right now".to_string())
        );
    }

    #[test]
    fn wallet_refresh_failure_beats_nested_connect_failed_detail() {
        let raw = r#"HTTP 400: {"error":{"code":-18,"message":"wallet_state_refresh_failed: connect_failed: Connection refused (os error 111)"}}"#;
        assert_eq!(
            human_error_message(raw),
            Some("wallet could not refresh its balance yet".to_string())
        );
    }

    #[test]
    fn amount_text_prefers_display_amount_for_getbalance_shape() {
        let item = json!({
            "amount": "598",
            "amount_dut": 59800000000i64,
            "unit": "DUTA",
            "base_unit": "dut",
            "decimals": 8
        });
        assert_eq!(
            amount_text(&item, "amount", "amount_dut"),
            "598.00000000 DUTA"
        );
    }

    #[test]
    fn getbalance_uses_balance_and_spendable_fields() {
        let out = format_response(
            &Cmd::Getbalance,
            &json!({
                "balance": "18720.76996788",
                "balance_dut": 1872076996788i64,
                "spendable": "15960.76996788",
                "spendable_dut": 1596076996788i64,
                "reserved": "0",
                "reserved_dut": 0,
                "pending_send": "0",
                "pending_send_dut": 0,
                "pending_change": "0",
                "pending_change_dut": 0,
                "height": 407,
                "utxos": 407,
                "pending_txs": 0,
                "unit": "DUTA",
                "base_unit": "dut",
                "decimals": 8
            })
            .to_string(),
        );
        assert!(out.contains("balance: 18720.76996788 DUTA"));
        assert!(out.contains("spendable: 15960.76996788 DUTA"));
        assert!(out.contains("reserved: 0.00000000 DUTA"));
        assert!(out.contains("pending send: 0.00000000 DUTA"));
        assert!(out.contains("pending change: 0.00000000 DUTA"));
    }

    #[test]
    fn health_formats_closed_wallet_as_waiting_state() {
        let out = format_response(&Cmd::Health, r#"{"ok":true,"wallet_open":false}"#);
        assert!(out.contains("wallet service is online"));
        assert!(out.contains("wallet: closed"));
    }

    #[test]
    fn health_formats_open_wallet_height() {
        let out = format_response(
            &Cmd::Health,
            r#"{"ok":true,"wallet_open":true,"wallet_state":"unlocked","height":42,"last_sync_height":41,"pending_txs":2}"#,
        );
        assert!(out.contains("wallet service is online"));
        assert!(out.contains("wallet: unlocked"));
        assert!(out.contains("height: 42"));
        assert!(out.contains("last sync height: 41"));
        assert!(out.contains("pending txs: 2"));
    }

    #[test]
    fn health_formats_backend_degradation_actionably() {
        let out = format_response(
            &Cmd::Health,
            r#"{"ok":false,"wallet_open":true,"wallet_state":"locked","height":42,"last_sync_height":41,"pending_txs":2,"backend_status":"timeout","error":"daemon_unreachable","detail":"connect_failed: connection refused"}"#,
        );
        assert!(out.contains("wallet service degraded"));
        assert!(out.contains("wallet: locked"));
        assert!(out.contains("backend: timeout"));
        assert!(out.contains("error: daemon_unreachable"));
        assert!(out.contains("detail: connect_failed: connection refused"));
    }

    #[test]
    fn doctor_formats_wallet_state_summary() {
        let out = format_response(
            &Cmd::Doctor,
            &json!({
                "result": {
                    "walletname": "ops",
                    "db_health": "open",
                    "wallet_unlocked": false,
                    "balance": "1.00000000",
                    "balance_dut": 100000000i64,
                    "spendable_balance": "0.50000000",
                    "spendable_balance_dut": 50000000i64,
                    "reserved_balance": "0.10000000",
                    "reserved_balance_dut": 10000000i64,
                    "pending_send": "0.20000000",
                    "pending_send_dut": 20000000i64,
                    "pending_change": "0.05000000",
                    "pending_change_dut": 5000000i64,
                    "pending_txs": 2,
                    "reserved_inputs": 3,
                    "last_sync_height": 77,
                    "next_index": 9,
                    "backend_status": "ok"
                }
            })
            .to_string(),
        );
        assert!(out.contains("wallet: ops"));
        assert!(out.contains("db health: open"));
        assert!(out.contains("status: locked"));
        assert!(out.contains("pending txs: 2"));
        assert!(out.contains("reserved inputs: 3"));
        assert!(out.contains("next index: 9"));
    }

    #[test]
    fn send_formats_partial_commit_state_honestly() {
        let out = format_response(
            &Cmd::Send {
                to: "dut1dest".to_string(),
                amount: "1.0".to_string(),
                fee: None,
            },
            &json!({
                "ok": false,
                "error": "wallet_state_partially_committed",
                "txid": "tx123",
                "amount": "1.00000000",
                "amount_dut": 100000000i64,
                "fee": "0.00010000",
                "fee_dut": 10000i64,
                "change": "0.99990000",
                "change_dut": 99990000i64
            })
            .to_string(),
        );
        assert!(out.contains("wallet only saved recovery state"));
        assert!(out.contains("txid: tx123"));
        assert!(out.contains("reserved inputs committed: yes"));
        assert!(out.contains("full wallet state committed: no"));
    }

    #[test]
    fn amount_text_keeps_fixed_eight_decimal_places() {
        let item = json!({
            "amount_dut": 50_000_000,
            "unit": "DUTA",
            "base_unit": "dut",
            "decimals": 8
        });
        assert_eq!(
            amount_text(&item, "amount", "amount_dut"),
            "0.50000000 DUTA"
        );
    }

    #[test]
    fn gettransaction_alias_maps_to_tx_command() {
        let parsed = Args::try_parse_from(["duta-wallet-cli", "gettransaction", "abcd1234"])
            .expect("gettransaction alias should parse");
        match parsed.cmd {
            Cmd::Tx { txid } => assert_eq!(txid, "abcd1234"),
            other => panic!("unexpected command parsed: {other:?}"),
        }
    }

    #[test]
    fn listtransactions_alias_maps_to_history_command() {
        let parsed = Args::try_parse_from([
            "duta-wallet-cli",
            "listtransactions",
            "--count",
            "25",
            "--skip",
            "5",
        ])
        .expect("listtransactions alias should parse");
        match parsed.cmd {
            Cmd::History { count, skip } => {
                assert_eq!(count, 25);
                assert_eq!(skip, 5);
            }
            other => panic!("unexpected command parsed: {other:?}"),
        }
    }

    #[test]
    fn exportmnemonic_accepts_passphrase_flag() {
        let parsed = Args::try_parse_from([
            "duta-wallet-cli",
            "exportmnemonic",
            "--passphrase",
            "phasec-pass-123",
        ])
        .expect("exportmnemonic should accept --passphrase");
        match parsed.cmd {
            Cmd::Exportmnemonic { passphrase } => {
                assert_eq!(passphrase, "phasec-pass-123");
            }
            other => panic!("unexpected command parsed: {other:?}"),
        }
    }

    #[test]
    fn exportseed_accepts_passphrase_flag() {
        let parsed = Args::try_parse_from([
            "duta-wallet-cli",
            "exportseed",
            "--passphrase",
            "phasec-pass-123",
        ])
        .expect("exportseed should accept --passphrase");
        match parsed.cmd {
            Cmd::Exportseed { passphrase } => {
                assert_eq!(passphrase, "phasec-pass-123");
            }
            other => panic!("unexpected command parsed: {other:?}"),
        }
    }
}
