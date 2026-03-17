//! duta-wallet-cli: lightweight wallet RPC client (bitcoin-cli style)
//!
//! Build example:
//!   RUSTFLAGS="-Dwarnings" cargo build --release -p dutawalletd --bin duta-wallet-cli

use clap::{Parser, Subcommand};
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
    Exportseed,

    /// POST /export_mnemonic
    Exportmnemonic,

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
        /// Amount (integer, current wallet API uses i64)
        amount: i64,
        /// Fee (optional; if omitted, wallet defaults to 1)
        #[arg(long)]
        fee: Option<i64>,
    },

    /// POST /rpc {"method":"listunspent"}
    Listunspent,

    /// POST /rpc {"method":"getwalletinfo"}
    Getwalletinfo,

    /// POST /rpc {"method":"listtransactions","params":[count, skip]}
    History {
        /// Max number of entries (default 10)
        #[arg(long, default_value_t = 10)]
        count: i64,
        /// Skip first N entries (default 0)
        #[arg(long, default_value_t = 0)]
        skip: i64,
    },

    /// POST /rpc {"method":"gettransaction","params":[txid]}
    Tx { txid: String },
}

fn main() {
    let args = Args::parse();
    let (host, port) = resolve_rpc(&args);
    let cmd = args.cmd.clone();

    let res = match args.cmd {
        Cmd::Health => http_get_json(&host, port, "/health"),
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
        Cmd::Exportseed => http_post_json(&host, port, "/export_seed", json!({})),
        Cmd::Exportmnemonic => http_post_json(&host, port, "/export_mnemonic", json!({})),

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
    let category = item.get("category").and_then(|x| x.as_str()).unwrap_or("tx");
    let amount = item.get("amount").and_then(|x| x.as_i64()).unwrap_or(0);
    let fee = item.get("fee").and_then(|x| x.as_i64()).unwrap_or(0);

    if category == "send" {
        let sent = if amount >= 0 {
            amount
        } else if fee != 0 {
            amount.abs().saturating_sub(fee.abs())
        } else {
            amount.abs()
        };
        let fee_text = if fee != 0 {
            Some(format!("fee {} DUTA", fee.abs()))
        } else {
            None
        };
        (format!("{sent} DUTA"), fee_text)
    } else {
        (format!("{amount} DUTA"), None)
    }
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

fn format_response(cmd: &Cmd, body: &str) -> String {
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return body.to_string(),
    };

    match cmd {
        Cmd::Health => {
            if json_bool(&v, &["ok"]) == Some(true) {
                ok_line("wallet service is online")
            } else {
                body.to_string()
            }
        }
        Cmd::Info => {
            let net = json_str(&v, &["net"]).unwrap_or("unknown");
            let rpc = json_str(&v, &["wallet_rpc"]).unwrap_or("-");
            let balance = json_i64(&v, &["balance"]).unwrap_or(0);
            let spendable = json_i64(&v, &["spendable"]).unwrap_or(0);
            let height = json_i64(&v, &["height"]).unwrap_or(0);
            format!(
                "{}\n{}\n{}\n{}\n{}",
                info_line(format!("network: {net}")),
                info_line(format!("wallet rpc: {rpc}")),
                info_line(format!("balance: {balance} DUTA")),
                info_line(format!("spendable: {spendable} DUTA")),
                info_line(format!("height: {height}"))
            )
        }
        Cmd::Open { .. } => {
            if json_bool(&v, &["ok"]) == Some(true) {
                let wallet = json_str(&v, &["wallet"]).unwrap_or("wallet");
                let unlocked = json_bool(&v, &["unlocked"]).unwrap_or(false);
                ok_line(format!(
                    "opened {wallet} ({})",
                    if unlocked { "already unlocked" } else { "locked" }
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
            let balance = json_i64(&v, &["balance"]).unwrap_or(0);
            let spendable = json_i64(&v, &["spendable"]).unwrap_or(0);
            let height = json_i64(&v, &["height"]).unwrap_or(0);
            let utxos = json_i64(&v, &["utxos"]).unwrap_or(0);
            format!(
                "{}\n{}\n{}\n{}",
                info_line(format!("balance: {balance} DUTA")),
                info_line(format!("spendable: {spendable} DUTA")),
                info_line(format!("height: {height}")),
                info_line(format!("utxos: {utxos}"))
            )
        }
        Cmd::Getwalletinfo => {
            let balance = json_i64(&v, &["result", "balance"]).unwrap_or(0);
            let spendable = json_i64(&v, &["result", "spendable_balance"]).unwrap_or(0);
            let immature = json_i64(&v, &["result", "immature_balance"]).unwrap_or(0);
            let height = json_i64(&v, &["result", "height"]).unwrap_or(0);
            let wallet = json_str(&v, &["result", "walletname"]).unwrap_or("wallet");
            let unlocked = json_bool(&v, &["result", "unlocked"]).unwrap_or(false);
            format!(
                "{}\n{}\n{}\n{}\n{}\n{}",
                info_line(format!("wallet: {wallet}")),
                info_line(format!("balance: {balance} DUTA")),
                info_line(format!("spendable: {spendable} DUTA")),
                info_line(format!("immature: {immature} DUTA")),
                info_line(format!("height: {height}")),
                info_line(format!(
                    "status: {}",
                    if unlocked { "unlocked" } else { "locked" }
                ))
            )
        }
        Cmd::Sync => {
            if json_bool(&v, &["ok"]) == Some(true) {
                let balance = json_i64(&v, &["balance"]).unwrap_or(0);
                let spendable = json_i64(&v, &["spendable"]).unwrap_or(0);
                let height = json_i64(&v, &["height"]).unwrap_or(0);
                let pending = json_i64(&v, &["pending_txs"]).unwrap_or(0);
                format!(
                    "{}\n{}\n{}\n{}\n{}",
                    ok_line("wallet sync complete"),
                    info_line(format!("balance: {balance} DUTA")),
                    info_line(format!("spendable: {spendable} DUTA")),
                    info_line(format!("height: {height}")),
                    wait_line(format!("pending sends: {pending}"))
                )
            } else {
                body.to_string()
            }
        }
        Cmd::Send { .. } => {
            if json_bool(&v, &["ok"]) == Some(true) {
                let txid = json_str(&v, &["txid"]).unwrap_or("-");
                let amount = json_i64(&v, &["amount"]).unwrap_or(0);
                let fee = json_i64(&v, &["fee"]).unwrap_or(0);
                let change = json_i64(&v, &["change"]).unwrap_or(0);
                let persisted = json_bool(&v, &["wallet_state_persisted"]).unwrap_or(false);
                format!(
                    "{}\n{}\n{}\n{}\n{}\n{}",
                    ok_line("send accepted"),
                    info_line(format!("txid: {txid}")),
                    info_line(format!("amount: {amount} DUTA")),
                    info_line(format!("fee: {fee} DUTA")),
                    info_line(format!("change: {change} DUTA")),
                    info_line(format!(
                        "saved: {}",
                        if persisted { "yes" } else { "not yet" }
                    ))
                )
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
                    let category = item.get("category").and_then(|x| x.as_str()).unwrap_or("tx");
                    let txid = item.get("txid").and_then(|x| x.as_str()).unwrap_or("-");
                    let height = item.get("blockheight").and_then(|x| x.as_i64()).unwrap_or(0);
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
    if lower.contains("connect_failed") {
        return Some("cannot reach wallet service".to_string());
    }
    if lower.contains("read_failed") || lower.contains("write_failed") {
        return Some("wallet service connection was interrupted".to_string());
    }
    if lower.contains("wallet_open_failed") {
        return Some("could not open wallet file".to_string());
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
    if lower.contains("daemon_unreachable") {
        return Some("node is not reachable right now".to_string());
    }
    if lower.contains("wallet_state_refresh_failed") {
        return Some("wallet could not refresh its balance yet".to_string());
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
    if w.contains('/') {
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

    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|e| format!("read_failed: {}", e))?;

    let resp = String::from_utf8_lossy(&buf).to_string();
    parse_http_response(&resp)
}

fn parse_http_response(resp: &str) -> Result<String, String> {
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

    if (200..300).contains(&code) {
        let chunked_header = head.lines().any(|line| {
            let mut parts = line.splitn(2, ':');
            let name = parts.next().unwrap_or("").trim();
            let value = parts.next().unwrap_or("").trim();
            name.eq_ignore_ascii_case("Transfer-Encoding")
                && value.eq_ignore_ascii_case("chunked")
        });
        if chunked_header || looks_like_chunked_body(body) {
            decode_chunked_body(body)
        } else {
            Ok(body.to_string())
        }
    } else {
        Err(format!("HTTP {}: {}", code, body))
    }
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
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| "invalid_chunk_size".to_string())?;
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
