//! duta-wallet-cli: lightweight wallet RPC client (bitcoin-cli style)
//!
//! Build example:
//!   RUSTFLAGS="-Dwarnings" cargo build --release -p dutawalletd --bin duta-wallet-cli

use clap::{Parser, Subcommand};
use duta_core::netparams::Network;
use serde_json::json;
use std::io::{Read, Write};
use std::net::TcpStream;

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

#[derive(Subcommand, Debug)]
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

        Cmd::History { count, skip } => http_post_json(
            &host,
            port,
            "/rpc",
            json!({"jsonrpc":"2.0","id":1,"method":"listtransactions","params":[count, skip]}),
        ),

        Cmd::Tx { txid } => http_post_json(
            &host,
            port,
            "/rpc",
            json!({"jsonrpc":"2.0","id":1,"method":"gettransaction","params":[txid]}),
        ),
    };

    match res {
        Ok(body) => {
            print!("{body}");
            if !body.ends_with('\n') {
                println!();
            }
        }
        Err(e) => {
            eprintln!("duta-wallet-cli error: {e}");
            std::process::exit(1);
        }
    }
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
        Ok(body.to_string())
    } else {
        Err(format!("HTTP {}: {}", code, body))
    }
}
