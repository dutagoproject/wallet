#[macro_use]
mod walletlog;

use clap::{Parser, Subcommand};
use duta_core::netparams::Network;
use hex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, OnceLock};
use zeroize::Zeroize;

mod walletdb;
use std::time::Duration;

mod router;

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD: &str = "\x1b[1m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_BLUE: &str = "\x1b[34m";
const ANSI_CYAN: &str = "\x1b[36m";
const ANSI_YELLOW: &str = "\x1b[33m";

fn console_tag(tag: &str, color: &str) -> String {
    format!("{}{}{: <8}{}", ANSI_BOLD, color, tag, ANSI_RESET)
}

fn console_kv(tag: &str, color: &str, key: &str, value: impl AsRef<str>) {
    println!(
        "{} {}{}{}",
        console_tag(tag, color),
        key,
        format!("{}:{}", ANSI_DIM, ANSI_RESET),
        value.as_ref()
    );
}

fn console_line(tag: &str, color: &str, value: impl AsRef<str>) {
    println!("{} {}", console_tag(tag, color), value.as_ref());
}

fn print_wallet_startup_banner(net: Network, data_dir: &str, rpc_addr: &str, daemon_rpc_port: u16) {
    println!();
    console_line(
        "WALLET",
        ANSI_CYAN,
        format!(
            "DUTA wallet {} starting on {}",
            env!("CARGO_PKG_VERSION"),
            net.as_str()
        ),
    );
    console_kv("PATH", ANSI_YELLOW, "data dir", data_dir);
    console_kv("RPC", ANSI_BLUE, "bind", rpc_addr);
    console_kv(
        "DAEMON",
        ANSI_CYAN,
        "rpc",
        format!("127.0.0.1:{}", daemon_rpc_port),
    );
    println!();
}

fn print_wallet_startup_guidance(rpc_addr: &str) {
    console_kv("RPC", ANSI_BLUE, "health", format!("http://{}/health", rpc_addr));
    console_kv("RPC", ANSI_BLUE, "info", format!("http://{}/info", rpc_addr));
    console_line("STATUS", ANSI_GREEN, "wallet rpc ready, waiting for open/unlock/sync commands");
    println!();
}

fn print_wallet_startup_warning(message: impl AsRef<str>) {
    console_line("WARN", ANSI_YELLOW, message);
}

fn build_http_request(method: &str, addr: &str, path: &str, body: &[u8]) -> String {
    let content_type = if method.eq_ignore_ascii_case("POST") {
        "Content-Type: application/json\r\n"
    } else {
        ""
    };
    format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: dutawalletd-cli\r\nAccept: application/json\r\n{content_type}Content-Length: {len}\r\nConnection: close\r\n\r\n",
        method = method,
        path = path,
        host = addr,
        content_type = content_type,
        len = body.len()
    )
}

fn http_call(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<(u16, String), String> {
    let mut stream =
        TcpStream::connect(addr).map_err(|e| format!("connect {} failed: {}", addr, e))?;
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(10))).ok();

    let body_bytes = body.unwrap_or("").as_bytes();
    let req = build_http_request(method, addr, path, body_bytes);
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    if !body_bytes.is_empty() {
        stream.write_all(body_bytes).map_err(|e| e.to_string())?;
    }

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).map_err(|e| e.to_string())?;
    let resp_s = String::from_utf8_lossy(&resp).to_string();
    let (head, body) = resp_s
        .split_once("\r\n\r\n")
        .unwrap_or((resp_s.as_str(), ""));
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    Ok((status, body.to_string()))
}

pub(crate) const MAX_RPC_BODY_BYTES: usize = 128 * 1024;
pub(crate) const MAX_RPC_URL_BYTES: usize = 8 * 1024;

#[derive(Parser, Debug)]
#[command(
    name = "dutawalletd",
    about = "DUTA wallet rpc daemon (native)",
    after_help = "Examples:\n  dutawalletd --daemon\n  dutawalletd status\n  dutawalletd stop\n  dutawalletd get-wallet-info\n  duta-wallet-cli --rpc 127.0.0.1:19084 getwalletinfo"
)]
struct Args {
    #[arg(long)]
    testnet: bool,
    #[arg(long)]
    stagenet: bool,

    /// Data directory (default mainnet: ~/.duta, testnet: ~/.duta/testnet, stagenet: ~/.duta/stagenet). Overrides config datadir=
    #[arg(long)]
    datadir: Option<String>,

    /// Path to config file (default: <datadir>/duta.conf)
    #[arg(long)]
    conf: Option<String>,

    /// Run in background (spawn child and exit). Logs to <datadir>/dutawalletd.stdout.log and <datadir>/dutawalletd.stderr.log, and writes PID to <datadir>/dutawalletd.pid
    #[arg(long)]
    daemon: bool,

    /// Internal: run in foreground (used by --daemon). Do not set manually.
    #[arg(long, hide = true)]
    foreground: bool,

    /// Run a one-shot local wallet RPC request instead of starting the daemon
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Subcommand, Debug, Clone)]
enum Cmd {
    /// POST /getnewaddress
    GetNewAddress,
    /// GET /balance
    GetBalance,
    /// POST /rpc {\"method\":\"listunspent\"}
    ListUnspent,
    /// POST /rpc {\"method\":\"getwalletinfo\"}
    GetWalletInfo,
    /// Stop background wallet daemon
    Stop,
    /// Show wallet daemon status
    Status,
}

#[derive(Clone, Debug)]
pub(crate) struct WalletState {
    pub(crate) wallet_path: String,
    pub(crate) primary_address: String,
    pub(crate) keys: BTreeMap<String, String>,
    pub(crate) pubkeys: BTreeMap<String, String>,
    pub(crate) utxos: Vec<Utxo>,
    pub(crate) pending_txs: Vec<PendingTx>,
    pub(crate) reserved_inputs: Vec<ReservedInput>,
    pub(crate) last_sync_height: i64,
    pub(crate) seed_hex: Option<String>,
    pub(crate) next_index: u32,
    pub(crate) is_db: bool,
    pub(crate) locked: bool,
    pub(crate) db_passphrase: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Utxo {
    #[serde(default)]
    pub(crate) value: i64,
    #[serde(default)]
    pub(crate) height: i64,
    #[serde(default)]
    pub(crate) coinbase: bool,
    #[serde(default)]
    pub(crate) address: String,

    // Keep forward-compat with legacy/python fields.
    #[serde(default)]
    pub(crate) txid: String,
    #[serde(default)]
    pub(crate) vout: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PendingTx {
    #[serde(default)]
    pub(crate) txid: String,
    #[serde(default)]
    pub(crate) category: String,
    #[serde(default)]
    pub(crate) amount: i64,
    #[serde(default)]
    pub(crate) fee: i64,
    #[serde(default)]
    pub(crate) change: i64,
    #[serde(default)]
    pub(crate) timestamp: i64,
    #[serde(default)]
    pub(crate) details: Vec<serde_json::Value>,
    #[serde(default)]
    pub(crate) spent_inputs: Vec<PendingInput>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SubmittedTxRecovery {
    pub(crate) pending_tx: PendingTx,
    pub(crate) change_address: String,
    pub(crate) change_vout: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PendingInput {
    #[serde(default)]
    pub(crate) txid: String,
    #[serde(default)]
    pub(crate) vout: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ReservedInput {
    #[serde(default)]
    pub(crate) txid: String,
    #[serde(default)]
    pub(crate) vout: u32,
    #[serde(default)]
    pub(crate) timestamp: i64,
}

static WALLET: OnceLock<Mutex<Option<WalletState>>> = OnceLock::new();
static WALLET_SEND_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub(crate) fn wallet_lock() -> &'static Mutex<Option<WalletState>> {
    WALLET.get_or_init(|| Mutex::new(None))
}

pub(crate) fn wallet_send_lock() -> &'static Mutex<()> {
    WALLET_SEND_LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) fn wallet_lock_or_recover() -> MutexGuard<'static, Option<WalletState>> {
    match wallet_lock().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            wwlog!("dutawalletd: mutex_poison_recovered name=wallet_state");
            poisoned.into_inner()
        }
    }
}

pub(crate) fn respond_json(
    request: tiny_http::Request,
    status: tiny_http::StatusCode,
    body: String,
) {
    let resp = match tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]) {
        Ok(header) => tiny_http::Response::from_string(body)
            .with_status_code(status)
            .with_header(header),
        Err(_) => tiny_http::Response::from_string(body).with_status_code(status),
    };
    let _ = request.respond(resp);
}

pub(crate) fn read_body(request: &mut tiny_http::Request) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut r = request.as_reader().take((MAX_RPC_BODY_BYTES + 1) as u64);
    r.read_to_end(&mut out)
        .map_err(|e| format!("read_body_failed: {}", e))?;
    if out.len() > MAX_RPC_BODY_BYTES {
        return Err(format!("body_too_large: max_bytes={}", MAX_RPC_BODY_BYTES));
    }
    Ok(out)
}

pub(crate) fn request_is_loopback(request: &tiny_http::Request) -> bool {
    request
        .remote_addr()
        .map(|a: &SocketAddr| a.ip().is_loopback())
        .unwrap_or(false)
}

pub(crate) fn durable_write_bytes(path: &str, body: &[u8]) -> Result<(), String> {
    let target = Path::new(path);
    let parent = target
        .parent()
        .ok_or_else(|| format!("wallet_path_parent_missing: {}", path))?;
    fs::create_dir_all(parent).map_err(|e| format!("wallet_mkdir_failed: {}", e))?;

    let tmp = format!("{}.tmp.{}", path, std::process::id());
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| format!("wallet_write_failed: {}", e))?;
        f.write_all(body)
            .map_err(|e| format!("wallet_write_failed: {}", e))?;
        f.sync_all()
            .map_err(|e| format!("wallet_sync_failed: {}", e))?;
    }

    match fs::rename(&tmp, path) {
        Ok(()) => {}
        Err(rename_err) => {
            if target.exists() {
                fs::remove_file(path)
                    .map_err(|e| format!("wallet_replace_remove_failed: {}", e))?;
                fs::rename(&tmp, path)
                    .map_err(|e| format!("wallet_rename_failed: {} (initial={})", e, rename_err))?;
            } else {
                return Err(format!("wallet_rename_failed: {}", rename_err));
            }
        }
    }

    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

pub(crate) fn durable_write_string(path: &str, body: &str) -> Result<(), String> {
    durable_write_bytes(path, body.as_bytes())
}

fn pid_is_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }

    #[cfg(windows)]
    {
        let out = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid)])
            .output();
        return out
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.lines().any(|line| line.contains(&pid.to_string())))
            .unwrap_or(false);
    }

    #[cfg(not(windows))]
    {
        Path::new(&format!("/proc/{}", pid)).exists()
    }
}

fn pid_matches_wallet_process(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }

    #[cfg(windows)]
    {
        return std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid), "/FO", "CSV", "/NH"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.to_ascii_lowercase().contains("dutawalletd"))
            .unwrap_or(false);
    }

    #[cfg(not(windows))]
    {
        let cmdline_path = format!("/proc/{}/cmdline", pid);
        fs::read(&cmdline_path)
            .ok()
            .map(|bytes| {
                String::from_utf8_lossy(&bytes)
                    .to_ascii_lowercase()
                    .contains("dutawalletd")
            })
            .unwrap_or(false)
    }
}

fn read_pid_file(path: &str) -> Result<Option<u32>, String> {
    let raw = match fs::read_to_string(path) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("pid_read_failed: {}", e)),
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let pid = trimmed
        .parse::<u32>()
        .map_err(|_| "pid_parse_failed".to_string())?;
    Ok(Some(pid))
}

fn terminate_pid(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }

    #[cfg(windows)]
    {
        let soft = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if soft {
            return true;
        }
        std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[cfg(not(windows))]
    {
        std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

fn stop_wallet_daemon(data_dir: &str) -> Result<(), String> {
    let pid_path = format!("{}/dutawalletd.pid", data_dir.trim_end_matches('/'));
    let Some(pid) = read_pid_file(&pid_path)? else {
        return Err(format!("wallet_not_running: missing_pid_file={}", pid_path));
    };

    if !pid_is_alive(pid) || !pid_matches_wallet_process(pid) {
        let _ = fs::remove_file(&pid_path);
        return Err(format!(
            "wallet_not_running: stale_pid={} removed_pid_file={}",
            pid, pid_path
        ));
    }

    if !terminate_pid(pid) {
        return Err(format!("wallet_stop_failed: pid={}", pid));
    }

    for _ in 0..50 {
        if !pid_is_alive(pid) {
            let _ = fs::remove_file(&pid_path);
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    Err(format!("wallet_stop_timeout: pid={}", pid))
}

fn wallet_daemon_status(data_dir: &str, rpc_addr: &str) -> Result<i32, String> {
    let pid_path = format!("{}/dutawalletd.pid", data_dir.trim_end_matches('/'));
    let pid = read_pid_file(&pid_path)?;
    let rpc_reachable = matches!(http_call(rpc_addr, "GET", "/health", None), Ok((200, _)));
    match pid {
        Some(pid) if pid_is_alive(pid) && pid_matches_wallet_process(pid) => {
            console_line("WALLET", ANSI_GREEN, "dutawalletd running");
            console_kv("PROC", ANSI_CYAN, "pid", pid.to_string());
            console_kv("RPC", ANSI_BLUE, "bind", rpc_addr);
            console_kv(
                "RPC",
                ANSI_BLUE,
                "reachable",
                if rpc_reachable { "yes" } else { "no" },
            );
            Ok(if rpc_reachable { 0 } else { 1 })
        }
        Some(pid) => {
            let _ = fs::remove_file(&pid_path);
            if rpc_reachable {
                console_line("WALLET", ANSI_GREEN, "dutawalletd running");
                console_kv("PROC", ANSI_YELLOW, "stale_pid_removed", pid.to_string());
                console_kv("PROC", ANSI_YELLOW, "pid", "externally_managed_or_missing");
                console_kv("RPC", ANSI_BLUE, "bind", rpc_addr);
                console_kv("RPC", ANSI_BLUE, "reachable", "yes");
                Ok(0)
            } else {
                console_line("WALLET", ANSI_YELLOW, "dutawalletd stopped");
                console_kv("PROC", ANSI_YELLOW, "stale_pid_removed", pid.to_string());
                console_kv("RPC", ANSI_BLUE, "bind", rpc_addr);
                console_kv("RPC", ANSI_BLUE, "reachable", "no");
                Ok(1)
            }
        }
        None => {
            if rpc_reachable {
                console_line("WALLET", ANSI_GREEN, "dutawalletd running");
                console_kv("PROC", ANSI_YELLOW, "pid", "externally_managed_or_missing");
                console_kv("RPC", ANSI_BLUE, "bind", rpc_addr);
                console_kv("RPC", ANSI_BLUE, "reachable", "yes");
                Ok(0)
            } else {
                console_line("WALLET", ANSI_YELLOW, "dutawalletd stopped");
                console_kv("RPC", ANSI_BLUE, "bind", rpc_addr);
                console_kv("RPC", ANSI_BLUE, "reachable", "no");
                Ok(1)
            }
        }
    }
}

pub(crate) fn request_content_type_is_json(request: &tiny_http::Request) -> bool {
    request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Content-Type"))
        .and_then(|h| {
            let v = h.value.as_str().trim().to_ascii_lowercase();
            Some(v.starts_with("application/json"))
        })
        .unwrap_or(false)
}

pub(crate) fn respond_415(request: tiny_http::Request) {
    respond_json(
        request,
        tiny_http::StatusCode(415),
        serde_json::json!({"error":"unsupported_media_type"}).to_string(),
    );
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
        return Some(pos + 4);
    }
    if let Some(pos) = buf.windows(2).position(|w| w == b"\n\n") {
        return Some(pos + 2);
    }
    None
}

fn parse_chunked_body(mut body: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let mut out: Vec<u8> = Vec::new();

    loop {
        // Need "<hex>\r\n"
        let line_end = body
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| "chunked_incomplete".to_string())?;
        let (line, rest) = body.split_at(line_end);
        let rest = &rest[2..]; // skip CRLF

        let line_str = std::str::from_utf8(line).map_err(|_| "chunked_bad_size".to_string())?;
        let size_str = line_str.split(';').next().unwrap_or("").trim();
        let size =
            usize::from_str_radix(size_str, 16).map_err(|_| "chunked_bad_size".to_string())?;

        if size == 0 {
            // Final chunk. Expect trailing "\r\n" (maybe after trailers); simplest: require at least CRLF.
            if rest.len() < 2 {
                return Ok(None);
            }
            return Ok(Some(out));
        }

        if rest.len() < size + 2 {
            return Ok(None);
        }

        out.extend_from_slice(&rest[..size]);

        // Next must be CRLF after chunk data.
        if &rest[size..size + 2] != b"\r\n" {
            return Err("chunked_bad_crlf".to_string());
        }

        body = &rest[size + 2..];
    }
}

fn decode_http_body(buf: &[u8]) -> Result<Vec<u8>, String> {
    let hend = find_header_end(buf).ok_or_else(|| "bad_http_response".to_string())?;
    let hdr = String::from_utf8_lossy(&buf[..hend]).to_ascii_lowercase();

    let is_chunked = hdr.contains("transfer-encoding: chunked");

    if is_chunked {
        match parse_chunked_body(&buf[hend..])? {
            Some(b) => Ok(b),
            None => Err("chunked_incomplete".to_string()),
        }
    } else {
        // If Content-Length exists, respect it; otherwise return whatever remains.
        if let Some(i) = hdr.find("content-length:") {
            let rest = &hdr[i + "content-length:".len()..];
            let len_str = rest
                .split(|c: char| c == '\r' || c == '\n')
                .next()
                .unwrap_or("")
                .trim();
            if let Ok(clen) = len_str.parse::<usize>() {
                if buf.len() < hend + clen {
                    return Err("body_incomplete".to_string());
                }
                return Ok(buf[hend..hend + clen].to_vec());
            }
        }
        Ok(buf[hend..].to_vec())
    }
}

fn read_http_response(stream: &mut TcpStream, deadline_secs: u64) -> Result<Vec<u8>, String> {
    stream.set_read_timeout(Some(Duration::from_secs(1))).ok();

    let start = std::time::Instant::now();
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];

    // framing
    let mut header_end: Option<usize> = None;
    let mut is_chunked: Option<bool> = None;
    let mut content_len: Option<usize> = None;

    loop {
        if start.elapsed() > Duration::from_secs(deadline_secs) {
            return Err("read_timeout".to_string());
        }

        match stream.read(&mut tmp) {
            Ok(0) => break, // EOF
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);

                if header_end.is_none() {
                    if let Some(hend) = find_header_end(&buf) {
                        header_end = Some(hend);
                        let hdr = String::from_utf8_lossy(&buf[..hend]).to_ascii_lowercase();
                        is_chunked = Some(hdr.contains("transfer-encoding: chunked"));

                        if let Some(i) = hdr.find("content-length:") {
                            let rest = &hdr[i + "content-length:".len()..];
                            let len_str = rest
                                .split(|c: char| c == '\r' || c == '\n')
                                .next()
                                .unwrap_or("")
                                .trim();
                            if let Ok(n) = len_str.parse::<usize>() {
                                content_len = Some(n);
                            }
                        }
                    }
                }

                if let Some(hend) = header_end {
                    if is_chunked == Some(true) {
                        if let Ok(Some(_)) = parse_chunked_body(&buf[hend..]) {
                            break;
                        }
                    } else if let Some(clen) = content_len {
                        if buf.len() >= hend + clen {
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) {
                    continue;
                }
                return Err(format!("read_failed: {}", e));
            }
        }
    }

    Ok(buf)
}

pub(crate) fn http_get_local_with_deadline(
    host: &str,
    port: u16,
    path: &str,
    deadline_secs: u64,
) -> Result<String, String> {
    let mut stream =
        TcpStream::connect((host, port)).map_err(|e| format!("connect_failed: {}", e))?;
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();

    let req = format!(
        "GET {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\n\r\n",
        path, host, port
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("write_failed: {}", e))?;

    let buf = read_http_response(&mut stream, deadline_secs)?;
    let body = decode_http_body(&buf)?;
    Ok(String::from_utf8_lossy(&body).to_string())
}

pub(crate) fn http_get_local(host: &str, port: u16, path: &str) -> Result<String, String> {
    http_get_local_with_deadline(host, port, path, 20)
}

pub(crate) fn http_post_local(
    host: &str,
    port: u16,
    path: &str,
    content_type: &str,
    body: &[u8],
) -> Result<String, String> {
    let mut stream =
        TcpStream::connect((host, port)).map_err(|e| format!("connect_failed: {}", e))?;
    stream.set_write_timeout(Some(Duration::from_secs(3))).ok();

    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        path,
        host,
        port,
        content_type,
        body.len()
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("write_failed: {}", e))?;
    stream
        .write_all(body)
        .map_err(|e| format!("write_failed: {}", e))?;

    let buf = read_http_response(&mut stream, 20)?;
    let body = decode_http_body(&buf)?;
    Ok(String::from_utf8_lossy(&body).to_string())
}
pub(crate) fn load_wallet_from_path(path: &str) -> Result<WalletState, String> {
    if path.ends_with(".db") || path.ends_with(".dat") {
        return load_wallet_db_to_state(path);
    }
    return Err("legacy_plaintext_wallet_disabled_use_db_wallet".to_string());
}

fn cleanup_wallet_stage_files(path: &str) {
    let target = std::path::Path::new(path);
    let Some(parent) = target.parent() else {
        return;
    };
    let Some(name) = target.file_name().and_then(|s| s.to_str()) else {
        return;
    };
    let prefix = format!("{name}.importing-");
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let entry_path = entry.path();
        let Some(entry_name) = entry_path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if entry_name.starts_with(&prefix) {
            let _ = std::fs::remove_file(entry_path);
        }
    }
}

pub(crate) fn load_wallet_db_to_state(path: &str) -> Result<WalletState, String> {
    if std::fs::metadata(path).is_err() {
        cleanup_wallet_stage_files(path);
        return Err("wallet_not_found".to_string());
    }
    let db = walletdb::WalletDb::open(path)?;
    let rows = db.list_keys()?;
    let next_index = db.read_next_index()?.max(0) as u32;
    let utxos = db.read_utxos()?;
    let pending_txs = db.read_pending_txs()?;
    let reserved_inputs = db.read_reserved_inputs()?;
    let last_sync_height = db.read_last_sync_height()?;
    let mut primary_address = db.read_primary_address()?;
    if primary_address.is_empty() {
        primary_address = rows.get(0).map(|r| r.addr.clone()).unwrap_or_default();
    }
    let mut pubkeys = BTreeMap::new();
    for r in rows {
        pubkeys.insert(r.addr, r.pubkey_hex);
    }
    let ws = WalletState {
        wallet_path: path.to_string(),
        primary_address,
        keys: BTreeMap::new(), // locked by default
        pubkeys,
        utxos,
        pending_txs,
        reserved_inputs,
        last_sync_height,
        seed_hex: None,
        next_index,
        is_db: true,
        locked: true,
        db_passphrase: None,
    };
    validate_wallet_state_addresses(&ws)?;
    Ok(ws)
}

pub(crate) fn clear_wallet_sensitive_state(ws: &mut WalletState) {
    for value in ws.keys.values_mut() {
        value.zeroize();
    }
    ws.keys.clear();
    if let Some(seed_hex) = ws.seed_hex.as_mut() {
        seed_hex.zeroize();
    }
    ws.seed_hex = None;
    if let Some(passphrase) = ws.db_passphrase.as_mut() {
        passphrase.zeroize();
    }
    ws.db_passphrase = None;
}

fn validate_wallet_key_material(
    ws: &WalletState,
    net: duta_core::netparams::Network,
) -> Result<(), String> {
    for (addr, sk_hex) in ws.keys.iter() {
        let sk_bytes = hex::decode(sk_hex).map_err(|_| "wallet_key_invalid".to_string())?;
        if sk_bytes.len() != 32 {
            return Err("wallet_key_invalid".to_string());
        }
        let mut ent = [0u8; 32];
        ent.copy_from_slice(&sk_bytes);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&ent);
        let pubkey_hex = hex::encode(signing_key.verifying_key().to_bytes());
        let expected_addr = duta_core::address::pkh_to_address_for_network(
            net,
            &duta_core::address::pkh_from_pubkey(signing_key.verifying_key().as_bytes()),
        );
        if expected_addr != *addr {
            return Err("wallet_key_address_mismatch".to_string());
        }
        if let Some(stored_pubkey_hex) = ws.pubkeys.get(addr) {
            if *stored_pubkey_hex != pubkey_hex {
                return Err("wallet_pubkey_mismatch".to_string());
            }
        }
    }

    for (addr, pubkey_hex) in ws.pubkeys.iter() {
        let pubkey_bytes =
            hex::decode(pubkey_hex).map_err(|_| "wallet_pubkey_invalid".to_string())?;
        if pubkey_bytes.len() != 32 {
            return Err("wallet_pubkey_invalid".to_string());
        }
        let expected_addr = duta_core::address::pkh_to_address_for_network(
            net,
            &duta_core::address::pkh_from_pubkey(&pubkey_bytes),
        );
        if expected_addr != *addr {
            return Err("wallet_pubkey_address_mismatch".to_string());
        }
    }

    Ok(())
}

fn validate_wallet_state_addresses(ws: &WalletState) -> Result<(), String> {
    let mut addrs: Vec<&str> = ws
        .pubkeys
        .keys()
        .map(|s| s.as_str())
        .chain(ws.keys.keys().map(|s| s.as_str()))
        .collect();
    addrs.sort_unstable();
    addrs.dedup();
    if addrs.is_empty() {
        return Err("wallet_keys_empty".to_string());
    }

    let net = duta_core::address::detect_network(addrs[0])
        .ok_or_else(|| "wallet_address_invalid".to_string())?;
    for addr in addrs.iter().copied() {
        if duta_core::address::parse_address_for_network(net, addr).is_none() {
            return Err("wallet_address_invalid".to_string());
        }
    }
    if duta_core::address::parse_address_for_network(net, &ws.primary_address).is_none() {
        return Err("wallet_primary_address_invalid".to_string());
    }
    if !addrs.iter().any(|addr| *addr == ws.primary_address) {
        return Err("wallet_primary_address_unknown".to_string());
    }
    validate_wallet_key_material(ws, net)?;

    if ws.primary_address.is_empty() {
        return Err("wallet_primary_address_missing".to_string());
    }
    if duta_core::address::parse_address_for_network(net, &ws.primary_address).is_none() {
        return Err("wallet_primary_address_invalid".to_string());
    }
    if !addrs.iter().any(|addr| *addr == ws.primary_address) {
        return Err("wallet_primary_address_not_found".to_string());
    }

    Ok(())
}
pub(crate) fn save_wallet_sync_state(
    path: &str,
    utxos: &[Utxo],
    last_sync_height: i64,
    reserved_inputs: &[ReservedInput],
) -> Result<(), String> {
    if !(path.ends_with(".db") || path.ends_with(".dat")) {
        return Err("legacy_plaintext_wallet_disabled_use_db_wallet".to_string());
    }
    let db = walletdb::WalletDb::open(path)?;
    db.update_sync_state(utxos, last_sync_height, reserved_inputs)
}

pub(crate) fn save_wallet_sync_state_with_recovery(
    path: &str,
    utxos: &[Utxo],
    last_sync_height: i64,
    reserved_inputs: &[ReservedInput],
    recovery: Option<&SubmittedTxRecovery>,
) -> Result<(), String> {
    if !(path.ends_with(".db") || path.ends_with(".dat")) {
        return Err("legacy_plaintext_wallet_disabled_use_db_wallet".to_string());
    }
    let db = walletdb::WalletDb::open(path)?;
    db.update_sync_state_with_recovery(utxos, last_sync_height, reserved_inputs, recovery)
}

pub(crate) fn save_wallet_pending_txs(path: &str, pending_txs: &[PendingTx]) -> Result<(), String> {
    if !(path.ends_with(".db") || path.ends_with(".dat")) {
        return Err("legacy_plaintext_wallet_disabled_use_db_wallet".to_string());
    }
    let db = walletdb::WalletDb::open(path)?;
    db.update_pending_txs(pending_txs)
}

pub(crate) fn save_wallet_full_state(
    path: &str,
    utxos: &[Utxo],
    last_sync_height: i64,
    pending_txs: &[PendingTx],
    reserved_inputs: &[ReservedInput],
) -> Result<(), String> {
    if !(path.ends_with(".db") || path.ends_with(".dat")) {
        return Err("legacy_plaintext_wallet_disabled_use_db_wallet".to_string());
    }
    let db = walletdb::WalletDb::open(path)?;
    db.update_full_state(utxos, last_sync_height, pending_txs, reserved_inputs)
}

pub(crate) fn save_wallet_full_state_with_recovery(
    path: &str,
    utxos: &[Utxo],
    last_sync_height: i64,
    pending_txs: &[PendingTx],
    reserved_inputs: &[ReservedInput],
    recovery: Option<&SubmittedTxRecovery>,
) -> Result<(), String> {
    if !(path.ends_with(".db") || path.ends_with(".dat")) {
        return Err("legacy_plaintext_wallet_disabled_use_db_wallet".to_string());
    }
    let db = walletdb::WalletDb::open(path)?;
    db.update_full_state_with_recovery(
        utxos,
        last_sync_height,
        pending_txs,
        reserved_inputs,
        recovery,
    )
}

fn start_wallet_rpc(rpc_addr: String, daemon_rpc_port: u16, net: String) -> Result<(), String> {
    let server = tiny_http::Server::http(&rpc_addr)
        .map_err(|e| format!("wallet_rpc_bind_failed addr={} err={}", rpc_addr, e))?;

    wdlog!("wallet_rpc: listening on http://{}", rpc_addr);
    console_line("RPC", ANSI_GREEN, format!("listening on http://{}", rpc_addr));

    for request in server.incoming_requests() {
        router::handle_request(request, &rpc_addr, daemon_rpc_port, &net);
    }
    Ok(())
}

fn normalize_path_maybe_home(p: &str) -> String {
    let mut s = p.trim().to_string();
    if s.is_empty() {
        return s;
    }

    let home_dir = || -> Option<String> {
        std::env::var("HOME")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                std::env::var("USERPROFILE")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
            })
            .or_else(|| {
                let drive = std::env::var("HOMEDRIVE").ok()?;
                let path = std::env::var("HOMEPATH").ok()?;
                let joined = format!("{}{}", drive.trim(), path.trim());
                if joined.trim().is_empty() {
                    None
                } else {
                    Some(joined)
                }
            })
    };

    if s == "~" || s.starts_with("~/") || s.starts_with("~\\") {
        if let Some(home) = home_dir() {
            let home = home.trim_end_matches(['/', '\\']).to_string();
            if s == "~" {
                return home;
            }
            s = format!("{}/{}", home, s[1..].trim_start_matches(['/', '\\']));
        }
    } else if s.starts_with(".duta/") || s.starts_with(".duta\\") {
        if let Some(home) = home_dir() {
            let home = home.trim_end_matches(['/', '\\']).to_string();
            s = format!("{}/{}", home, s);
        }
    }
    s
}

fn write_pid_file(data_dir: &str, name: &str) -> std::io::Result<()> {
    let path = format!("{}/{}", data_dir.trim_end_matches('/'), name);
    let pid = std::process::id();
    std::fs::create_dir_all(data_dir)?;
    durable_write_string(&path, &format!("{}\n", pid)).map_err(std::io::Error::other)
}

fn prepare_wallet_runtime_files(data_dir: &str) -> Result<PidFileGuard, String> {
    write_pid_file(data_dir, "dutawalletd.pid")
        .map_err(|e| format!("wallet_write_pid_failed: {}", e))?;
    let pid_guard = PidFileGuard::new(data_dir, "dutawalletd.pid");
    if let Err(e) = walletlog::init(data_dir) {
        remove_pid_file_if_matches(&pid_guard.path, pid_guard.pid);
        return Err(format!("wallet_log_init_failed: {}", e));
    }
    Ok(pid_guard)
}

struct PidFileGuard {
    path: String,
    pid: u32,
}

impl PidFileGuard {
    fn new(data_dir: &str, name: &str) -> Self {
        Self {
            path: format!("{}/{}", data_dir.trim_end_matches('/'), name),
            pid: std::process::id(),
        }
    }
}

fn remove_pid_file_if_matches(path: &str, pid: u32) {
    match read_pid_file(path) {
        Ok(Some(current_pid)) if current_pid == pid => {
            let _ = fs::remove_file(path);
        }
        _ => {}
    }
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        remove_pid_file_if_matches(&self.path, self.pid);
    }
}

fn install_pid_cleanup_handlers(data_dir: &str, name: &str) {
    let path = format!("{}/{}", data_dir.trim_end_matches('/'), name);
    let pid = std::process::id();
    let _ = ctrlc::set_handler(move || {
        remove_pid_file_if_matches(&path, pid);
        std::process::exit(0);
    });

    #[cfg(unix)]
    {
        use signal_hook::consts::signal::{SIGINT, SIGTERM};
        use signal_hook::iterator::Signals;

        let path = format!("{}/{}", data_dir.trim_end_matches('/'), name);
        std::thread::spawn(move || {
            let mut signals = match Signals::new([SIGTERM, SIGINT]) {
                Ok(v) => v,
                Err(_) => return,
            };
            if signals.forever().next().is_some() {
                remove_pid_file_if_matches(&path, pid);
                std::process::exit(0);
            }
        });
    }
}

fn wait_for_wallet_rpc_ready(rpc_addr: &str, child_pid: u32, timeout_ms: u64) -> Result<(), String> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    let mut last_err = String::new();
    while std::time::Instant::now() < deadline {
        if !pid_is_alive(child_pid) {
            return Err(format!("wallet_daemon_exited_early: pid={}", child_pid));
        }
        match http_call(rpc_addr, "GET", "/health", None) {
            Ok((200, _)) => return Ok(()),
            Ok((status, _)) => last_err = format!("wallet_health_status={}", status),
            Err(e) => last_err = e,
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!(
        "wallet_daemon_not_ready: rpc={} timeout_ms={} last_err={}",
        rpc_addr, timeout_ms, last_err
    ))
}

fn parse_wallet_loopback_bind(bind: &str, default_port: u16) -> Result<Option<String>, String> {
    let bind = bind.trim();
    if bind.is_empty() {
        return Ok(None);
    }
    if bind == "127.0.0.1" {
        return Ok(Some(format!("127.0.0.1:{}", default_port)));
    }
    if let Some((host, port)) = bind.split_once(':') {
        let host = host.trim();
        let port = port
            .trim()
            .parse::<u16>()
            .map_err(|_| format!("invalid_wallet_rpc_bind: {}", bind))?;
        if port == 0 {
            return Err(format!("invalid_wallet_rpc_bind: {}", bind));
        }
        if host == "127.0.0.1" {
            return Ok(Some(format!("127.0.0.1:{}", port)));
        }
        return Ok(None);
    }
    Ok(None)
}

fn wallet_rpc_bind_warning(net: Network, conf: &duta_core::netparams::Conf) -> Option<String> {
    let raw = conf
        .get_last("walletrpcbind")
        .or_else(|| conf.get_last("rpcbind"))?;
    if matches!(
        parse_wallet_loopback_bind(&raw, net.default_wallet_rpc_port()),
        Ok(Some(_))
    ) {
        return None;
    }
    Some(format!(
        "ignoring non-loopback wallet rpc bind override '{}' and keeping loopback-only policy",
        raw.trim()
    ))
}

fn wallet_rpc_settings(
    net: Network,
    conf: &duta_core::netparams::Conf,
) -> Result<(String, u16, String), String> {
    let (mut rpc_addr, mut daemon_rpc_port, net_s) = match net {
        Network::Mainnet => (
            "127.0.0.1:19084".to_string(),
            19083u16,
            "mainnet".to_string(),
        ),
        Network::Testnet => (
            "127.0.0.1:18084".to_string(),
            18083u16,
            "testnet".to_string(),
        ),
        Network::Stagenet => (
            "127.0.0.1:17084".to_string(),
            17083u16,
            "stagenet".to_string(),
        ),
    };

    if let Some(b) = conf
        .get_last("walletrpcbind")
        .or_else(|| conf.get_last("rpcbind"))
    {
        if let Some(addr) = parse_wallet_loopback_bind(&b, net.default_wallet_rpc_port())? {
            rpc_addr = addr;
        }
    }

    if let Some(p) = conf
        .get_last("daemonrpcport")
        .or_else(|| conf.get_last("rpcport"))
    {
        let v = p
            .trim()
            .parse::<u16>()
            .map_err(|_| format!("invalid_daemon_rpc_port: {}", p.trim()))?;
        if v == 0 {
            return Err(format!("invalid_daemon_rpc_port: {}", p.trim()));
        }
        daemon_rpc_port = v;
    }

    Ok((rpc_addr, daemon_rpc_port, net_s))
}

fn validate_conf_network_name(conf: &duta_core::netparams::Conf) -> Result<(), String> {
    if let Some(raw) = conf.get_last("network").or_else(|| conf.get_last("chain")) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() && Network::parse_name(trimmed).is_none() {
            return Err(format!("invalid_network_name: {}", trimmed));
        }
    }
    Ok(())
}

fn validate_conf_wallet_rpc_settings(
    net: Network,
    conf: &duta_core::netparams::Conf,
) -> Result<(), String> {
    if let Some(raw) = conf
        .get_last("walletrpcbind")
        .or_else(|| conf.get_last("rpcbind"))
    {
        let _ = parse_wallet_loopback_bind(&raw, net.default_wallet_rpc_port())?;
    }
    if let Some(raw) = conf
        .get_last("daemonrpcport")
        .or_else(|| conf.get_last("rpcport"))
    {
        let port = raw
            .trim()
            .parse::<u16>()
            .map_err(|_| format!("invalid_daemon_rpc_port: {}", raw.trim()))?;
        if port == 0 {
            return Err(format!("invalid_daemon_rpc_port: {}", raw.trim()));
        }
    }
    Ok(())
}

fn load_runtime_conf(conf_path: &str, required: bool) -> Result<duta_core::netparams::Conf, String> {
    match fs::read_to_string(conf_path) {
        Ok(s) => Ok(duta_core::netparams::Conf::parse(&s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !required => {
            Ok(duta_core::netparams::Conf::default())
        }
        Err(e) => Err(format!("config_read_failed: path={} err={}", conf_path, e)),
    }
}

fn spawn_daemon_wallet(data_dir: &str, rpc_addr: &str) -> Result<u32, String> {
    use std::process::{Command, Stdio};
    std::fs::create_dir_all(data_dir).map_err(|e| e.to_string())?;
    let pid_path = format!("{}/dutawalletd.pid", data_dir.trim_end_matches('/'));
    match read_pid_file(&pid_path) {
        Ok(Some(existing_pid)) => {
            if pid_is_alive(existing_pid) && pid_matches_wallet_process(existing_pid) {
                return Err(format!(
                    "wallet_daemon_already_running: pid={}",
                    existing_pid
                ));
            }
            let _ = std::fs::remove_file(&pid_path);
        }
        Ok(None) | Err(_) => {
            let _ = std::fs::remove_file(&pid_path);
        }
    }

    let debug_log_path = format!("{}/dutawalletd.stdout.log", data_dir.trim_end_matches('/'));
    let error_log_path = format!("{}/dutawalletd.stderr.log", data_dir.trim_end_matches('/'));
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&debug_log_path)
        .map_err(|e| format!("open log {}: {}", debug_log_path, e))?;
    let err_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&error_log_path)
        .map_err(|e| format!("open log {}: {}", error_log_path, e))?;

    let mut cmd = Command::new(std::env::current_exe().map_err(|e| e.to_string())?);
    for a in std::env::args().skip(1) {
        if a == "--daemon" {
            continue;
        }
        cmd.arg(a);
    }
    cmd.arg("--foreground");

    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::from(
        log_file.try_clone().map_err(|e| e.to_string())?,
    ));
    cmd.stderr(Stdio::from(err_file));

    let mut child = cmd.spawn().map_err(|e| e.to_string())?;
    durable_write_string(&pid_path, &format!("{}\n", child.id()))
        .map_err(|e| format!("write pid {}: {}", pid_path, e))?;
    if let Err(e) = wait_for_wallet_rpc_ready(rpc_addr, child.id(), 5000) {
        let _ = child.kill();
        let _ = child.wait();
        let _ = fs::remove_file(&pid_path);
        return Err(e);
    }
    Ok(child.id())
}

fn main() {
    let args = Args::parse();
    let command = args.command.clone();

    let net = if args.stagenet {
        Network::Stagenet
    } else if args.testnet {
        Network::Testnet
    } else {
        Network::Mainnet
    };

    // Resolve data dir early so we can load $DATADIR/duta.conf.
    let mut data_dir = net.default_data_dir_unix().to_string();
    data_dir = normalize_path_maybe_home(&data_dir);
    if let Some(dd) = args.datadir.as_deref() {
        let dd2 = normalize_path_maybe_home(dd);
        if !dd2.is_empty() {
            data_dir = dd2;
        }
    }

    // Optional config file (bitcoin.conf style): default $DATADIR/duta.conf, or CLI --conf
    let mut conf = duta_core::netparams::Conf::default();
    let mut conf_path = format!("{}/duta.conf", data_dir.trim_end_matches('/'));
    if let Some(cp) = args.conf.as_deref() {
        let cp2 = normalize_path_maybe_home(cp);
        if !cp2.is_empty() {
            conf_path = cp2;
        }
    }
    match load_runtime_conf(&conf_path, args.conf.is_some()) {
        Ok(parsed) => conf = parsed,
        Err(e) => {
            eprintln!("dutawalletd: {}", e);
            std::process::exit(1);
        }
    }
    if let Err(e) = validate_conf_network_name(&conf) {
        eprintln!("dutawalletd: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = validate_conf_wallet_rpc_settings(net, &conf) {
        eprintln!("dutawalletd: {}", e);
        std::process::exit(1);
    }

    // datadir= override (optional). If present, re-load config from that dir.
    // NOTE: CLI --datadir always wins.
    if args.datadir.is_none() {
        if let Some(dd) = conf.get_last("datadir") {
            let dd2 = normalize_path_maybe_home(&dd);
            if !dd2.is_empty() && dd2 != data_dir {
                data_dir = dd2;
                if args.conf.is_none() {
                    conf_path = format!("{}/duta.conf", data_dir.trim_end_matches('/'));
                }
                match load_runtime_conf(&conf_path, args.conf.is_some()) {
                    Ok(parsed) => conf = parsed,
                    Err(e) => {
                        eprintln!("dutawalletd: {}", e);
                        std::process::exit(1);
                    }
                }
                if let Err(e) = validate_conf_network_name(&conf) {
                    eprintln!("dutawalletd: {}", e);
                    std::process::exit(1);
                }
                if let Err(e) = validate_conf_wallet_rpc_settings(net, &conf) {
                    eprintln!("dutawalletd: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    let (rpc_addr, daemon_rpc_port, net_s) = match wallet_rpc_settings(net, &conf) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("dutawalletd: {}", e);
            std::process::exit(1);
        }
    };
    let startup_bind_warning = wallet_rpc_bind_warning(net, &conf);

    if matches!(args.command, Some(Cmd::Stop)) {
        if args.daemon || args.foreground {
            eprintln!("dutawalletd: stop cannot be combined with daemon flags");
            std::process::exit(1);
        }
        match stop_wallet_daemon(&data_dir) {
            Ok(()) => {
                console_line("WALLET", ANSI_YELLOW, "dutawalletd stopped");
                return;
            }
            Err(e) => {
                eprintln!("dutawalletd stop failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    if matches!(args.command, Some(Cmd::Status)) {
        if args.daemon || args.foreground {
            eprintln!("dutawalletd: status cannot be combined with daemon flags");
            std::process::exit(1);
        }
        match wallet_daemon_status(&data_dir, &rpc_addr) {
            Ok(rc) => std::process::exit(rc),
            Err(e) => {
                eprintln!("dutawalletd status failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    if let Some(cmd) = command {
        let call = match cmd {
            Cmd::GetNewAddress => http_call(&rpc_addr, "POST", "/getnewaddress", Some("")),
            Cmd::GetBalance => http_call(&rpc_addr, "GET", "/balance", None),
            Cmd::ListUnspent => http_call(
                &rpc_addr,
                "POST",
                "/rpc",
                Some(r#"{"jsonrpc":"2.0","id":1,"method":"listunspent","params":[]}"#),
            ),
            Cmd::GetWalletInfo => http_call(
                &rpc_addr,
                "POST",
                "/rpc",
                Some(r#"{"jsonrpc":"2.0","id":1,"method":"getwalletinfo","params":[]}"#),
            ),
            Cmd::Stop => unreachable!("stop is handled before wallet RPC command dispatch"),
            Cmd::Status => unreachable!("status is handled before wallet RPC command dispatch"),
        };
        match call {
            Ok((status, body)) if (200..=299).contains(&status) => {
                print!("{}", body);
                return;
            }
            Ok((status, body)) if status > 0 => {
                if !body.trim().is_empty() {
                    eprintln!("{}", body);
                } else {
                    eprintln!("error: wallet rpc call failed with HTTP {}", status);
                }
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("error: wallet rpc call failed: {}", e);
                std::process::exit(1);
            }
            _ => {
                eprintln!("error: wallet rpc call failed");
                std::process::exit(1);
            }
        }
    }

    // --daemon support: spawn a detached child process, redirect stdout/stderr to explicit log files,
    // write PID to <datadir>/dutawalletd.pid, wait until RPC is reachable, then exit the parent.
    if args.daemon && !args.foreground && args.command.is_none() {
        match spawn_daemon_wallet(&data_dir, &rpc_addr) {
            Ok(pid) => {
                console_line("WALLET", ANSI_GREEN, format!("dutawalletd started (pid={})", pid));
            }
            Err(e) => {
                eprintln!("dutawalletd --daemon failed: {}", e);
                std::process::exit(1);
            }
        }
        return;
    }
    let _pid_guard = match prepare_wallet_runtime_files(&data_dir) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("dutawalletd: {}", e);
            std::process::exit(1);
        }
    };
    install_pid_cleanup_handlers(&data_dir, "dutawalletd.pid");

    std::panic::set_hook(Box::new(|info| {
        wedlog!("wallet_rpc: PANIC {}", info);
    }));
    wdlog!(
        "wallet_rpc: START data={} stdout_log={}/dutawalletd.stdout.log stderr_log={}/dutawalletd.stderr.log",
        data_dir,
        data_dir,
        data_dir
    );

    print_wallet_startup_banner(net, &data_dir, &rpc_addr, daemon_rpc_port);
    if let Some(msg) = startup_bind_warning.as_deref() {
        print_wallet_startup_warning(msg);
        wwlog!("wallet_rpc: CONFIG_WARN {}", msg);
    }
    print_wallet_startup_guidance(&rpc_addr);

    if let Err(e) = start_wallet_rpc(rpc_addr, daemon_rpc_port, net_s) {
        wedlog!("wallet_rpc: {}", e);
        eprintln!("wallet_rpc: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_http_request, clear_wallet_sensitive_state, load_wallet_db_to_state, read_pid_file,
        load_runtime_conf, remove_pid_file_if_matches, save_wallet_sync_state, validate_conf_network_name,
        validate_conf_wallet_rpc_settings, wallet_rpc_bind_warning, Args, Cmd,
        validate_wallet_state_addresses,
        wallet_rpc_settings, PidFileGuard, Utxo, WalletState,
    };
    use crate::{PendingInput, PendingTx, ReservedInput};
    use clap::Parser;
    use duta_core::address::{pkh_from_pubkey, pkh_to_address_for_network};
    use duta_core::netparams::{Conf, Network};
    use ed25519_dalek::SigningKey;
    use crate::walletdb::{WalletDb, WALLET_DB_SCHEMA_VERSION};
    use std::collections::BTreeMap;
    use std::fs;

    fn sample_wallet_state() -> WalletState {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let pubkey_hex = hex::encode(signing_key.verifying_key().to_bytes());
        let address = pkh_to_address_for_network(
            Network::Mainnet,
            &pkh_from_pubkey(signing_key.verifying_key().as_bytes()),
        );
        let mut keys = BTreeMap::new();
        keys.insert(address.clone(), hex::encode([7u8; 32]));
        let mut pubkeys = BTreeMap::new();
        pubkeys.insert(address.clone(), pubkey_hex);
        WalletState {
            wallet_path: "wallet.json".to_string(),
            primary_address: address.clone(),
            keys,
            pubkeys,
            utxos: vec![Utxo {
                value: 1,
                height: 1,
                coinbase: false,
                address: address.clone(),
                txid: "tx".to_string(),
                vout: 0,
            }],
            pending_txs: Vec::new(),
            reserved_inputs: Vec::new(),
            last_sync_height: 1,
            seed_hex: Some("deadbeef".to_string()),
            next_index: 1,
            is_db: false,
            locked: false,
            db_passphrase: Some("secret-pass".to_string()),
        }
    }

    #[test]
    fn post_requests_send_json_content_type() {
        let req = build_http_request("POST", "127.0.0.1:19084", "/rpc", br#"{"id":1}"#);
        assert!(req.contains("\r\nContent-Type: application/json\r\n"));
        assert!(req.contains("Content-Length: 8\r\n"));
    }

    #[test]
    fn get_requests_do_not_force_json_content_type() {
        let req = build_http_request("GET", "127.0.0.1:19084", "/balance", b"");
        assert!(!req.contains("Content-Type: application/json"));
        assert!(req.contains("Content-Length: 0\r\n"));
    }

    #[test]
    fn wallet_state_validation_rejects_mismatched_pubkey_mapping() {
        let mut ws = sample_wallet_state();
        ws.pubkeys
            .insert(ws.primary_address.clone(), hex::encode([9u8; 32]));
        assert_eq!(
            validate_wallet_state_addresses(&ws),
            Err("wallet_pubkey_mismatch".to_string())
        );
    }

    #[test]
    fn wallet_state_validation_rejects_unknown_primary_address() {
        let mut ws = sample_wallet_state();
        ws.primary_address = "dut34eb9f0d0d9e0ec7fbf78e6bfc7277f10f01aaf8".to_string();
        assert_eq!(
            validate_wallet_state_addresses(&ws),
            Err("wallet_primary_address_unknown".to_string())
        );
    }

    #[test]
    fn sensitive_wallet_state_is_zeroized_and_cleared() {
        let mut ws = sample_wallet_state();
        clear_wallet_sensitive_state(&mut ws);
        assert!(ws.keys.is_empty());
        assert!(ws.seed_hex.is_none());
        assert!(ws.db_passphrase.is_none());
    }

    #[test]
    fn save_wallet_sync_state_rejects_plaintext_wallet_paths() {
        let err = save_wallet_sync_state("wallet.json", &[], 0, &[]).unwrap_err();
        assert_eq!(err, "legacy_plaintext_wallet_disabled_use_db_wallet");
    }

    #[test]
    fn load_wallet_db_to_state_rejects_corrupt_utxo_metadata() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-wallet-corrupt-utxos-{}.db", uniq));
        let path = p.to_string_lossy().to_string();

        let _db = WalletDb::create_new(&path, "strong-pass-123", &[7u8; 32], 1).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('utxos_json', ?1)",
            rusqlite::params![b"{not-json".to_vec()],
        )
        .unwrap();
        drop(conn);

        let err = load_wallet_db_to_state(&path).unwrap_err();
        assert!(err.contains("db_utxos_invalid"));
    }

    #[test]
    fn load_wallet_db_to_state_missing_path_fails_without_creating_file() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-wallet-missing-{}.db", uniq));
        let path = p.to_string_lossy().to_string();

        let err = load_wallet_db_to_state(&path).unwrap_err();
        assert_eq!(err, "wallet_not_found");
        assert!(!std::path::Path::new(&path).exists());
    }

    #[test]
    fn load_wallet_db_to_state_missing_path_cleans_stale_stage_files() {
        let mut dir = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        dir.push(format!("duta-wallet-stage-clean-{}", uniq));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("restore.db");
        let staged = dir.join("restore.db.importing-123");
        let staged_journal = dir.join("restore.db.importing-123-journal");
        std::fs::write(&staged, b"stage").unwrap();
        std::fs::write(&staged_journal, b"journal").unwrap();

        let err = load_wallet_db_to_state(&path.to_string_lossy()).unwrap_err();
        assert_eq!(err, "wallet_not_found");
        assert!(!staged.exists());
        assert!(!staged_journal.exists());
    }

    #[test]
    fn load_wallet_db_to_state_rejects_unknown_primary_address_metadata() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-wallet-corrupt-primary-{}.db", uniq));
        let path = p.to_string_lossy().to_string();

        let db = WalletDb::create_new(&path, "strong-pass-123", &[8u8; 32], 1).unwrap();
        let signing_key = SigningKey::from_bytes(&[8u8; 32]);
        let pubkey_hex = hex::encode(signing_key.verifying_key().to_bytes());
        let mut ent = [0u8; 32];
        ent.copy_from_slice(&[8u8; 32]);
        let addr = pkh_to_address_for_network(
            Network::Mainnet,
            &pkh_from_pubkey(signing_key.verifying_key().as_bytes()),
        );
        db.insert_key_with_meta_atomic(
            &addr,
            &pubkey_hex,
            &ent,
            "strong-pass-123",
            Some(1),
            Some(addr.as_str()),
            None,
        )
        .unwrap();
        drop(db);

        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('primary_address', ?1)",
            rusqlite::params!["dut34eb9f0d0d9e0ec7fbf78e6bfc7277f10f01aaf8"],
        )
        .unwrap();
        drop(conn);

        let err = load_wallet_db_to_state(&path).unwrap_err();
        assert_eq!(err, "wallet_primary_address_unknown");
    }

    #[test]
    fn load_wallet_db_to_state_rejects_future_wallet_schema_versions() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-wallet-schema-too-new-{}.db", uniq));
        let path = p.to_string_lossy().to_string();

        let _db = WalletDb::create_new(&path, "strong-pass-123", &[5u8; 32], 1).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('schema_version', ?1)",
            rusqlite::params![WALLET_DB_SCHEMA_VERSION + 1],
        )
        .unwrap();
        drop(conn);

        let err = load_wallet_db_to_state(&path).unwrap_err();
        assert_eq!(
            err,
            format!(
                "wallet_schema_too_new: have={} supported={}",
                WALLET_DB_SCHEMA_VERSION + 1,
                WALLET_DB_SCHEMA_VERSION
            )
        );
    }

    #[test]
    fn load_wallet_db_to_state_restores_pending_and_reserved_runtime_state() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-wallet-runtime-state-{}.db", uniq));
        let path = p.to_string_lossy().to_string();

        let db = WalletDb::create_new(&path, "strong-pass-123", &[9u8; 32], 1).unwrap();
        let signing_key = SigningKey::from_bytes(&[9u8; 32]);
        let pubkey_hex = hex::encode(signing_key.verifying_key().to_bytes());
        let mut ent = [0u8; 32];
        ent.copy_from_slice(&[9u8; 32]);
        let addr = pkh_to_address_for_network(
            Network::Mainnet,
            &pkh_from_pubkey(signing_key.verifying_key().as_bytes()),
        );
        db.insert_key_with_meta_atomic(
            &addr,
            &pubkey_hex,
            &ent,
            "strong-pass-123",
            Some(1),
            Some(addr.as_str()),
            None,
        )
        .unwrap();

        let utxos = vec![Utxo {
            txid: "aa".repeat(32),
            vout: 0,
            value: 4_600_000_000,
            address: addr.clone(),
            height: 70,
            coinbase: true,
        }];
        let pending = vec![PendingTx {
            txid: "bb".repeat(32),
            category: "send".to_string(),
            amount: 100_000_001,
            fee: 10_000,
            change: 4_499_989_999,
            timestamp: 1_700_000_000,
            details: vec![],
            spent_inputs: vec![PendingInput {
                txid: "aa".repeat(32),
                vout: 0,
            }],
        }];
        let reserved = vec![ReservedInput {
            txid: "aa".repeat(32),
            vout: 0,
            timestamp: 1_700_000_001,
        }];
        db.update_full_state(&utxos, 77, &pending, &reserved).unwrap();
        drop(db);

        let ws = load_wallet_db_to_state(&path).unwrap();
        assert_eq!(ws.last_sync_height, 77);
        assert_eq!(ws.utxos.len(), 1);
        assert_eq!(ws.pending_txs.len(), 1);
        assert_eq!(ws.pending_txs[0].txid, "bb".repeat(32));
        assert_eq!(ws.pending_txs[0].spent_inputs.len(), 1);
        assert_eq!(ws.pending_txs[0].spent_inputs[0].txid, "aa".repeat(32));
        assert_eq!(ws.reserved_inputs.len(), 1);
        assert_eq!(ws.reserved_inputs[0].txid, "aa".repeat(32));
        assert!(ws.keys.is_empty());
        assert!(ws.locked);
        assert!(ws.is_db);
        assert_eq!(ws.primary_address, addr);
    }

    #[test]
    fn wallet_rpc_settings_keeps_release_loopback_policy() {
        let conf = Conf::default();
        let (rpc_addr, daemon_rpc_port, net_s) =
            wallet_rpc_settings(Network::Testnet, &conf).unwrap();
        assert_eq!(rpc_addr, "127.0.0.1:18084");
        assert_eq!(daemon_rpc_port, 18083);
        assert_eq!(net_s, "testnet");
    }

    #[test]
    fn wallet_rpc_settings_ignores_non_loopback_override() {
        let conf = Conf::parse("walletrpcbind=0.0.0.0:18084\ndaemonrpcport=18083\n");
        let (rpc_addr, daemon_rpc_port, _) =
            wallet_rpc_settings(Network::Testnet, &conf).unwrap();
        assert_eq!(rpc_addr, "127.0.0.1:18084");
        assert_eq!(daemon_rpc_port, 18083);
        assert_eq!(
            wallet_rpc_bind_warning(Network::Testnet, &conf),
            Some(
                "ignoring non-loopback wallet rpc bind override '0.0.0.0:18084' and keeping loopback-only policy"
                    .to_string()
            )
        );
    }

    #[test]
    fn wallet_rpc_settings_accepts_custom_loopback_ports() {
        let conf = Conf::parse("walletrpcbind=127.0.0.1:28084\ndaemonrpcport=28083\n");
        let (rpc_addr, daemon_rpc_port, net_s) =
            wallet_rpc_settings(Network::Testnet, &conf).unwrap();
        assert_eq!(rpc_addr, "127.0.0.1:28084");
        assert_eq!(daemon_rpc_port, 28083);
        assert_eq!(net_s, "testnet");
        assert_eq!(wallet_rpc_bind_warning(Network::Testnet, &conf), None);
    }

    #[test]
    fn runtime_config_rejects_unknown_network_name() {
        let conf = Conf::parse("network=bogus\n");
        assert_eq!(
            validate_conf_network_name(&conf).unwrap_err(),
            "invalid_network_name: bogus"
        );
        let ok = Conf::parse("chain=mainnet\n");
        assert!(validate_conf_network_name(&ok).is_ok());
    }

    #[test]
    fn runtime_config_rejects_invalid_wallet_rpc_bind_and_daemon_port() {
        let bad_bind = Conf::parse("walletrpcbind=127.0.0.1:notaport\n");
        assert_eq!(
            validate_conf_wallet_rpc_settings(Network::Mainnet, &bad_bind).unwrap_err(),
            "invalid_wallet_rpc_bind: 127.0.0.1:notaport"
        );
        let bad_port = Conf::parse("daemonrpcport=notaport\n");
        assert_eq!(
            validate_conf_wallet_rpc_settings(Network::Mainnet, &bad_port).unwrap_err(),
            "invalid_daemon_rpc_port: notaport"
        );
    }

    #[test]
    fn load_runtime_conf_returns_default_when_missing() {
        let dir = std::env::temp_dir().join(format!(
            "dutawalletd-load-runtime-conf-missing-{}",
            std::process::id()
        ));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("missing.conf");
        let conf = load_runtime_conf(path.to_str().unwrap(), false).unwrap();
        assert!(conf.get_last("network").is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(unix)]
    fn load_runtime_conf_fails_when_existing_file_is_unreadable() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!(
            "dutawalletd-load-runtime-conf-unreadable-{}",
            std::process::id()
        ));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("duta.conf");
        {
            let mut f = fs::File::create(&path).unwrap();
            writeln!(f, "network=testnet").unwrap();
        }
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o000);
        fs::set_permissions(&path, perms).unwrap();
        let err = load_runtime_conf(path.to_str().unwrap(), false).unwrap_err();
        assert!(err.contains("config_read_failed"));
        let mut cleanup_perms = fs::metadata(&path).unwrap().permissions();
        cleanup_perms.set_mode(0o644);
        let _ = fs::set_permissions(&path, cleanup_perms);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_runtime_conf_fails_when_explicit_path_is_missing() {
        let dir = std::env::temp_dir().join(format!(
            "dutawalletd-load-runtime-conf-required-{}",
            std::process::id()
        ));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("missing-required.conf");
        let err = load_runtime_conf(path.to_str().unwrap(), true).unwrap_err();
        assert!(err.contains("config_read_failed"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn pid_file_guard_removes_matching_pid_on_drop() {
        let mut dir = std::env::temp_dir();
        dir.push(format!("duta-wallet-pid-guard-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let pid_path = dir.join("dutawalletd.pid");
        std::fs::write(&pid_path, format!("{}\n", std::process::id())).unwrap();
        {
            let _guard = PidFileGuard::new(dir.to_str().unwrap(), "dutawalletd.pid");
            assert_eq!(
                read_pid_file(pid_path.to_str().unwrap()).unwrap(),
                Some(std::process::id())
            );
        }
        assert!(!pid_path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pid_file_guard_preserves_foreign_pid_file() {
        let mut dir = std::env::temp_dir();
        dir.push(format!("duta-wallet-pid-guard-foreign-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let pid_path = dir.join("dutawalletd.pid");
        std::fs::write(&pid_path, "999999\n").unwrap();
        {
            let _guard = PidFileGuard::new(dir.to_str().unwrap(), "dutawalletd.pid");
            assert_eq!(read_pid_file(pid_path.to_str().unwrap()).unwrap(), Some(999999));
        }
        assert!(pid_path.exists());
        let _ = std::fs::remove_file(&pid_path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn remove_pid_file_if_matches_cleans_only_current_pid() {
        let mut dir = std::env::temp_dir();
        dir.push(format!("duta-wallet-pid-clean-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let pid_path = dir.join("dutawalletd.pid");

        std::fs::write(&pid_path, format!("{}\n", std::process::id())).unwrap();
        remove_pid_file_if_matches(pid_path.to_str().unwrap(), std::process::id());
        assert!(!pid_path.exists());

        std::fs::write(&pid_path, "999999\n").unwrap();
        remove_pid_file_if_matches(pid_path.to_str().unwrap(), std::process::id());
        assert!(pid_path.exists());

        let _ = std::fs::remove_file(&pid_path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn pid_matches_wallet_process_rejects_foreign_live_pid() {
        let mut child = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        assert!(pid_is_alive(pid));
        assert!(!pid_matches_wallet_process(pid));
        let _ = child.kill();
        let _ = child.wait();
    }

    #[cfg(unix)]
    #[test]
    fn prepare_wallet_runtime_files_fails_closed_on_readonly_datadir() {
        use std::os::unix::fs::PermissionsExt;

        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "duta-wallet-readonly-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        let err = match prepare_wallet_runtime_files(dir.to_str().unwrap()) {
            Ok(_) => panic!("readonly datadir should fail"),
            Err(err) => err,
        };
        assert!(err.contains("wallet_write_pid_failed") || err.contains("wallet_log_init_failed"));

        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stop_subcommand_is_parsed_for_wallet_daemon() {
        let args = Args::parse_from(["dutawalletd", "stop"]);
        assert!(matches!(args.command, Some(Cmd::Stop)));
        assert!(!args.daemon);
    }

    #[test]
    fn status_subcommand_is_parsed_for_wallet_daemon() {
        let args = Args::parse_from(["dutawalletd", "status"]);
        assert!(matches!(args.command, Some(Cmd::Status)));
        assert!(!args.daemon);
    }

    #[test]
    fn wallet_status_treats_reachable_rpc_without_pid_file_as_running() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut dir = std::env::temp_dir();
        dir.push(format!("duta-wallet-status-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rc = super::wallet_daemon_status(dir.to_str().unwrap(), &addr.to_string()).unwrap();
        assert_eq!(rc, 1);
        drop(listener);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wallet_status_accepts_real_health_endpoint_without_pid_file() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);
                let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 31\r\nConnection: close\r\n\r\n{\"ok\":true,\"wallet_open\":false}";
                let _ = stream.write_all(resp);
            }
        });
        let mut dir = std::env::temp_dir();
        dir.push(format!("duta-wallet-status-health-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let rc = super::wallet_daemon_status(dir.to_str().unwrap(), &addr.to_string()).unwrap();
        assert_eq!(rc, 0);
        let _ = handle.join();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
