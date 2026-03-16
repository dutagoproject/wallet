use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use rusqlite::{params, Connection};
use serde_json;
use zeroize::Zeroize;

pub(crate) const WALLET_DB_SCHEMA_VERSION: i64 = 1;
const MIN_PASSPHRASE_LEN: usize = 12;
const WALLET_KDF_MEMORY_KIB: u32 = 64 * 1024;
const WALLET_KDF_TIME_COST: u32 = 3;
const WALLET_KDF_PARALLELISM: u32 = 1;

#[derive(Debug)]
pub(crate) struct WalletDb {
    conn: Connection,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct WalletMeta {
    pub(crate) schema_version: i64,
    pub(crate) salt: Vec<u8>,
    pub(crate) seed_nonce: Vec<u8>,
    pub(crate) seed_ct: Vec<u8>,
    pub(crate) next_index: i64,
}

#[derive(Debug)]
pub(crate) struct WalletKeyRow {
    pub(crate) addr: String,
    pub(crate) pubkey_hex: String,
    pub(crate) sk_nonce: Vec<u8>,
    pub(crate) sk_ct: Vec<u8>,
}

fn kdf_key_from_pass(passphrase: &str, salt: &[u8]) -> Result<[u8; 32], String> {
    let params = Params::new(
        WALLET_KDF_MEMORY_KIB,
        WALLET_KDF_TIME_COST,
        WALLET_KDF_PARALLELISM,
        Some(32),
    )
    .map_err(|e| format!("argon2_params_invalid: {e}"))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut out = [0u8; 32];
    argon2
        .hash_password_into(passphrase.as_bytes(), salt, &mut out)
        .map_err(|e| format!("argon2_failed: {e}"))?;
    Ok(out)
}

fn encrypt_bytes(key32: &[u8; 32], plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>), String> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key32));
    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| "encrypt_failed".to_string())?;
    Ok((nonce.to_vec(), ct))
}

fn decrypt_bytes(key32: &[u8; 32], nonce: &[u8], ct: &[u8]) -> Result<Vec<u8>, String> {
    if nonce.len() != 12 {
        return Err("nonce_len_invalid".to_string());
    }
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key32));
    cipher
        .decrypt(Nonce::from_slice(nonce), ct)
        .map_err(|_| "decrypt_failed".to_string())
}

fn configure_connection(conn: &Connection) -> Result<(), String> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| format!("db_init_failed: {e}"))?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(|e| format!("db_init_failed: {e}"))?;
    conn.pragma_update(None, "busy_timeout", 5000i64)
        .map_err(|e| format!("db_init_failed: {e}"))?;
    conn.pragma_update(None, "foreign_keys", 1i64)
        .map_err(|e| format!("db_init_failed: {e}"))?;
    Ok(())
}

impl WalletDb {
    pub(crate) fn create_new(
        path: &str,
        passphrase: &str,
        seed: &[u8],
        next_index: i64,
    ) -> Result<Self, String> {
        if passphrase.trim().is_empty() {
            return Err("missing_passphrase".to_string());
        }
        if passphrase.trim().len() < MIN_PASSPHRASE_LEN {
            return Err("passphrase_too_short".to_string());
        }
        let mut conn = Connection::open(path).map_err(|e| format!("db_open_failed: {e}"))?;
        configure_connection(&conn)?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS keys (
              addr TEXT PRIMARY KEY,
              pubkey_hex TEXT NOT NULL,
              sk_nonce BLOB NOT NULL,
              sk_ct BLOB NOT NULL
            );
            "#,
        )
        .map_err(|e| format!("db_init_failed: {e}"))?;

        // Random salt for KDF
        let mut salt = [0u8; 16];
        OsRng.fill_bytes(&mut salt);
        let key32 = kdf_key_from_pass(passphrase, &salt)?;

        let (seed_nonce, seed_ct) = encrypt_bytes(&key32, seed)?;

        let tx = conn
            .transaction()
            .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        tx.execute("DELETE FROM meta", []).ok();
        tx.execute("DELETE FROM keys", []).ok();

        tx.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('schema_version', ?1)",
            params![WALLET_DB_SCHEMA_VERSION],
        )
        .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('salt', ?1)",
            params![salt.to_vec()],
        )
        .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('seed_nonce', ?1)",
            params![seed_nonce],
        )
        .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('seed_ct', ?1)",
            params![seed_ct],
        )
        .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('next_index', ?1)",
            params![next_index],
        )
        .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('utxos_json', ?1)",
            params![b"[]".to_vec()],
        )
        .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('last_sync_height', ?1)",
            params![0i64],
        )
        .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES('primary_address', ?1)",
            params![""],
        )
        .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("db_meta_write_failed: {e}"))?;

        Ok(Self { conn })
    }

    pub(crate) fn open(path: &str) -> Result<Self, String> {
        let conn = Connection::open(path).map_err(|e| format!("db_open_failed: {e}"))?;
        configure_connection(&conn)?;
        Ok(Self { conn })
    }

    pub(crate) fn read_meta(&self) -> Result<WalletMeta, String> {
        let schema_version: i64 = self
            .conn
            .query_row("SELECT v FROM meta WHERE k='schema_version'", [], |r| {
                r.get(0)
            })
            .map_err(|e| format!("db_meta_read_failed: {e}"))?;
        let salt: Vec<u8> = self
            .conn
            .query_row("SELECT v FROM meta WHERE k='salt'", [], |r| r.get(0))
            .map_err(|e| format!("db_meta_read_failed: {e}"))?;
        let seed_nonce: Vec<u8> = self
            .conn
            .query_row("SELECT v FROM meta WHERE k='seed_nonce'", [], |r| r.get(0))
            .map_err(|e| format!("db_meta_read_failed: {e}"))?;
        let seed_ct: Vec<u8> = self
            .conn
            .query_row("SELECT v FROM meta WHERE k='seed_ct'", [], |r| r.get(0))
            .map_err(|e| format!("db_meta_read_failed: {e}"))?;
        let next_index: i64 = self
            .conn
            .query_row("SELECT v FROM meta WHERE k='next_index'", [], |r| r.get(0))
            .map_err(|e| format!("db_meta_read_failed: {e}"))?;
        Ok(WalletMeta {
            schema_version,
            salt,
            seed_nonce,
            seed_ct,
            next_index,
        })
    }

    pub(crate) fn list_keys(&self) -> Result<Vec<WalletKeyRow>, String> {
        let mut stmt = self
            .conn
            .prepare("SELECT addr, pubkey_hex, sk_nonce, sk_ct FROM keys ORDER BY addr ASC")
            .map_err(|e| format!("db_keys_read_failed: {e}"))?;
        let rows = stmt
            .query_map([], |r| {
                Ok(WalletKeyRow {
                    addr: r.get(0)?,
                    pubkey_hex: r.get(1)?,
                    sk_nonce: r.get(2)?,
                    sk_ct: r.get(3)?,
                })
            })
            .map_err(|e| format!("db_keys_read_failed: {e}"))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| format!("db_keys_read_failed: {e}"))?);
        }
        Ok(out)
    }

    pub(crate) fn insert_key_encrypted(
        &self,
        addr: &str,
        pubkey_hex: &str,
        sk_bytes32: &[u8; 32],
        passphrase: &str,
    ) -> Result<(), String> {
        let meta = self.read_meta()?;
        let key32 = kdf_key_from_pass(passphrase, &meta.salt)?;
        let (nonce, ct) = encrypt_bytes(&key32, sk_bytes32)?;
        self.conn
            .execute(
                "INSERT OR REPLACE INTO keys(addr,pubkey_hex,sk_nonce,sk_ct) VALUES(?1,?2,?3,?4)",
                params![addr, pubkey_hex, nonce, ct],
            )
            .map_err(|e| format!("db_keys_write_failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn insert_key_with_meta_atomic(
        &self,
        addr: &str,
        pubkey_hex: &str,
        sk_bytes32: &[u8; 32],
        passphrase: &str,
        next_index: Option<i64>,
        primary_address: Option<&str>,
        mnemonic_entropy: Option<&[u8]>,
    ) -> Result<(), String> {
        let meta = self.read_meta()?;
        let key32 = kdf_key_from_pass(passphrase, &meta.salt)?;
        let (nonce, ct) = encrypt_bytes(&key32, sk_bytes32)?;

        self.conn
            .execute("BEGIN IMMEDIATE TRANSACTION", [])
            .map_err(|e| format!("db_tx_begin_failed: {e}"))?;

        let result: Result<(), String> = (|| {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO keys(addr,pubkey_hex,sk_nonce,sk_ct) VALUES(?1,?2,?3,?4)",
                    params![addr, pubkey_hex, nonce, ct],
                )
                .map_err(|e| format!("db_keys_write_failed: {e}"))?;

            if let Some(v) = next_index {
                self.conn
                    .execute(
                        "INSERT OR REPLACE INTO meta(k,v) VALUES('next_index', ?1)",
                        params![v],
                    )
                    .map_err(|e| format!("db_meta_write_failed: {e}"))?;
            }

            if let Some(v) = primary_address {
                self.conn
                    .execute(
                        "INSERT OR REPLACE INTO meta(k,v) VALUES('primary_address', ?1)",
                        params![v],
                    )
                    .map_err(|e| format!("db_meta_write_failed: {e}"))?;
            }

            if let Some(v) = mnemonic_entropy {
                self.conn
                    .execute(
                        "INSERT OR REPLACE INTO meta(k,v) VALUES('mnemonic_entropy', ?1)",
                        params![v],
                    )
                    .map_err(|e| format!("db_meta_write_failed: {e}"))?;
            }

            self.conn
                .execute("COMMIT", [])
                .map_err(|e| format!("db_tx_commit_failed: {e}"))?;
            Ok(())
        })();

        if let Err(e) = result {
            let _ = self.conn.execute("ROLLBACK", []);
            return Err(e);
        }
        Ok(())
    }

    pub(crate) fn decrypt_all_keys(
        &self,
        passphrase: &str,
    ) -> Result<Vec<(String, String, [u8; 32])>, String> {
        let meta = self.read_meta()?;
        let mut key32 = kdf_key_from_pass(passphrase, &meta.salt)?;
        let mut legacy_empty_key32 = kdf_key_from_pass("", &meta.salt)?;
        let rows = self.list_keys()?;
        let mut out = Vec::new();

        for r in rows {
            let sk_plain = match decrypt_bytes(&key32, &r.sk_nonce, &r.sk_ct) {
                Ok(v) => v,
                Err(_) => {
                    let recovered = decrypt_bytes(&legacy_empty_key32, &r.sk_nonce, &r.sk_ct)
                        .map_err(|_| "decrypt_failed".to_string())?;
                    if recovered.len() != 32 {
                        return Err("sk_len_invalid".to_string());
                    }
                    let mut ent = [0u8; 32];
                    ent.copy_from_slice(&recovered);
                    self.insert_key_encrypted(&r.addr, &r.pubkey_hex, &ent, passphrase)?;
                    out.push((r.addr, r.pubkey_hex, ent));
                    continue;
                }
            };
            if sk_plain.len() != 32 {
                return Err("sk_len_invalid".to_string());
            }
            let mut ent = [0u8; 32];
            ent.copy_from_slice(&sk_plain);
            out.push((r.addr, r.pubkey_hex, ent));
        }
        key32.zeroize();
        legacy_empty_key32.zeroize();
        Ok(out)
    }

    #[allow(dead_code)]
    pub(crate) fn decrypt_seed(&self, passphrase: &str) -> Result<Vec<u8>, String> {
        let meta = self.read_meta()?;
        let mut key32 = kdf_key_from_pass(passphrase, &meta.salt)?;
        let seed = decrypt_bytes(&key32, &meta.seed_nonce, &meta.seed_ct)?;
        key32.zeroize();
        Ok(seed)
    }

    pub(crate) fn change_passphrase(
        &self,
        old_passphrase: &str,
        new_passphrase: &str,
    ) -> Result<(), String> {
        if new_passphrase.trim().is_empty() {
            return Err("new_passphrase_empty".to_string());
        }
        if new_passphrase.trim().len() < MIN_PASSPHRASE_LEN {
            return Err("new_passphrase_too_short".to_string());
        }

        let meta = self.read_meta()?;
        let mut old_key32 = kdf_key_from_pass(old_passphrase, &meta.salt)?;
        let mut seed = decrypt_bytes(&old_key32, &meta.seed_nonce, &meta.seed_ct)
            .map_err(|_| "old_passphrase_invalid".to_string())?;

        let rows = self.list_keys()?;
        let mut plain_keys: Vec<(String, String, [u8; 32])> = Vec::with_capacity(rows.len());
        for r in rows {
            let mut sk_plain = decrypt_bytes(&old_key32, &r.sk_nonce, &r.sk_ct)
                .map_err(|_| "old_passphrase_invalid".to_string())?;
            if sk_plain.len() != 32 {
                seed.zeroize();
                return Err("sk_len_invalid".to_string());
            }
            let mut ent = [0u8; 32];
            ent.copy_from_slice(&sk_plain);
            sk_plain.zeroize();
            plain_keys.push((r.addr, r.pubkey_hex, ent));
        }

        let mut new_salt = [0u8; 16];
        OsRng.fill_bytes(&mut new_salt);
        let mut new_key32 = kdf_key_from_pass(new_passphrase, &new_salt)?;
        let (seed_nonce, seed_ct) = encrypt_bytes(&new_key32, &seed)?;

        self.conn
            .execute("BEGIN IMMEDIATE TRANSACTION", [])
            .map_err(|e| format!("db_tx_begin_failed: {e}"))?;

        let result: Result<(), String> = (|| {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO meta(k,v) VALUES('salt', ?1)",
                    params![new_salt.to_vec()],
                )
                .map_err(|e| format!("db_meta_write_failed: {e}"))?;
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO meta(k,v) VALUES('seed_nonce', ?1)",
                    params![seed_nonce],
                )
                .map_err(|e| format!("db_meta_write_failed: {e}"))?;
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO meta(k,v) VALUES('seed_ct', ?1)",
                    params![seed_ct],
                )
                .map_err(|e| format!("db_meta_write_failed: {e}"))?;

            for (addr, pubkey_hex, sk_bytes32) in plain_keys.iter() {
                let (nonce, ct) = encrypt_bytes(&new_key32, sk_bytes32)?;
                self.conn
                    .execute(
                        "INSERT OR REPLACE INTO keys(addr,pubkey_hex,sk_nonce,sk_ct) VALUES(?1,?2,?3,?4)",
                        params![addr, pubkey_hex, nonce, ct],
                    )
                    .map_err(|e| format!("db_keys_write_failed: {e}"))?;
            }

            self.conn
                .execute("COMMIT", [])
                .map_err(|e| format!("db_tx_commit_failed: {e}"))?;
            Ok(())
        })();

        if let Err(e) = result {
            let _ = self.conn.execute("ROLLBACK", []);
            seed.zeroize();
            for (_, _, sk_bytes32) in plain_keys.iter_mut() {
                sk_bytes32.zeroize();
            }
            old_key32.zeroize();
            new_key32.zeroize();
            return Err(e);
        }

        seed.zeroize();
        for (_, _, sk_bytes32) in plain_keys.iter_mut() {
            sk_bytes32.zeroize();
        }
        old_key32.zeroize();
        new_key32.zeroize();
        Ok(())
    }

    pub(crate) fn read_next_index(&self) -> Result<i64, String> {
        Ok(self.read_meta()?.next_index)
    }

    pub(crate) fn read_primary_address(&self) -> Result<String, String> {
        match self
            .conn
            .query_row("SELECT v FROM meta WHERE k='primary_address'", [], |r| {
                r.get(0)
            }) {
            Ok(v) => Ok(v),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(String::new()),
            Err(e) => Err(format!("db_meta_read_failed: {e}")),
        }
    }

    pub(crate) fn update_primary_address(&self, primary_address: &str) -> Result<(), String> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO meta(k,v) VALUES('primary_address', ?1)",
                params![primary_address],
            )
            .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn read_last_sync_height(&self) -> Result<i64, String> {
        match self
            .conn
            .query_row("SELECT v FROM meta WHERE k='last_sync_height'", [], |r| {
                r.get(0)
            }) {
            Ok(v) => Ok(v),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(format!("db_meta_read_failed: {e}")),
        }
    }

    pub(crate) fn update_sync_state(
        &self,
        utxos: &[crate::Utxo],
        last_sync_height: i64,
    ) -> Result<(), String> {
        let body = serde_json::to_vec(utxos).map_err(|e| format!("db_utxos_encode_failed: {e}"))?;
        self.conn
            .execute("BEGIN IMMEDIATE TRANSACTION", [])
            .map_err(|e| format!("db_tx_begin_failed: {e}"))?;

        let result: Result<(), String> = (|| {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO meta(k,v) VALUES('utxos_json', ?1)",
                    params![body],
                )
                .map_err(|e| format!("db_meta_write_failed: {e}"))?;
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO meta(k,v) VALUES('last_sync_height', ?1)",
                    params![last_sync_height],
                )
                .map_err(|e| format!("db_meta_write_failed: {e}"))?;
            self.conn
                .execute("COMMIT", [])
                .map_err(|e| format!("db_tx_commit_failed: {e}"))?;
            Ok(())
        })();

        if let Err(e) = result {
            let _ = self.conn.execute("ROLLBACK", []);
            return Err(e);
        }
        Ok(())
    }

    pub(crate) fn read_mnemonic_entropy(&self) -> Result<Option<Vec<u8>>, String> {
        match self
            .conn
            .query_row("SELECT v FROM meta WHERE k='mnemonic_entropy'", [], |r| {
                r.get(0)
            }) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(format!("db_meta_read_failed: {e}")),
        }
    }

    pub(crate) fn read_utxos(&self) -> Result<Vec<crate::Utxo>, String> {
        let raw: Vec<u8> =
            match self
                .conn
                .query_row("SELECT v FROM meta WHERE k='utxos_json'", [], |r| r.get(0))
            {
                Ok(v) => v,
                Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(Vec::new()),
                Err(e) => return Err(format!("db_meta_read_failed: {e}")),
            };
        serde_json::from_slice(&raw).map_err(|e| format!("db_utxos_invalid: {e}"))
    }

    pub(crate) fn update_utxos(&self, utxos: &[crate::Utxo]) -> Result<(), String> {
        let body = serde_json::to_vec(utxos).map_err(|e| format!("db_utxos_encode_failed: {e}"))?;
        self.conn
            .execute(
                "INSERT OR REPLACE INTO meta(k,v) VALUES('utxos_json', ?1)",
                params![body],
            )
            .map_err(|e| format!("db_meta_write_failed: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::WalletDb;
    use hex;

    fn temp_wallet_path(tag: &str) -> String {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-walletdb-test-{}-{}.db", tag, uniq));
        p.to_string_lossy().to_string()
    }

    #[test]
    fn create_new_rejects_short_passphrase() {
        let path = temp_wallet_path("short-create");
        let err = WalletDb::create_new(&path, "short-pass", &[1u8; 32], 1).unwrap_err();
        assert_eq!(err, "passphrase_too_short");
    }

    #[test]
    fn change_passphrase_rejects_short_new_passphrase() {
        let path = temp_wallet_path("short-change");
        let db = WalletDb::create_new(&path, "strong-pass-123", &[2u8; 32], 1).unwrap();
        let err = db
            .change_passphrase("strong-pass-123", "short-pass")
            .unwrap_err();
        assert_eq!(err, "new_passphrase_too_short");
    }

    #[test]
    fn change_passphrase_preserves_seed_and_rejects_old_passphrase() {
        let path = temp_wallet_path("preserve-seed");
        let db = WalletDb::create_new(&path, "strong-pass-123", &[7u8; 32], 1).unwrap();
        let sk = [9u8; 32];
        db.insert_key_encrypted("dut1test", &hex::encode([3u8; 32]), &sk, "strong-pass-123")
            .unwrap();

        db.change_passphrase("strong-pass-123", "new-strong-pass-456")
            .unwrap();

        let err = db.decrypt_seed("strong-pass-123").unwrap_err();
        assert_eq!(err, "decrypt_failed");
        assert_eq!(
            db.decrypt_seed("new-strong-pass-456").unwrap(),
            vec![7u8; 32]
        );

        let keys = db.decrypt_all_keys("new-strong-pass-456").unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].0, "dut1test");
        assert_eq!(keys[0].2, sk);
    }

    #[test]
    fn update_sync_state_writes_utxos_and_height_together() {
        let path = temp_wallet_path("sync-state");
        let db = WalletDb::create_new(&path, "strong-pass-123", &[5u8; 32], 1).unwrap();
        let utxos = vec![crate::Utxo {
            value: 11,
            height: 22,
            coinbase: false,
            address: "dut1sync".to_string(),
            txid: "ab".repeat(32),
            vout: 1,
        }];

        db.update_sync_state(&utxos, 44).unwrap();

        let reopened = WalletDb::open(&path).unwrap();
        assert_eq!(reopened.read_last_sync_height().unwrap(), 44);
        let got = reopened.read_utxos().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].txid, "ab".repeat(32));
        assert_eq!(got[0].value, 11);
    }
}
