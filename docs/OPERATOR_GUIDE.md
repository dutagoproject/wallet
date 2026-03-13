# dutawalletd Operator Guide

This guide covers the public operator-facing behavior of the DUTA wallet daemon.

## Default ports

Mainnet:

- wallet RPC: `127.0.0.1:19084`
- daemon RPC dependency: `127.0.0.1:19083`

Testnet:

- wallet RPC: `127.0.0.1:18084`
- daemon RPC dependency: `127.0.0.1:18083`

## Network selection

Use:

- mainnet: no extra flag
- testnet: `--testnet`
- stagenet: `--stagenet`

Keep each network on its own data directory.

## Start the daemon

Mainnet foreground example:

```bash
./dutawalletd --datadir /srv/duta/wallet/mainnet
```

Testnet foreground example:

```bash
./dutawalletd --testnet --datadir /srv/duta/wallet/testnet
```

Background mode:

```bash
./dutawalletd --daemon --datadir /srv/duta/wallet/mainnet
```

When `--daemon` is used, the process writes:

- `debug.log`
- `error.log`
- `dutawalletd.pid`

inside the selected data directory.

## RPC surface

Common wallet RPC endpoints include:

- `POST /open`
- `POST /createwallet`
- `POST /import_mnemonic`
- `GET /balance`
- `GET /address`
- `POST /send`
- `POST /unlock`
- `POST /lock`
- `POST /change_passphrase`

Treat the wallet RPC as private operator surface. Do not expose it directly to the public internet.

## Operational rules

- keep wallet RPC bound to loopback unless you have a strong reason not to
- keep wallet and node on the same network
- keep mainnet and testnet data directories separate
- back up wallet database files before major upgrades or migrations
- protect passphrases outside shell history and public scripts

## Backup expectations

Back up:

- wallet database files
- any deployment-specific configuration
- any operational notes required to identify which wallet belongs to which environment

If you run multiple wallets, keep naming and folder layout consistent so restores are obvious.

## Common mistakes

### Wallet RPC points to the wrong network

If the daemon is running on testnet but the wallet is still pointed at mainnet ports, wallet open and send operations will fail or return misleading results.

### Wallet RPC is exposed publicly

This daemon is not meant to be a public internet-facing wallet API. Put a controlled service layer in front of it if remote access is required.

### Mainnet and testnet wallets share one directory

Keep network-specific wallet storage separate. Mixing environments makes recovery and operator troubleshooting much harder.
