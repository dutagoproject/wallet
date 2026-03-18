# dutawalletd Operator Guide

This guide covers how to run `dutawalletd` in a clean and predictable way.

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

Treat wallet RPC as private operator surface. Do not expose it directly to the public internet.

## Denomination rules

- `DUTA` is the display unit
- `dut` is the base unit
- all on-chain and stored values remain integer `dut`
- RPC fields like `amount`, `balance`, `fee`, and `change` are display-layer values in `DUTA`
- raw values are exposed as `*_dut`
- operators and automation should prefer `*_dut` for accounting and exact comparisons

Examples:

- `amount = "0.00000001"` means `amount_dut = 1`
- `fee = "0.0001"` means `fee_dut = 10000`

## Operating rules

- keep wallet RPC bound to loopback unless you have a strong reason not to
- keep wallet and node on the same network
- keep mainnet and testnet data directories separate
- back up wallet database files before major upgrades or migrations
- protect passphrases outside shell history and public scripts

## What to back up

Back up:

- wallet database files
- any deployment-specific configuration
- any operational notes required to identify which wallet belongs to which environment

If you run more than one wallet, keep the naming and folder layout consistent so restores are easy to follow.

## Common mistakes

### Wallet RPC is pointed at the wrong network

If the daemon is running on testnet but the wallet is still using mainnet ports, open and send operations will fail or look inconsistent.

### Wallet RPC is exposed publicly

This daemon is not meant to be a public internet-facing wallet API. Put a controlled service layer in front of it if remote access is required.

### Mainnet and testnet share one directory

Keep network-specific wallet storage separate. Mixing environments makes recovery and troubleshooting harder than it needs to be.
