# dutawalletd CLI Guide

This guide is for people who want to use the DUTA wallet daemon and CLI without having to guess the flow.

## Binaries in this repo

- `dutawalletd`
  The wallet daemon.
- `duta-wallet-cli`
  A simple command-line client for wallet RPC.

## The basic idea

- `dutawalletd` runs the wallet service
- `duta-wallet-cli` talks to that wallet service

In normal use, start the wallet daemon first, then talk to it from the CLI or your own RPC client.

## Start the wallet daemon

Mainnet, foreground:

```bash
./dutawalletd --datadir /srv/duta/wallet/mainnet
```

Mainnet, background:

```bash
./dutawalletd --daemon --datadir /srv/duta/wallet/mainnet
```

Testnet:

```bash
./dutawalletd --testnet --datadir /srv/duta/wallet/testnet
```

## What to check after startup

Inside the selected data directory:

- `debug.log`
- `error.log`
- `dutawalletd.pid` when using `--daemon`

Mainnet default wallet RPC:

- `127.0.0.1:19084`

Testnet default wallet RPC:

- `127.0.0.1:18084`

## Basic wallet CLI examples

Create a wallet:

```bash
./duta-wallet-cli createwallet --wallet-path /srv/duta/wallet/mainnet/alice.db --passphrase YOUR_PASS
```

Open a wallet:

```bash
./duta-wallet-cli open --wallet-path /srv/duta/wallet/mainnet/alice.db --passphrase YOUR_PASS
```

Get the current address:

```bash
./duta-wallet-cli address
```

Get the current balance:

```bash
./duta-wallet-cli balance
```

Unlock the wallet:

```bash
./duta-wallet-cli unlock --passphrase YOUR_PASS
```

Send funds:

```bash
./duta-wallet-cli send --to DESTINATION_ADDRESS --amount 1.25 --passphrase YOUR_PASS
```

If your wallet RPC uses a non-default port:

```bash
./duta-wallet-cli --rpc 127.0.0.1:19084 balance
```

## Good habits

- keep wallet RPC private
- keep wallet daemon and node on the same network
- keep mainnet and testnet wallets in separate folders
- use DB-backed wallet files only

## Common mistakes

### The daemon is not running

If the CLI returns connection errors, start `dutawalletd` first.

### The wallet is on the wrong network

A mainnet wallet daemon cannot manage a testnet wallet correctly, and the reverse is also true.

### The wallet path keeps changing

Pick one folder layout and stick to it.

Good example:

- `/srv/duta/wallet/mainnet/alice.db`
- `/srv/duta/wallet/testnet/alice.db`

## First run checklist

1. Start `dutawalletd`
2. Confirm the wallet RPC is listening
3. Create or open a wallet
4. Verify `address` and `balance`
5. Back up the wallet database file before moving further
