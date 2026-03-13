# dutawalletd

`dutawalletd` is the DUTA wallet daemon repository.

It contains encrypted wallet storage, wallet RPC, mnemonic and recovery handling, passphrase management, and wallet-side send and sync logic.

Current release line: `0.0.1-beta`

Website: https://dutago.xyz

## Repository scope

This repository includes:

- encrypted wallet storage
- wallet RPC endpoints
- mnemonic import and export flows
- passphrase unlock and change flows
- address derivation
- wallet-side sync, balance, and send operations

This repository does not include:

- chain consensus rules
- P2P networking
- public stratum mining
- desktop GUI packaging

## Main binary

- `dutawalletd`

## Release position

This repo is for operators and integrators who need the wallet daemon itself.

It is not the node repository and it is not the public stratum repository.

## Documentation

- Linux build: [docs/BUILD_LINUX.md](./docs/BUILD_LINUX.md)
- Windows build: [docs/BUILD_WINDOWS.md](./docs/BUILD_WINDOWS.md)
- Operator guide: [docs/OPERATOR_GUIDE.md](./docs/OPERATOR_GUIDE.md)
- CLI guide: [docs/CLI_GUIDE.md](./docs/CLI_GUIDE.md)
