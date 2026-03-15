# Build dutawalletd on Linux

This guide is for producing the Linux wallet daemon binary for packaging and deployment.

## Requirements

- Rust toolchain with Cargo
- standard Linux build tools required by Rust crates with native dependencies

## Build

From the repository root:

```bash
./scripts/build-linux.sh
```

## Output

The release binary is written to:

```text
target/release/dutawalletd
```

## Recommended verification

Check the available flags before packaging or deploying:

```bash
./target/release/dutawalletd --help
```

## Run

```bash
./scripts/run-walletd.sh ./data/mainnet
```

## Next step

For ports, datadir layout, daemon mode, and wallet RPC usage, continue with:

- [OPERATOR_GUIDE.md](./OPERATOR_GUIDE.md)
- [CLI_GUIDE.md](./CLI_GUIDE.md)
