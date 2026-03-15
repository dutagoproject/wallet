# Build dutawalletd on Windows

This guide is for producing the Windows wallet daemon binary without using PowerShell.

## Requirements

- Rust toolchain with Cargo
- Visual Studio C++ build tools or an equivalent MSVC-capable Rust environment

## Build

From the repository root:

```bat
scripts\build-windows.cmd
```

## Output

The release binary is written to:

```text
target\release\dutawalletd.exe
```

## Recommended verification

Check the available flags before packaging or deploying:

```bat
target\release\dutawalletd.exe --help
```

## Run

```bat
scripts\run-walletd.cmd .\data\mainnet
```

## Next step

For ports, datadir layout, daemon mode, and wallet RPC usage, continue with:

- [OPERATOR_GUIDE.md](./OPERATOR_GUIDE.md)
- [CLI_GUIDE.md](./CLI_GUIDE.md)
