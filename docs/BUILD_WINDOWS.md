# Build dutawalletd on Windows

This guide is for producing the Windows wallet daemon binary for packaging and deployment.

## Requirements

- Rust toolchain with Cargo
- Visual Studio C++ build tools or an equivalent MSVC-capable Rust environment

## Build

From the repository root:

```powershell
cargo build --release
```

## Output

The release binary is written to:

```text
target\release\dutawalletd.exe
```

## Recommended verification

Check the available flags before packaging or deploying:

```powershell
.\target\release\dutawalletd.exe --help
```

## Next step

For ports, datadir layout, daemon mode, and wallet RPC usage, continue with:

- [OPERATOR_GUIDE.md](./OPERATOR_GUIDE.md)
- [CLI_GUIDE.md](./CLI_GUIDE.md)
