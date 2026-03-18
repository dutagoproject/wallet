# Install dutawalletd from Binary Bundles

This guide is for operators who want to run the wallet binaries from a release bundle instead of building from source.

It covers:

- `dutawalletd`
- `duta-wallet-cli`

## What to download

For each final release, download the bundle that matches your platform:

- Windows: `duta-wallet-<version>-windows-x86_64`
- Linux: `duta-wallet-<version>-linux-x86_64`

Also keep:

- `manifest.json`
- `sha256sums.txt`

## Verify the checksums

On Linux:

```bash
sha256sum -c sha256sums.txt
```

On Windows:

```bat
certutil -hashfile dutawalletd.exe SHA256
certutil -hashfile duta-wallet-cli.exe SHA256
```

Compare the output to `sha256sums.txt`.

## Linux install example

```bash
tar -xzf duta-wallet-1.0.0-linux-x86_64.tar.gz
cd duta-wallet-1.0.0-linux-x86_64
install -m 0755 dutawalletd /usr/local/bin/dutawalletd
install -m 0755 duta-wallet-cli /usr/local/bin/duta-wallet-cli
mkdir -p /root/.duta
chmod 700 /root/.duta
```

## Windows install example

Extract the ZIP archive into a folder you control, for example:

```text
C:\DUTA\wallet
```

Then verify the binaries directly:

```bat
dutawalletd.exe --help
duta-wallet-cli.exe --help
```

## Suggested layout

Linux:

```text
/usr/local/bin/dutawalletd
/usr/local/bin/duta-wallet-cli
/root/.duta
```

Windows:

```text
C:\DUTA\wallet\dutawalletd.exe
C:\DUTA\wallet\duta-wallet-cli.exe
```

## Important notes

- keep wallet RPC private
- keep wallet and node on the same network
- prefer DB-backed wallet files only
- verify denomination-aware automation against `*_dut` fields before production rollout
