# dutawalletd 0.0.1-beta

This is the first public beta release of the DUTA wallet daemon.
It is for people who want to run the wallet service themselves and keep control of the storage and RPC surface.

## Highlights

- Encrypted wallet storage
- Wallet RPC for controlled operator use
- Address management, signing, and send flow

## Included files

- `dutawalletd`

## Who should use this

Use this package if you want to:

- run the wallet daemon on your own host
- manage encrypted wallet storage
- unlock wallets through wallet RPC
- send transactions through a controlled node setup

## Quick start

1. Extract the archive.
2. Create or import a wallet.
3. Store the recovery material offline.
4. Start `dutawalletd`.
5. Unlock only when needed.

## Security notes

- Keep wallet RPC private.
- Do not run the wallet daemon as a public internet service.
- Use a strong passphrase and store recovery material offline.
- Pair the wallet with a trusted DUTA node.

## Checksums and archives

Choose the archive that matches your platform:

- Linux x86_64
- Windows x86_64

If a checksum file is attached to the release, verify it before using the binaries.

## Notes for this beta

This release is for operators and early integrators. GUI packaging is outside the scope of this release line.

For operator guidance, CLI notes, and build instructions, see the repository documentation.
