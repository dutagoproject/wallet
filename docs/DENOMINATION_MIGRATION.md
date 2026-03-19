# Wallet Denomination Migration

This wallet runtime now treats `dut` as the raw/base unit and `DUTA` as the display unit.

## Runtime rules

- On-chain and persisted wallet values remain integer `dut`.
- Display-facing fields such as `amount`, `balance`, `fee`, `change`, `spendable_balance`, and `immature_balance` are formatted in `DUTA`.
- Display-facing `DUTA` values are rendered with fixed `8` decimal places.
- Exact raw values are exposed as `*_dut`.
- Unit metadata is exposed as:
- `unit = "DUTA"`
- `display_unit = "DUTA"`
- `base_unit = "dut"`
- `decimals = 8`

## Direct migration impact

- Existing wallet files remain usable because stored amounts were already integer values.
- Existing automation that treated `amount` or `balance` as raw integers must migrate to `amount_dut` or `balance_dut`.
- Decimal user input such as `0.00000001` now maps cleanly to `1 dut`.
- Automation and accounting should prefer `*_dut` for exact comparisons and totals.

## Critical wallet surfaces

- `getbalance` returns display `balance`, `spendable`, `reserved`, `pending_send`, and `pending_change`, plus matching raw `*_dut` fields.
- `getwalletinfo` returns display `balance`/`spendable_balance` and raw `balance_dut`/`spendable_balance_dut`.
- `listunspent` returns display `amount` and raw `amount_dut`.
- `send`, `sendmany`, `gettransaction`, and `listtransactions` return display `amount`/`fee`/`change` fields and matching raw `*_dut` fields.
