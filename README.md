# Project: SAP B1 TO SAP ECC Queries

Description: SAP HANA Queries created to migrate SAP B1 balances to SAP ECC.

- (P&L) Profit and Loss account balances
- (BS) Balance Sheet account balances​
- (Assets) Fixed Assets balances​
- (AR-OI) Customer open items balances​
- (AP-OI) Vendor open items balances​

---

## Commands

Prepare

- `npm run clean`: Delete installed files
- `pnpm install`: Install dependencies
- `pnpm up --latest --interactive`: Update dependencies

Format

- `pnpm run format`: Run prettier

## Dependencies

- "prettier"
  - "prettier-plugin-sh"
  - "prettier-plugin-sql"
