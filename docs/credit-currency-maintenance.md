# Invitations credit currency maintenance

This release supports both the existing console API 1 / legacy quota and API 2 /
USD credit. Install the compatible image under the joint maintenance procedure;
freeze all financial writers before changing either database. This command does
not stop services, create backups, or modify NewAPI.

Use the existing secret-file entrypoint to supply `INVITATIONS_DATABASE_URL` and
read-only `NEWAPI_DATABASE_URL`. Set `SETTLEMENT_MODE=read_only`. Plan and verify
are read-only; apply and restore require the backup SHA256 and an exact plan hash.
Keep output files private (umask 077).

```sh
invitations credit-migrate plan --migration-id ID > invitations-credit-plan.json
# Read .sha256 from the resulting JSON and retain the file with the joint backup.
# Apply NewAPI's same-ID currency plan first, while all services remain frozen.
invitations credit-migrate apply --maintenance --confirm-sha256 PLAN_SHA256 --backup-sha256 BACKUP_SHA256 < invitations-credit-plan.json
invitations credit-migrate verify --confirm-sha256 PLAN_SHA256 < invitations-credit-plan.json
```

The unit is an integer quota. Each monetary projection/configuration is converted
by exact 3/40, rounding half away from zero at one quota. For example, an old
1330-credit wallet corresponds to 665000000 quota; this becomes 49875000 quota,
or $99.75. Request totals are summed from individually converted item entitlements.
Original event payloads/hashes, paid payout/reversal values, operation identities,
and NewAPI's legacy `kkai_invitation_rebate_balance_marks` remain unchanged.

The transaction checks the complete monetary before image, operation/payout
counts and IDs, a drained balance outbox, and the same NewAPI migration receipt.
It locks the financial tables and temporarily disables only the immutable request
item amount trigger for the denomination conversion. That trigger must start
enabled and is re-enabled before commit; a transaction rollback also restores it.
Before/after images and the backup hash are retained in `system_config` under
`kkai_usd_credit_migration`. Runtime startup checks both services' migration IDs
and NewAPI plan hash. Existing financial preflight still validates original
ledger identities and uses converted projections when checking legacy payouts.

Do the normal schema/startup preflight and browser acceptance before releasing
maintenance. An old browser epoch is rejected. API 2 authentication requires an
active browser session proved by NewAPI `/api/user/self`; a PAT does not grant
Invitations administrator access. An API 2 failure never falls back to cookies.

After cutover, new rebate source events require schema 2, the USD epoch and the
same migration ID. Old source events only replay an identical existing receipt;
an unknown old event stays frozen for audit. Subscription/redemption scanners
recognize pre-cutover orders already present as duplicates and refuse automatic
backfilling of missing old orders. They do not complete or retry payment orders.
Original balance-operation JSON/hashes are preserved, with source denomination
sent separately in `X-KKAI-Quota-Epoch`; old reversals retain the original credit
identity and validate the actual wallet delta.

Rollback is permitted only while all writers remain frozen and the exact after
image still matches. Restore Invitations first while NewAPI's USD receipt still
exists, then restore NewAPI and the coordinated database/service bundle:

```sh
invitations credit-migrate restore --maintenance --confirm-sha256 PLAN_SHA256 --backup-sha256 BACKUP_SHA256 < invitations-credit-plan.json
```

The command refuses drift or a different receipt instead of guessing. Once new
financial activity occurs, use the joint backup/recovery procedure; this is not
an online rollback mechanism. The converted projection is used for current
entitlements, while immutable historical payouts keep their original unit.
