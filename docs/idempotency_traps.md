# Idempotency Traps in the Converter

- Non-deterministic inputs
- `Time.now`, `Date.today`, or `“expires_at = now + 7d”`.
- Random IDs or UUIDs generated per run.
- `Autotagger` that’s stochastic or time-aware.
- Order sensitivity
- Merging price rules or variants with “last wins” without stable sort keys.
- Iterating over Hashes without sorting keys.
- Incremental updates
- `inventory += x` or counters instead of set-to-value.
- Appending to arrays/JSON without de-duplication.
- Regenerating derived data without keys
- Rebuilding variants creates new rows each run because you insert by position, not by a deterministic key.
- Price rules compared by full object equality instead of a natural key (type+value+weight_key).
- Defaults that write every time
- `“If missing, set license_type = recreational”` runs even when already set.
- `Autotags` merged into tags without set semantics.
- Side-effects outside the DB
- Calling external services that mutate or rate-limit differently on retries.

Race conditions:

- Two workers run the transform. First write affects second read, producing a different merge.

## How to make it idempotent

Make the transformer pure and deterministic.

- No clocks
- no RNG
- no I/O
- Inject any “now” once at pipeline start.
- Use stable natural keys.
- `Variants`: derive key from weight or from `(external_id || "#{grams}g")`. Upsert on key.
- Price rules: upsert on (`adjustment_type`, `adjustment_value`, `expires_at`) or a hashed “fingerprint”.
- Sort everything.
- Variants sorted by `weight` then `external_id`.
- Tags sorted and de-duplicated.
- Use set semantics, not append.
- Compute the full desired set and replace, or diff with adds/removes by key.
- Avoid `+=`; do SET `value = desired_value`.
- Guard writes.
- Only write fields in `changed_keys` for updates.
- Drop writes that would null out unresolved FKs.
- Separate `create` vs `update` policies.
- Create can fill defaults; update should not re-apply defaults.
- Upsert everywhere you rebuild: `INSERT ... ON CONFLICT (key) DO UPDATE SET ...`
- Delete-and-recreate only if keys are stable and you compare before acting.
- Freeze rounding rules.
- One rounding strategy, applied once, with unit tests.

Versioning/ETag.

- Compare current DB snapshot to computed desired snapshot. If equal, no-op.

Tests to enforce:

- Run `transform+load` twice → DB state identical.
- Property: permutation of input order doesn’t change output.
- Clock injection test: same now yields same rows.
- Golden cases for variant/price-rule upserts keyed by natural keys.
