# MVP ETL Pipeline

## Tables, Models, Factories

- [ ] Menu
- [ ] MenuItem
- [ ] MenuItem::Variant

- [ ] Category
- [ ] CategoryBinding

- [ ] Strain

- [ ] Brand

- [ ] Tag
- [ ] TagBinding

## ETL Services

- [ ] IngestContext
- [ ] Rule
- [ ] Abstract Rule
- [ ] Concrete Rule
- [ ] RuleSet
- [ ] AssociationCache (Lookup Maps + PreloadRefs + Repos)
- [ ] Config- Rule Entry
- [ ] Config- Rule Set
- [ ] Mapper
- [ ] Contracts
- [ ] Flag Snapshots

## Pipeline Overview

### Extraction

Attach `ingest_id` and `ingested_at` once. No clocks downstream.

### Filter Collection

- Dedupe by (`integration_id`, `external_id`) with a stable canonicalizer.

- Validate with `dry-schema` first, then `dry-validation` with coercions.

- Drop unknown keys early.

### Map → MenuItemPayload

- Pure mappers.
- No DB.
- Normalize units, rounding, enums.
- Compute Diff (prev, incoming).

### Transform (Ruleset)

- Preload reference lookups across the batch: `brands`, `strains`, `tags`, `products`.
- `Context = {payload, changed_keys, menu_item?, lookups, now}`
- Rules are pure.
- Emit {`changes`, `fired_rules`, `desired_variants`}.
- Sort and de-dupe all sets. Emit desired_snapshot structs to compare in persistence.

### Persist

- Re-validate transformed payload with a strict contract.
  - If invalid → 1.a `:noop`
- Open one transaction per item.
    1.
        a) `:noop`: if diff unchanged, Record a `:noop` event with `fired_rules`.
        b) `Update`: row exists for (`integration_id`, `external_id`).
        - Upsert variants by natural key `variant_key` (e.g., `weight_key` or `external_id`).
        - Delete-orphans by key diff only.
    2. Create: row absent.
        - Insert parent.
        - Bulk insert child rows using the same keys.
    3. Delete: only via explicit tombstone signal, not inference.
        - Soft-delete sync record (`status: :deleted`, `deleted_at`, `delete_reason`, `source_id`).

### Outcomes and errors

Return:

```ruby
{
    status: :noop|:updated|:created|:deleted|:rejected,
    violations:,
    fired_rules:,
    timings:
}
```

Classify errors:
    - `schema_reject`
    - `validation_reject`
    - `referential_miss`
    - `db_conflict`
    - `unknown`

Policy details to lock in:

- Preload only reference data. Never fetch inside rules.
- Inject a single `:now` into transform for any time-based fields.
- Stable sort everywhere:
  - `variants` by (`weight`, `external_id`)
  - `rules` by `priority`
  - `tags` by `name`
- Never write unresolved `FKs` on update; drop those writes.
- External services (`autotag`) behind a `flag`.
- Cache within batch.
- Hash-based no-op: payload_fingerprint on raw normalized payload, plus desired_snapshot_hash for derived state.
