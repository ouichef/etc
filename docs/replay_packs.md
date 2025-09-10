# Record-and-Replay with "Packs"

Implement record-and-replay with “replay packs”.

## What to capture per item

Immutable JSON blob:

- `raw_payload_normalized`
- `mapped_payload` (post mapping, pre rules)
- `changed_keys`
- `batch_ctx {now, ruleset_version, flags_version}`
- `flags_snapshot {key=>bool}`
- `resolver_snapshot` (only the keys your rules read: {brands:{id=>...}, strains:{name=>id}, tags:{name=>id}})
- `autotagger_cache` (outputs used, if any)
- `rules_order` (names + priorities)
- `outcome` {status, violations?, fired_rules}

Store to S3 (or DB JSONB) keyed by (source_id, external_id, ingest_id).

## Step-through runner

Expose an iterator that replays rule by rule.

```ruby
class Replay
  Step = Struct.new(:rule, :applied, :reason, :patch, :conflicts, :state_after, keyword_init: true)

  def initialize(pack)
    @ctx   = build_ctx(pack)
    @rules = build_rules(pack['rules_order'])
  end

  def steps
    state = {}
    @rules.map do |rule|
      applied = rule.applies?(@ctx)
      patch   = applied ? rule.changes(@ctx) : {}
      conflicts = state.keys & patch.keys
      state = merge(state, patch)
      Step.new(rule: rule.name,
               applied:, reason: rule.respond_to?(:reason) ? rule.reason : nil,
               patch:, conflicts:, state_after: state.dup)
    end
  end

  private

  def build_ctx(p)
    ctx = Integration::MenuSync::ItemContext.new(
      payload: p['mapped_payload'],
      menu_item: nil,
      changed_keys: p['changed_keys'],
      now: Time.at(p['batch_ctx']['now'])
    )
    snap = p['flags_snapshot']
    ctx.define_singleton_method(:flag?)    { |k| snap.fetch(k) }
    brands = p['resolver_snapshot']['brands'] || {}
    strains= p['resolver_snapshot']['strains'] || {}
    tags   = p['resolver_snapshot']['tags'] || {}
    ctx.define_singleton_method(:brand_lookup)  { |id| brands[id] }
    ctx.define_singleton_method(:strain_lookup) { |n|  strains[n] }
    ctx.define_singleton_method(:tag_lookup)    { |n|  tags[n] }
    ctx
  end

  def build_rules(order)
    # Instantiate concrete rules in the frozen order recorded in the pack
    order.map { |r| RuleFactory.build(r['name'], priority: r['priority']) }
  end

  def merge(a,b) = a.merge(b) # same policy used in production
end
```

### Producing the replay pack

At transform time:

```ruby
pack = {
  raw_payload_normalized: normalized_raw,
  mapped_payload: mapped,
  changed_keys: changed_keys,
  batch_ctx: { now: batch_now.to_i, ruleset_version: RULESET_VERSION, flags_version: flags.version },
  flags_snapshot: flags.values,
  resolver_snapshot: { brands: brand_map, strains: strain_map, tags: tag_map },
  autotagger_cache: autotagger_cache, # optional
  rules_order: ruleset.rules.map { |r| {name: r.name, priority: r.priority} },
  outcome: { status:, violations:, fired_rules: }
}
ArtifactStore.write!(key: key_for(item), body: pack)
```

### Support UI and CS tooling

- CLI: `etl:replay --key <k> --step` prints each step: rule, applied, patch, conflicts, state digest.
- Web UI: show a diff table per step: state_before → patch → state_after. Filter by field.
- Datadog link: store `trace_id` in the pack; add a button “Open APM trace”.

Determinism checklist

- Freeze `now` in `batch_ctx`.
- Persist outputs of non-deterministic services (autotagger). Use them during replay.
- Pin ruleset code by version. Keep a git tag → class map. If code differs, show “version drift”.
- Error triage via replay
- Load failing pack → iterate steps → first step with violations or unexpected conflict is your root cause.
- Capture reason from rules when possible: set `@reason` in `applies?`.

### Storage schema versioning

Add `pack_version`. Migrate forward by loader when fields evolve.
This gives you:

- immutable artifacts
- deterministic local replay
- rule-by-rule stepping
- CS visibility without turning the pipeline into a state machine.

---

## Artifact Store

In this context “artifact store” just means “a place you write immutable replay packs.” S3 is the simplest choice:

- Immutable: write-once, never mutate.
- Addressable: key pattern like
`s3://etl-artifacts/<env>/<source_id>/<external_id>/<ingest_id>.json`
- Queryable: you can point Athena/Glue at it for ad-hoc analysis.

Cheap + durable: you keep years of history at low cost.

### Implementation sketch

```ruby
require 'aws-sdk-s3'

class ArtifactStore
  BUCKET = ENV.fetch("ETL_ARTIFACT_BUCKET")

  def self.write!(key:, body:)
    s3.put_object(
      bucket: BUCKET,
      key: key,
      body: JSON.dump(body),
      content_type: 'application/json'
    )
  end

  def self.read(key:)
    obj = s3.get_object(bucket: BUCKET, key: key)
    JSON.parse(obj.body.read)
  end

  def self.key_for(item)
    [
      ENV['DD_ENV'] || 'dev',
      item[:source_id],
      item[:payload]['external_id'],
      item[:ingest_id]
    ].join('/') + ".json"
  end

  def self.s3
    @s3 ||= Aws::S3::Client.new
  end
end
```

or

```ruby
# lib/ingest/observe.rb
require "json"
require "zlib"
require "stringio"

module Ingest
  class ArtifactWriter
    def initialize(s3:, bucket:)
      @s3, @bucket = s3, bucket
    end

    def write!(pack_key:, record:)
      io = StringIO.new
      gz = Zlib::GzipWriter.new(io)
      gz.write(JSON.dump(record))
      gz.close
      @s3.put_object(bucket: @bucket, key: pack_key, body: io.string, content_type: "application/json", content_encoding: "gzip")
    end
  end
end
```

### Ops tips

- Versioning: enable S3 bucket versioning for safety.
- Retention: set lifecycle rules (e.g. 90 days in S3 Standard → Glacier).
- Access: CS and engineers can fetch a JSON pack via a simple tool.
- Trace link: include `trace_id` in the filename or JSON so you can jump from Datadog to the artifact.

### Retention Strategy

Don’t keep only errors. Keep all packs briefly, keep errors and rejects long-term, and sample successes.
Version inside the pack, plus optional S3 object versioning.

- `0–7 days`: keep all outcomes (noop, updated, created, deleted, rejected). Useful for hotfix replay and drift checks.
- `8–90 days`: keep errors/rejected 100%, successes at a small sample (e.g., 1–5%) to preserve baselines.
- `>90 days`: archive errors to Glacier; expire sampled successes.

#### Use S3 lifecycle rules per prefix

`s3://etl-artifacts/<env>/<date=YYYY-MM-DD>/<status=updated|noop|rejected|...>/`

Partitioning by date and status also makes Athena queries cheap.

Versioning
Add a pack header. Keep S3 object versioning ON for safety, but rely on an explicit `pack_version` for loaders.

Example pack skeleton:

```ruby
{
  "pack_version": 3,
  "produced_at": 1725062400,
  "env": "prod",
  "app_version": "1.14.2",
  "git_sha": "abc1234",
  "ruleset_version": "2025-08-15",
  "flags_version": "f9b2c1e0a4d3",
  "payload_schema_version": "v6",
  "source_id": "menu-crawler",
  "external_id": "X123",
  "ingest_id": "ing-789",
  "status": "rejected",          // or updated|created|noop|deleted
  "fired_rules": ["variants_v2", "brand_resolve"],
  "raw_payload_normalized": {...},
  "mapped_payload": {...},
  "changed_keys": ["price", "variants"],
  "resolver_snapshot": {"brands": {...}, "strains": {...}, "tags": {...}},
  "autotagger_cache": {...}      // if used
}
```

#### Guidelines

- Bump `pack_version` on any incompatible change to the pack shape. Loader switches behavior by version.
- Keep `ruleset_version`, `flags_version`, and `payload_schema_version` separate. They explain why behavior differed even when `pack_version` is constant.

Storage layout

- Use status/date prefixes for retention and ops:
`s3://etl-artifacts/prod/date=2025-08-31/status=updated/menu-crawler/X123/ing-789.json`
- Optionally include `ruleset_version` in the key for quick filtering:
`.../ruleset=2025-08-15/...`

Sampling

- Control with Flipper:
`etl.sample_success_packs_percent → 0.01..0.05`
- Always write packs when status in `{rejected, deleted}` or when `error_taxonomy != nil`.

Compression + security

- Gzip JSON (content-encoding: gzip) to cut cost.
- Bucket policies + SSE-S3 or SSE-KMS.
- Deny overwrite; only allow PUT if key absent to keep immutability.

Minimal writer

```ruby
status = outcome[:status]
prefix = "env=#{env}/date=#{batch_date}/status=#{status}"
key = "#{prefix}/#{source_id}/#{external_id}/#{ingest_id}.json.gz"

ArtifactStore.write!(key:, body: Zlib.gzip(JSON.dump(pack)), content_encoding: 'gzip')
```

This gives you cheap replay for support, a baseline for drift detection, and clean evolution via explicit `pack_version`.
