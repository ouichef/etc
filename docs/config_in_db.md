# Configuration in DB

Store configs in DB, version them, validate before publish, and flip an active pointer on a schedule. Batch runs read the pinned “active” version once per run and freeze it in BatchContext.

Tables

```sql
-- All authored configs
CREATE TABLE ingest_configs (
  id                BIGSERIAL PRIMARY KEY,
  version_label     TEXT NOT NULL,                -- "2025.09.06.a"
  env               TEXT NOT NULL,                -- "prod","staging"
  status            TEXT NOT NULL,                -- "draft","published","active","retired"
  doc               JSONB NOT NULL,               -- the full YAML-as-JSON
  created_by        TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  validated_at      TIMESTAMPTZ,
  validation_report JSONB
);

-- One row per env that points to the currently active config (atomic flip)
CREATE TABLE ingest_config_active (
  env               TEXT PRIMARY KEY,
  active_config_id  BIGINT REFERENCES ingest_configs(id),
  previous_config_id BIGINT REFERENCES ingest_configs(id),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by        TEXT
);

-- Optional: schedule activations during a window
CREATE TABLE ingest_config_schedules (
  id                BIGSERIAL PRIMARY KEY,
  env               TEXT NOT NULL,
  config_id         BIGINT NOT NULL REFERENCES ingest_configs(id),
  activate_at       TIMESTAMPTZ NOT NULL,         -- e.g. 02:00 local
  created_by        TEXT NOT NULL
);
```

Loader (typed, frozen per batch)

```ruby
# lib/config/store.rb

module ConfigStore
  module_function

  def active(env:)
    row = DB[:ingest_config_active].where(env:).first or raise "no active config for #{env}"
    cfg = DB[:ingest_configs].where(id: row[:active_config_id]).first or raise "dangling active pointer"
    [Cfg::Root.new(from_json(cfg[:doc])), cfg[:id]] # (typed_cfg, config_id)
  end

  def from_json(h)
    # reuse the Dry::Struct loader you already have (previous message)
    {
      "ruleset"       => {"version" => h["ruleset"]["version"]},
      "flags"         => h["flags"],
      "filtering"     => h["filtering"],
      "mapping"       => h["mapping"],
      "publishability"=> h["publishability"],
      "deletes"       => h["deletes"],
      "references"    => h["references"],
      "observability" => h["observability"]
    }
  end
end
```

# batch entry

```ruby
typed_cfg, cfg_id = ConfigStore.active(env: ENV.fetch("APP_ENV","prod"))
batch = Ingest::BatchContext.new(
  now: Time.now.utc,
  flag_snapshot: Flags.snapshot(typed_cfg.flags),
  repos: Repos.bundle,
  preloads: Preloads.load_all(typed_cfg, repos: Repos.bundle)
)
```

# Include cfg_id in artifacts

Validation pipeline before publish

```ruby
# lib/config/validator.rb

module ConfigValidator
  module_function
  def call(doc)
    errors = []
    # 1) schema check via Dry::Schema or Contracts
    res = Cfg::Root.new(doc) rescue ($!.message)
    errors << res if res.is_a?(String)

    # 2) references: ensure mode == "lookup_only" and actions ∈ {reject,ignore}
    # 3) publishability: keys exist in contract
    # 4) build ruleset with this cfg, run DAG validation
    rs = Ingest::RuleSet.compile(rules: RULES, version: doc.dig("ruleset","version"))
    # 5) dry-run a small golden batch to ensure determinism
    errors
  end
end
```

Promotion flow (UI + jobs)

 1. Author: Create ingest_configs row with status='draft', attach doc.
 2. Validate: Run ConfigValidator.call(doc). Store validation_report. Set status='published' if empty.
 3. Schedule: Insert into ingest_config_schedules(env, config_id, activate_at).
 4. Cron (nightly): Promote scheduled rows whose activate_at <= now.

```ruby
# jobs/activate_configs.rb

DB.transaction do
  DB[:ingest_config_schedules].where{ activate_at <= Sequel::CURRENT_TIMESTAMP }.for_update.each do |s|
    DB[:ingest_config_active].insert_conflict(target: :env,
      update: {
        active_config_id:   s[:config_id],
        previous_config_id: Sequel[:ingest_config_active][:active_config_id],
        updated_at: Sequel::CURRENT_TIMESTAMP,
        updated_by: "cron"
      }
    ).insert(env: s[:env], active_config_id: s[:config_id], updated_by: "cron")

    DB[:ingest_configs].where(id: s[:config_id]).update(status: "active")
    DB[:ingest_config_schedules].where(id: s[:id]).delete
  end
end
```

Manual activate/rollback endpoints flip the pointer atomically:

```shell
# POST /config/activate {env, config_id}

# POST /config/rollback {env} -> sets active = previous_config_id
```

Runtime guarantees
 • No auto drift: The pipeline only reads ingest_config_active at batch start. No mid-batch changes.
 • Versioned artifacts: Write config_id and ruleset_version to pack artifacts. You can trace any behavior to a config.
 • Rollback: O(1) pointer flip. No doc rewrites.

Caching
 • Optional in-memory cache with TTL or DB updated_at etag. Always include config_id in BatchContext to avoid confusion.
 • If you run many small webhooks, cache the active pair (cfg, id) for N seconds to reduce DB reads.

Strong opinions
 • Keep rules in code and validated in CI. Keep policy in DB. This avoids dynamic rule loading risk.
 • Treat config docs as immutable. New change = new row. Never update an active row’s doc.
 • Enforce “quiet hours” by only running the cron inside an allowed window and by env. Add per-source windows if needed.
 • Add a canary field if useful: allow ingest_config_active to carry optional percentage or source_whitelist to gate by source before full flip. If not used now, keep pointer simple.

UI checklist
 • Editor with JSON/YAML form.
 • Live validator with the same backend ConfigValidator.
 • Diff viewer between draft and active.
 • Schedule picker with timezone.
 • One-click activate and rollback (guarded by RBAC).
 • Audit log from pointer flips and validations.

This gives you safe, scheduled rollout with instant rollback and per-batch freezing.
