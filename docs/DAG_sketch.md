# ETL DAG

The DAG is not something you compute dynamically in the hot ETL path. It’s a design-time artifact:

- Primary use = CI/CD validation
- Build the DAG from rules’ metadata.
- Check for cycles → fail build.
- Check for write–write conflicts → fail build.
- Check all before/after refs point to real rules.
- Emit a fingerprint → compare to last known → alert if the graph changed.

Secondary uses:

- documentation + auditing
- Render to DOT/PNG so engineers and support can see “who writes what, who depends on what.”
- Publish weekly to Slack/Confluence. Diff highlights new/removed edges.
- Keep historical DAGs as a record of pipeline evolution.
Runtime

Flow

- Author adds or modifies a rule (changes reads, writes, or before/after).
- CI loads all rules, builds DAG, runs checks.
- If valid → merge.

Optional:

- weekly job builds DAG, diffs against last week’s, posts “3 edges added, 1 removed” with rendered graph.

## Build the DAG from metadata

Edges come from:

- Data deps: `A → B` if `A.writes` a∩d `B.reads` is non-empty.
- Explicit order: `A → B` if `B.after` includes `A` or `A.before` includes `B`.

```ruby
class RuleDAG
  Node = Struct.new(:name, :reads, :writes, :before, :after)

  def self.from_rules(rules)
    nodes = rules.map { |r| Node.new(r.name, r.metadata[:reads], r.metadata[:writes], r.metadata[:before], r.metadata[:after]) }
    edges = []
    nodes.combination(2).each do |a, b|
      edges << [a.name, b.name, :data] if (a.writes & b.reads).any?
      edges << [b.name, a.name, :data] if (b.writes & a.reads).any?
    end
    nodes.each do |n|
      n.before.each { |b| edges << [n.name, b, :explicit] }
      n.after.each  { |a| edges << [a, n.name, :explicit] }
    end
    { nodes:, edges: edges.uniq }
  end
end

# Toposort validation:
def validate_acyclic!(dag)

# Kahn’s algorithm; raise if cycle

end
# Render to DOT:
def to_dot(dag)
  <<~DOT
  digraph Rules {
    rankdir=LR;
    #{dag[:nodes].map { |n| %("#{n.name}" [shape=box]) }.join("\n")}
    #{dag[:edges].map { |a,b,t| %("#{a}" -> "#{b}" [label="#{t}"]) }.join("\n")}
  }
  DOT
end
```

or

```ruby
# lib/ingest/ruleset.rb
require "tsort"

module Ingest
  class RuleSet
    include TSort
    attr_reader :version

    def initialize(rules:, version:)
      @rules   = rules.index_by(&:key).freeze
      @version = version
    end

    def evaluate(ctx)
      changes = {}
      fired   = []

      tsort_each do |key|
        r = @rules.fetch(key)
        res = r.call(ctx)
        next unless res[:fired]

        conflict = (r.writes & changes.keys)
        raise "write conflict: #{r.key} -> #{conflict.inspect}" if conflict.any?

        changes.merge!(res[:changes])
        fired << r.key
        ctx = ctx.new(changed_keys: (ctx.changed_keys + r.writes).uniq.freeze)
      end

      [changes.freeze, fired.freeze]
    end

    # TSort hooks
    def tsort_each_node(&blk) = @rules.each_key(&blk)
    def tsort_each_child(key, &blk)
      @rules[key].before.each(&blk)   # edges: key -> before
      # also respect priority as a soft tie-breaker
    end

    # CI validation helpers
    def graph_edges
      @rules.transform_values { |r| r.before }
    end

    def detect_write_conflicts
      pairs = @rules.values.combination(2)
      pairs.filter_map do |a, b|
        overlap = (a.writes & b.writes)
        next if overlap.empty?
        [a.key, b.key, overlap]
      end
    end
  end
end
```

## Weekly diff

Compute a stable fingerprint of the DAG:

```ruby
def fingerprint(dag)
  payload = {
    nodes: dag[:nodes].map(&:name).sort,
    edges: dag[:edges].map { |a,b,t| [a,b,t] }.sort
  }
  Digest::SHA256.hexdigest[payload.to_json](0,12)
end
```

Job:

- Load current manifest
- build DAG
- validate
- compute fingerprint.
- Compare to last week’s fingerprint stored in S3/DB.

If changed, post Slack with:

- added/removed rules
- added/removed edges
- priority changes
- new required flags

## Guardrails in CI

- Ensure every enabled rule declares metadata.
- Ensure rules list only contains known classes.
- Validate DAG acyclicity and no write-write conflicts unless allowed.

```ruby
write_sets = rules.map { |r| [r.name, r.metadata[:writes]] }.to_h
conflicts = write_sets.to_a.combination(2).select { |(*,a),(*,b)| (a & b).any? }
raise "write conflicts: #{conflicts.inspect}" unless conflicts.empty?
```

## Observability

- Emit the manifest version and DAG fingerprint:
- `span.set_tag('ruleset.version', cfg.version)`
- `span.set_tag('ruleset.dag_fp', fingerprint(dag))`

## Summary

- Keep logic in Ruby classes.
- Keep activation, ordering, priorities, params, and flags in a manifest.
- Each rule exposes data-only metadata. Build a DAG from that.
- Validate, diff, and publish the DAG regularly.

You only need before/after if there’s an ordering dependency that the data‐flow edges (writes → reads) don’t already capture.
Think of it as two layers of metadata:

## Data-flow edges (default)

If `Rule A` writes `categories` and `Rule B` reads `categories`, you don’t need to say “A before B.”

The DAG builder will add `A → B` automatically.
That covers `80%` of ordering.

## Explicit ordering (escape hatch)

Some rules don’t have a clean read/write dependency but still need sequencing.

Example:

- `AutotagRule` writes `tags` based on `category_names`,
- `CategorizationRule` consumes `category_names` but doesn’t “read” `tags`.

You want Autotag first so tags reflect pre-categorization names.
That’s when you add:

```ruby
def metadata
  { before: ['CategorizationRule'], reads: ['category_names'], writes: ['tags'] }
end
```

## Minimal metadata contract

Each rule should always declare:

- reads: [..] (keys it inspects)
- writes: [..] (keys it mutates)

Optionally:

- before: [..] (must run before these rules)
- after: [..] (must run after these rules)
- flags: (feature flags it requires)

## Validation

- Build DAG with both sources: `writes→reads edges + explicit before/after`.
- Run topological sort.
- Fail CI if cycles.
- Detect “phantom” before/after (points to non-existent rule).

Example:
Minimal YAML (no explicit ordering)
Rules declare reads/writes in Ruby. YAML only turns rules on and sets priority/params.

```yaml
# config/rulesets/menu_item_update.yml

version: "2025-08-31"
ruleset: "menu_item_update"
rules:

- class: "AutotagRule"          # reads category_names, writes tags
    enabled: true
    priority: 10
- class: "CategorizationRule"   # reads category_names, writes categories
    enabled: true
    priority: 20
- class: "VariantsRebuildRule"  # reads categories, variants, price..., writes variants, price
    enabled: true
    priority: 50
- class: "BrandIdRule"          # reads brand_id, writes brand_id
    enabled: true
    priority: 70
```

Ruby rules expose metadata:

```ruby
class AutotagRule
  def name = "autotag"
  def metadata = { reads: %w[category_names name], writes: %w[tags], flags: %w[etl.autotag] }

# applies?/changes

end

class CategorizationRule
  def name = "categorization"
  def metadata = { reads: %w[category_names name brand_name], writes: %w[categories] }
end

class VariantsRebuildRule
  def name = "variants_rebuild"
  def metadata = { reads: %w[categories variants price direct_sale_price integrator_metadata],
                   writes: %w[variants price] }
end
```

DAG builder infers edges from writes→reads. No before/after needed.

### When inference isn’t enough

Say you want Autotag to run before Categorization even though there’s no data edge.

Two options:

- Put it in the rule (preferred if it’s intrinsic)

```ruby
class AutotagRule
  def metadata
    { reads: %w[category_names name], writes: %w[tags], before: %w[categorization] }
  end
end
```

- Override in YAML (good for environment-specific tweaks)
rules:

```yaml
- class: "AutotagRule"
    enabled: true
    priority: 10
    overrides:
      before: ["categorization"]
```

Loader merges overrides into the rule’s metadata before building the DAG.

## Validation (CI)

- Build DAG from writes→reads + before/after.
- Toposort. Fail on cycles.
- Ensure no unintended write–write conflicts:
  - conflicts = rules.combination(2).select { |a,b| (a.metadata[:writes] & b.metadata[:writes]).any? }
  - raise if conflicts.any?

This gives you:

- YAML controls activation, order, params.
- Rules define reads/writes once.
- before/after only where inference can’t express intent.
