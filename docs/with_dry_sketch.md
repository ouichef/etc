# ETL With Dry-RB

app/types.rb

```ruby
module Types
  include Dry.Types()

  ItemId      = String
  ExternalId  = String
  SourceId    = String
  Json        = Hash
  TimeInt     = Integer
  Bool        = Types::Bool
  StringList  = Types::Array.of(String)
end
```

Context as Dry::Struct

```ruby
# app/services/etl/menu_item_context.rb

class MenuItemContext < Dry::Struct
  attribute :payload,        Types::Json
  attribute :menu_item,      Types::Any.optional
  attribute :changed_keys,   Types::Array.of(String) | Types::Constant(:all)
  attribute :assigned_category, Types::Any.optional
  attribute :now,            Types::Time

# injected lambdas (no I/O in rules)

  attribute :flag?,          Types::Any
  attribute :brand_lookup,   Types::Any
  attribute :strain_lookup,  Types::Any
  attribute :tag_lookup,     Types::Any
end
```

Rule base with dry-initializer

```ruby
# app/services/etl/rules/rule.rb

class Rule
  extend Dry::Initializer

  option :name,      Types::String
  option :priority,  Types::Integer, default: proc { 0 }

# override in subclasses

  def metadata = { reads: [], writes: [], flags: [], before: [], after: [] }
  def applies?(_ctx) = true
  def changes(_ctx)  = {}
end
```

Concrete rules

```ruby
# app/services/etl/rules/autotag_rule.rb

class AutotagRule < Rule
  option :max_tags, Types::Integer, default: -> { 5 }

  def metadata = { reads: %w[name category_names], writes: %w[tags], flags: %w[etl.autotag] }

  def applies?(ctx) = ctx.flag?.call('etl.autotag') && (ctx.payload['name'] || ctx.payload['category_names']).present?

  def changes(ctx)
    text = "#{ctx.payload['name']} #{Array(ctx.payload['category_names']).join(' ')}"
    tags = AutoTagger.call(text:)&.tags || []
    { 'tags' => tags.uniq.first(max_tags) }
  end
end

# app/services/etl/rules/categorization_rule.rb

class CategorizationRule < Rule
  def metadata = { reads: %w[category_names name brand_name], writes: %w[categories] }

  def changes(ctx)
    cats = ExternalItemCategorizerService.call(
      category_names: Array(ctx.payload['category_names']).filter(&:presence),
      name: ctx.payload['name'],
      brand_name: ctx.payload['brand_name']
    ).categories
    { 'categories' => cats }
  end
end

# app/services/etl/rules/brand_id_rule.rb

class BrandIdRule < Rule
  def metadata = { reads: %w[brand_id], writes: %w[brand_id] }

  def applies?(ctx) = ctx.payload.key?('brand_id')

  def changes(ctx)
    id = ctx.payload['brand_id']
    ctx.brand_lookup.call(id) ? { 'brand_id' => id } : {}
  end
end

# app/services/etl/rules/variants_rebuild_rule.rb

class VariantsRebuildRule < Rule
  option :each_mapper
  option :weight_mapper

  VARIANT_TRIGGERS = (
    MenuItem::MIGRATED_EACH_VARIANT_ATTRIBUTES[MenuItem.name] +
    %w[categories direct_sale_price price integrator_metadata variants]
  ).uniq.freeze

  def metadata = { reads: %w[categories variants price direct_sale_price integrator_metadata], writes: %w[variants price] }

  def applies?(ctx) = ctx.changed_keys == :all || (Array(ctx.changed_keys) & VARIANT_TRIGGERS).any?

  def changes(ctx)
    weighted = (assigned_category(ctx)&.sold_by_weight?) && ctx.payload['variants'].to_a.any?
    if weighted
      { 'variants' => weight_mapper.call(ctx.payload), 'price' => Price.empty }
    else
      v = each_mapper.call(ctx.payload)
      { 'variants' => [v], 'price' => v.price }
    end
  end

  private

  def assigned_category(ctx)
    ctx.assigned_category || (ctx.menu_item ? DeprecatedRootCategoryService.call(ctx.menu_item) : nil)
  end
end
```

Mappers with dry-initializer

```ruby
# app/services/etl/mappers/each_variant_mapper.rb

class EachVariantMapper
  extend Dry::Initializer

  def call(p)
    MenuItem::Variant.new(
      key: MenuItem::Variant::DEFAULT_EACH_NAME.parameterize,
      price: price_from(p),
      cart_quantity_multiplier: p['cart_quantity_multiplier'].to_f.nonzero?,
      compliance_net_mg: p['compliance_net_mg'],
      compliance_net_precalc: p.fetch('compliance_net_precalc', false),
      external_id: p['external_id'],
      inventory_quantity: p['inventory_quantity']&.clamp(MenuItem::Variant::INVENTORY_QUANTITY_RANGE),
      items_per_pack: p['items_per_pack'],
      integrator_metadata: p['integrator_metadata'],
      ratio: p['ratio']
    )
  end

  private

  def price_from(h)
    amt = h.dig('price','amount').to_f
    cur = Crawler::MenuItemPayload::Converter::CURRENCY_ENUM[h.dig('price','currency')]
    return Price.empty if amt.zero? || cur.nil?
    Price.from_amount(amt, cur)
  end
end

# app/services/etl/mappers/weight_variants_mapper.rb

class WeightVariantsMapper
  extend Dry::Initializer
  MAX = MenuItem::VariantValidator::MAX_VARIANTS
  WE  = Crawler::MenuItemPayload::Converter::WEIGHT_ENUM

  def call(p)
    p['variants'].to_a.filter_map { build_one(_1, p) }.sort_by!(&:weight).first(MAX)
  end

  private

  def build_one(v, parent)
    price = price_from(v); weight = weight_from(v)
    return if price.empty? || weight&.value.to_f.zero?
    MenuItem::Variant.new(
      price:, weight:,
      cart_quantity_multiplier: v['cart_quantity_multiplier'].to_f.nonzero?,
      compliance_net_mg: v['compliance_net_mg'],
      compliance_net_precalc: v.fetch('compliance_net_precalc', false),
      external_id: v['external_id'].presence || parent['external_id'],
      inventory_quantity: v['inventory_quantity']&.clamp(MenuItem::Variant::INVENTORY_QUANTITY_RANGE),
      integrator_metadata: v['integrator_metadata'].presence || parent['integrator_metadata']
    )
  end

  def price_from(h)
    amt = h.dig('price','amount').to_f
    cur = Crawler::MenuItemPayload::Converter::CURRENCY_ENUM[h.dig('price','currency')]
    return Price.empty if amt.zero? || cur.nil?
    Price.from_amount(amt, cur)
  end

  def weight_from(h)
    unit = WE[h.dig('weight','unit')]; val = h.dig('weight','value').to_f
    return unless unit && val.nonzero?
    w = Weight.new(val, unit)
    case w.in_grams.value.to_f
    when 3.5 then Weight.new(0.125, :ounce)
    when 7.0 then Weight.new(0.25, :ounce)
    when 14.0 then Weight.new(0.5, :ounce)
    when 28.0 then Weight.new(1, :ounce)
    else w
    end
  end
end
```

Ruleset config as Dry::Struct

```ruby
# app/services/etl/rules/ruleset_config.rb

class RuleEntry < Dry::Struct
  attribute :class,     Types::String
  attribute :enabled,   Types::Bool
  attribute :priority,  Types::Integer.default(0)
  attribute :params,    Types::Hash.default({}.freeze)
end

class RulesetConfig < Dry::Struct
  attribute :version, Types::String
  attribute :ruleset, Types::String
  attribute :rules,   Types::Array.of(RuleEntry)

  def self.load(path)
    raw = YAML.load_file(path)
    new(raw)
  end

  def instantiate
    active = rules.select(&:enabled)
    list = active.map do |r|
      klass = Object.const_get(r.class)
      inst  = klass.new(**r.params.symbolize_keys.merge(name: r.class, priority: r.priority))
      inst
    end
    [ruleset, version, list]
  end
end
```

RuleSet as value

```ruby
# app/services/etl/rules/rule_set.rb

class RuleSet < Dry::Struct
  attribute :name,     Types::String
  attribute :rules,    Types::Array.of(Rule)
  attribute :conflict, Types::Symbol.default(:last_wins)

  def evaluate(ctx)
    result = {}; fired = []
    rules.sort_by { -_1.priority }.each do |r|
      next unless r.applies?(ctx)
      patch = r.changes(ctx) || {}
      fired << r.name
      result = merge(result, patch)
    end
    [result.freeze, fired.freeze]
  end

  private

  def merge(a,b)
    conflict == :first_wins ? b.merge(a) : a.merge(b)
  end
end
```

Preload output as Dry::Struct

```ruby
# app/services/etl/preload_refs.rb

class LookupMaps < Dry::Struct
  attribute :brands,  Types::Hash.default({}.freeze)
  attribute :strains, Types::Hash.default({}.freeze)
  attribute :tags,    Types::Hash.default({}.freeze)
end

class PreloadRefs
  extend Dry::Initializer

  option :brands_repo
  option :strains_repo
  option :tags_repo

  def call(items)
    brand_ids    = items.filter_map { _1[:payload]['brand_id'].presence }.uniq
    strain_names = items.filter_map { _1[:payload]['strain_name'].presence }.uniq
    tag_names    = items.flat_map { Array(_1[:payload]['tag_names']) }.compact.uniq
    LookupMaps.new(
      brands:  brands_repo.by_ids(brand_ids),
      strains: strains_repo.by_names(strain_names),
      tags:    tags_repo.by_names(tag_names)
    )
  end
end
```

Transformers with dry-initializer

```ruby
# app/services/etl/transformers/create_transformer.rb
# We are using rulesets + rules to transform the payload- the create transformer wraps that ruleset with its needed and specific context.
# The creation transformer should have less 'rules' around changes, we don't have to consider different potentials such as:
# A category change affecting the variant type etc.
class CreateTransformer
  extend Dry::Initializer
  option :ruleset

  def call(payload:, lookups:, flags_snap:, now:)
    ctx = MenuItemContext.new(
      payload:,
      menu_item: nil,
      changed_keys: :all,
      assigned_category: nil,
      now:,
      flag?: ->(k){ FlagProvider.enabled?(flags_snap, k) },
      brand_lookup:  ->(id){ lookups.brands[id] },
      strain_lookup: ->(n){  lookups.strains[n] },
      tag_lookup:    ->(n){  lookups.tags[n] }
    )
    ruleset.evaluate(ctx)
  end
end

# app/services/etl/transformers/update_transformer.rb
# We are using rulesets + rules to transform the payload- the update transformer wraps that ruleset with its needed and specific context.
class UpdateTransformer
  extend Dry::Initializer
  option :ruleset

  def call(payload:, menu_item:, changed_keys:, lookups:, flags_snap:, now:)
    ctx = MenuItemContext.new(
      payload:, menu_item:, changed_keys: Array(changed_keys), assigned_category: nil, now:,
      flag?: ->(k){ FlagProvider.enabled?(flags_snap, k) },
      brand_lookup:  ->(id){ lookups.brands[id] },
      strain_lookup: ->(n){  lookups.strains[n] },
      tag_lookup:    ->(n){  lookups.tags[n] }
    )
    ruleset.evaluate(ctx)
  end
end

```

Input contracts (dry-schema + dry-validation)

```ruby
# app/contracts/menu_item_input_contract.rb

class MenuItemInputContract < Dry::Validation::Contract
  params do
    required(:external_id).filled(:string)
    optional(:name).maybe(:string)
    optional(:category_names).array(:string)
    optional(:brand_id).maybe(:string)
    optional(:variants).array(:hash)
    # add fields as needed
  end
end

# app/contracts/transformed_contract.rb

class TransformedContract < Dry::Validation::Contract
  params do
    optional(:brand_id).filled(:integer)
    optional(:categories).array(:any)
    optional(:variants).array(:any)
    optional(:price)
    optional(:tags).array(:any)
  end
end
```

Ruleset config load and wiring

```ruby
cfg_name, cfg_ver, rule_list = RulesetConfig.load("config/rulesets/menu_item_update.yml").instantiate

ruleset = RuleSet.new(name: cfg_name, rules: rule_list, conflict: :last_wins)

update_tx = UpdateTransformer.new(ruleset:)
create_tx = CreateTransformer.new(ruleset: RuleSet.new(
  name: "menu_item_create",
  rules: rule_list, # or a different manifest
  conflict: :last_wins
))
```

Pipeline call site

```ruby
flags_snap = FlagProvider.snapshot(actor_key: "menu-crawler")
lookups    = PreloadRefs.new(
  brands_repo: BrandRepo.new, strains_repo: StrainRepo.new, tags_repo: TagRepo.new
).call(items)

items.each do |it|
  if it[:menu_item]
    changes, fired = update_tx.call(payload: it[:payload], menu_item: it[:menu_item],
                                    changed_keys: it[:changed_keys], lookups:, flags_snap:, now: Time.current)
  else
    changes, fired = create_tx.call(payload: it[:payload], lookups:, flags_snap:, now: Time.current)
  end

# validate and persist

end
```

DAG in CI (unchanged, but typed)

```ruby
# build nodes from rule_list.map(&:metadata); run validations; fail on cycles/conflicts
```

This keeps your ETL strongly typed at the boundaries, rules parameterized via `dry-initializer`, context immutable via `Dry::Struct`, and contracts enforcing input/output.

```ruby
batch_now  = Time.current.freeze
flags_snap = FlagProvider.snapshot(actor_key: "menu-crawler")
lookups    = PreloadRefs.new(
  brands_repo: BrandRepo.new, strains_repo: StrainRepo.new, tags_repo: TagRepo.new
).call(items)

items.each do |it|
  if it[:menu_item]
    changes, fired = update_tx.call(
      payload: it[:payload],
      menu_item: it[:menu_item],
      changed_keys: it[:changed_keys],
      lookups: lookups,
      flags_snap: flags_snap,
      now: batch_now
    )
  else
    changes, fired = create_tx.call(
      payload: it[:payload],
      lookups: lookups,
      flags_snap: flags_snap,
      now: batch_now
    )
  end

# validate and persist

end
```

If you prefer explicit typing, wrap it:

```ruby
BatchContext = Struct.new(:now, :flags_snap, :lookups, keyword_init: true)

ctx = BatchContext.new(now: Time.current.freeze, flags_snap:, lookups:)

items.each do |it|
  tx = it[:menu_item] ? update_tx : create_tx
  changes, fired = tx.call(**it.slice(:payload, :menu_item, :changed_keys).compact,**ctx.to_h)
end
```

Test it:

```ruby
it "uses the same time for all items" do
  batch_now = Time.utc(2025,8,31,12,0,0).freeze

# run pipeline

  expect(collected_nows.uniq).to eq([batch_now])
end
```

## PipeLine

```ruby
# frozen_string_literal: true

# Minimal dry-rb–styled pipeline with frozen time, preload lookups, flag snapshot, and create/update transformers

# types

module Types
  include Dry.Types()
  Json      = Types::Hash
  Item      = Types::Hash.schema(
    source_id:   Types::String,
    ingest_id:   Types::String,
    payload:     Types::Json,
    changed_keys: Types::Any.optional,
    menu_item:   Types::Any.optional
  )
  Items = Types::Array.of(Item)
end

# batch-scoped context

class BatchContext < Dry::Struct
  attribute :now,         Types::Params::Time
  attribute :flags_snap,  Types::Any
  attribute :lookups,     Types::Any
  attribute :env,         Types::String
  attribute :source_id,   Types::String
end

# preload refs (repos injected)

class PreloadRefs
  extend Dry::Initializer
  option :brands_repo
  option :strains_repo
  option :tags_repo

  def call(items)
    brand_ids    = items.filter_map { _1[:payload]['brand_id'].presence }.uniq
    strain_names = items.filter_map { _1[:payload]['strain_name'].presence }.uniq
    tag_names    = items.flat_map { Array(_1[:payload]['tag_names']) }.compact.uniq
    OpenStruct.new(
      brands:  brands_repo.by_ids(brand_ids),
      strains: strains_repo.by_names(strain_names),
      tags:    tags_repo.by_names(tag_names)
    ).freeze
  end
end

# input contracts

class MenuItemInputContract < Dry::Validation::Contract
  params do
    required(:external_id).filled(:string)
    optional(:name).maybe(:string)
    optional(:category_names).array(:string)
    optional(:brand_id).maybe(:string)
    optional(:variants).array(:hash)
  end
end

class TransformedContract < Dry::Validation::Contract
  params do
    optional(:brand_id).maybe(:string)
    optional(:categories).array(:any)
    optional(:variants).array(:any)
    optional(:price)
    optional(:tags).array(:any)
  end
end

# transformers (assume you already wired rulesets into these)

class CreateTransformer
  extend Dry::Initializer
  option :ruleset

  def call(payload:, lookups:, flags_snap:, now:)
    ctx = MenuItemContext.new(
      payload:, menu_item: nil, changed_keys: :all, assigned_category: nil, now:,
      flag?: ->(k){ FlagProvider.enabled?(flags_snap, k) },
      brand_lookup:  ->(id){ lookups.brands[id] },
      strain_lookup: ->(n){  lookups.strains[n] },
      tag_lookup:    ->(n){  lookups.tags[n] }
    )
    ruleset.evaluate(ctx)
  end
end

class UpdateTransformer
  extend Dry::Initializer
  option :ruleset

  def call(payload:, menu_item:, changed_keys:, lookups:, flags_snap:, now:)
    ctx = MenuItemContext.new(
      payload:, menu_item:, changed_keys: Array(changed_keys), assigned_category: nil, now:,
      flag?: ->(k){ FlagProvider.enabled?(flags_snap, k) },
      brand_lookup:  ->(id){ lookups.brands[id] },
      strain_lookup: ->(n){  lookups.strains[n] },
      tag_lookup:    ->(n){  lookups.tags[n] }
    )
    ruleset.evaluate(ctx)
  end
end

# artifact store (S3) is optional; inject anything responding to write!(key:, body:, content_encoding:)

class NullArtifactStore
  def self.write!(**) = true
end

# pipeline

class Pipeline
  extend Dry::Initializer

  option :create_tx,      reader: :private
  option :update_tx,      reader: :private
  option :preloader,      reader: :private
  option :input_contract,      default: -> { MenuItemInputContract.new }, reader: :private
  option :transformed_contract, default: -> { TransformedContract.new },   reader: :private
  option :artifact_store, default: -> { NullArtifactStore }, reader: :private
  option :ruleset_version, default: -> { 'v1' }

# items: Types::Items

  def run(items, env:, source_id:)
    items = Types::Items[items] # shape check
    batch_now  = Time.current.freeze
    flags_snap = FlagProvider.snapshot(actor_key: source_id)
    lookups    = preloader.call(items)

    batch_ctx = BatchContext.new(now: batch_now, flags_snap:, lookups:, env:, source_id:)
    items.map { |it| process_one(it, batch_ctx) }
  end

  private

  def process_one(it, batch_ctx)
    validated = input_contract.call(it[:payload])
    return write_and_result(:rejected, it, batch_ctx, {}, [], validated.errors.to_h) unless validated.success?

    batch_ctx => {lookups:, flags_snap:, now:}
    it => {menu_item:, payload:, changed_keys:}

    changes, fired = if menu_item.present?
      update_tx.call(
        payload:,
        menu_item:,
        changed_keys:,
        lookups:,
        flags_snap:,
        now:
      )
    else
      create_tx.call(
        payload:,
        lookups:,
        flags_snap:,
        now: 
      )
    end

    tval = transformed_contract.call(changes)
    return write_and_result(:rejected, it, batch_ctx, changes, fired, tval.errors.to_h) unless tval.success?

    status = persist(it, changes)
    write_and_result(status, it, batch_ctx, changes, fired, nil)
  rescue => e
    write_and_result(:rejected, it, batch_ctx, {}, [], { exception: e.class.name, message: e.message })
  end

  def persist(it, changes)
    return :noop if changes.empty?
    
    if it[:menu_item]
      it[:menu_item].update!(changes)
      :updated
    else
      MenuItem.create!(changes.merge(external_id: it[:payload]['external_id']))
      :created
    end
  end

  def write_and_result(status, it, batch_ctx, changes, fired, violations)
    pack = {
      pack_version: 1,
      produced_at:  batch_ctx.now.to_i,
      env:          batch_ctx.env,
      source_id:    it[:source_id],
      external_id:  it[:payload]['external_id'],
      ingest_id:    it[:ingest_id],
      ruleset_version: ruleset_version,
      flags_version:   batch_ctx.flags_snap.version,
      status:      status.to_s,
      fired_rules: fired,
      mapped_payload: it[:payload],
      changes:     changes,
      violations:  violations
    }
    body = StringIO.new.tap { |s| gz = Zlib::GzipWriter.new(s); gz.write(JSON.dump(pack)); gz.close }.string
    key  = ArtifactStore.key_for(
      env: batch_ctx.env,
      date: batch_ctx.now.strftime('%F'),
      status: status,
      source_id: it[:source_id],
      external_id: it[:payload]['external_id'],
      ingest_id: it[:ingest_id],
      ruleset_version:
    )
    artifact_store.write!(key:, body:, content_encoding: 'gzip')

    { id: it[:payload]['external_id'], status:, fired_rules: fired, violations: violations }
  end
end
```

## Entry Point Examples

Here are two concrete entry points: a single-item webhook and a batch ingest. Rails-style, minimal, deterministic.
Wiring (initializer)

```ruby
# config/initializers/etl_pipeline.rb

CREATE_CFG = Rails.root.join("config/rulesets/menu_item_create.yml")
UPDATE_CFG = Rails.root.join("config/rulesets/menu_item_update.yml")

create_cfg = RulesetConfig.load(CREATE_CFG).instantiate # => [name, ver, rules]
update_cfg = RulesetConfig.load(UPDATE_CFG).instantiate

CREATE_RULESET = RuleSet.new(name: create_cfg[0], rules: create_cfg[2])
UPDATE_RULESET = RuleSet.new(name: update_cfg[0], rules: update_cfg[2])

CREATE_TX = CreateTransformer.new(ruleset: CREATE_RULESET)
UPDATE_TX = UpdateTransformer.new(ruleset: UPDATE_RULESET)

PRELOADER = PreloadRefs.new(
  brands_repo:  BrandRepo.new,
  strains_repo: StrainRepo.new,
  tags_repo:    TagRepo.new
)

PIPELINE = Pipeline.new(
  create_tx: CREATE_TX,
  update_tx: UPDATE_TX,
  preloader: PRELOADER,
  artifact_store: ArtifactStore, # your S3 impl
  ruleset_version: update_cfg[1]
)
```

### Single-item webhook

```ruby
# app/controllers/webhooks/menu_items_controller.rb

class Webhooks::MenuItemsController < ApplicationController
  protect_from_forgery with: :null_session

# POST /webhooks/menu_items

  def create
    # 1) Parse + normalize
    source_id  = request.headers['X-Source-Id'] || 'menu-crawler'
    ingest_id  = request.headers['X-Request-Id'] || SecureRandom.uuid
    payload    = params.to_unsafe_h # or JSON.parse(request.raw_post)

    # 2) Find existing (one query)
    ext_id = payload['external_id']
    menu_item = MenuItem.find_by(source_id:, external_id: ext_id)

    # 3) Shape to internal item
    item = {
      source_id: source_id,
      ingest_id: ingest_id,
      payload:   payload,
      changed_keys: menu_item ? payload.keys : :all, #Use changeset here
      menu_item: menu_item
    }

    # 4) Run pipeline (uses frozen batch time inside)
    result = PIPELINE.run(Array.wrap(item), env: Rails.env, source_id: source_id).first

    # 5) Respond
    status_code = case result[:status]
                  when :created then 201
                  when :updated then 200
                  when :noop    then 200
                  when :rejected then 422
                  else 200
                  end

    render json: { external_id: ext_id, status: result[:status], fired_rules: result[:fired_rules], violations: result[:violations] }, status: status_code
  end
end
```

```json
// Example request:
// POST /webhooks/menu_items
// X-Source-Id: menu-crawler
// Content-Type: application/json

{
  "external_id": "X123",
  "name": "Blue Dream 3.5g",
  "brand_id": "b_42",
  "category_names": ["Flower"],
  "variants": [ { "price": { "amount": 25, "currency": "USD" }, "weight": { "unit":"GRAM", "value":3.5 } } ]
}
```

### Batch ingest endpoint

```ruby
# app/controllers/ingest/menu_items_controller.rb

class Ingest::MenuItemsController < ApplicationController
  protect_from_forgery with: :null_session

# POST /ingest/menu_items/batch

  def create
    source_id = request.headers['X-Source-Id'] || 'menu-crawler'
    ingest_id = request.headers['X-Request-Id'] || SecureRandom.uuid

    rows = params.require(:items) # array of payloads
    ext_ids = rows.map { |r| r['external_id'] }.compact.uniq

    # 1) Preload existing once
    existing = MenuItem.where(source_id:, external_id: ext_ids).index_by(&:external_id)

    # 2) Shape items
    items = rows.map do |payload|
      ext_id = payload['external_id']
      mi     = existing[ext_id]
      {
        source_id: source_id,
        ingest_id: ingest_id, # reuse batch id or add per-row id if you prefer
        payload:   payload.to_h,
        changed_keys: mi ? payload.keys : :all,
        menu_item: mi
      }
    end

    # 3) Run pipeline once for the collection
    results = PIPELINE.run(items, env: Rails.env, source_id: source_id)

    # 4) Summarize
    counts = results.group_by { |r| r[:status] }.transform_values!(&:size)

    render json: { counts:, results: results }, status: 200
  end
end
```

```json
// Example request:
// POST /ingest/menu_items/batch
// X-Source-Id: menu-crawler
// Content-Type: application/json

{
  "items": [
    { "external_id":"X123", "name":"Blue Dream 3.5g", "brand_id":"b_42", "category_names":["Flower"], "variants":[{ "price":{"amount":25,"currency":"USD"}, "weight":{"unit":"GRAM","value":3.5}}]},
    { "external_id":"X124", "name":"Gummies 10-pack", "brand_id":"b_99", "category_names":["Edible"], "price":{"amount":15,"currency":"USD"} }
  ]
}
```

#### Notes

Both endpoints reuse the same `PIPELINE`. The pipeline freezes a single `batch_now`, snapshots Flipper flags once, and preloads refs once for the batch.

`changed_keys`: on webhook you can pass precise diffs if you have them; else `payload.keys` is a reasonable update heuristic.
