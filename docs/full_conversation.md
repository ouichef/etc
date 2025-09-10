# Initial Conversation

Replace sprawling conditionals with data-driven rules. Structure it so each rule is tiny, testable, and composes into a final changes hash.

- High-leverage approach
- Model `“rule = predicate + effect”.`
- Evaluate rules over an immutable input. No globals.
- Merge effects with a clear conflict policy (first-win, last-win, or priority).
- Trace which rules fired.
- Unit test each rule, plus a few integration golden cases.

## First Sketch

```ruby
# input is a plain value object

Input = Struct.new(:user, :plan, :flags, keyword_init: true)

class Rule
  attr_reader :name, :priority
  def initialize(name:, priority: 0, if:, then:)
    @name     = name
    @priority = priority
    @pred     = binding.local_variable_get(:if)
    @eff      = binding.local_variable_get(:then)
  end
  def applies?(ctx) = @pred.call(ctx)
  def effect(ctx)   = @eff.call(ctx) # returns partial hash
end

class RuleSet
  def initialize(rules, conflict: :last_wins)
    @rules    = rules.sort_by { |r| -r.priority }
    @conflict = conflict
  end

  def evaluate(ctx)
    fired = []
    result = {}
    @rules.each do |r|
      next unless r.applies?(ctx)
      fired << r.name
      patch = r.effect(ctx)
      result = merge(result, patch)
    end
    [result.freeze, fired]
  end

  private

  def merge(base, patch)
    case @conflict
    when :last_wins  then base.merge(patch)
    when :first_wins then patch.merge(base)
    when :error_on_conflict
      conflict_keys = base.keys & patch.keys
      raise "Conflict on #{conflict_keys}" unless conflict_keys.empty?
      base.merge(patch)
    else base.merge(patch)
    end
  end
end

rules = [
  Rule.new(
    name: "trial_discount",
    priority: 10,
    if:  ->(c) { c.user.trial? && c.plan.promo? },
    then:->(c) { {discount_cents: 500, note: "trial+promo"} }
  ),
  Rule.new(
    name: "enterprise_overrides",
    priority: 100,
    if:  ->(c) { c.plan.enterprise? },
    then:->(c) { {support_tier: "gold", sla_hours: 4} }
  ),
  Rule.new(
    name: "feature_flag_gate",
    priority: 50,
    if:  ->(c) { c.flags[:new_checkout] },
    then:->(c) { {checkout_flow: "v2"} }
  )
]

changes, fired = RuleSet.new(rules, conflict: :last_wins).evaluate(input)
Variants if you prefer tables or pattern matching
Decision table (fast to scan, easier to audit):
DECISIONS = [
  {when: ->(c){ c.user.trial? && c.plan.promo? }, set: {discount_cents: 500}},
  {when: ->(c){ c.plan.enterprise? },              set: {support_tier: "gold"}}
]
def compute_changes(c)
  DECISIONS.each_with_object({}) { |row, out| out.merge!(row[:set]) if row[:when].call(c) }
end
```

### Testing scaffold

- Unit test each rule: predicate truth table + effect snapshot.
- Property test: rules never set unknown keys; conflicts handled as configured.
- Golden cases: end-to-end input -> changes fixtures.

### Migration plan

- List every current conditional branch as a candidate rule.
- Normalize outputs to a single flat hash API.
- Assign priorities or choose a deterministic merge policy.
- Add telemetry: store fired for debuggability.
- Delete the old branches once parity tests pass.

```ruby
class Crawler::MenuItemPayload::Converter < ApplicationService
  ATTRIBUTES = Crawler::MenuItemPayload::Parser::ATTRIBUTES.deep_dup.freeze
  CANNABINOID_ATTRIBUTES = %w[cbd_milligrams cbd_percentage thc_milligrams thc_percentage].freeze
  CURRENCY_ENUM = { 'USD' => 'USD' }.freeze
  GENETICS_ENUM = { 'INDICA' => :indica, 'HYBRID' => :hybrid, 'SATIVA' => :sativa }.freeze
  LICENSE_TYPE_ENUM = { 'HYBRID' => :recreational, 'MEDICAL' => :medical, 'RECREATIONAL' => :recreational }.freeze
  MILLIGRAM_RANGE = 0.0..Float::MAX
  PERCENTAGE_RANGE = Measurement::PERCENT_RANGE
  PRICE_RULE_ENUM = { 'PERCENTAGE' => :percentage, 'FIXED_AMOUNT' => :fixed_amount }.freeze
  VARIANT_CHANGES_ATTRIBUTES = (
    MenuItem::MIGRATED_EACH_VARIANT_ATTRIBUTES[MenuItem.name] + %w[categories direct_sale_price price
                                                                   integrator_metadata variants]
  ).uniq.freeze
  WEIGHT_ENUM = { 'GRAM' => :g, 'OUNCE' => :oz, 'MILLIGRAM' => :mg, 'KILOGRAM' => :kg, 'POUND' => :lb }.freeze

  option :changed_attributes, Types::Coercible::Array
  option :menu_item_payload, Types::Coercible::Hash
  option :menu_item, Types.Instance(MenuItem), optional: true

  def call
    @changes = menu_item_payload.slice(*changed_attributes)
    apply_changes_to_tags # apply tags before categorization since category_names will be removed
    apply_changes_to_categorization
    apply_changes_to_variants
    apply_changes_to_brand_id
    apply_changes_to_remaining_attributes
    apply_autotag_attributes
    apply_changes_to_measurements
    changes
  end

  delegate :menu, to: :menu_item, allow_nil: true

  private

  attr_reader :changes

  def apply_changes_to_measurements
    return unless changes.key?('cannabinoids') || changes.key?('terpenes')
    params = changes.slice('cannabinoids', 'terpenes').deep_symbolize_keys
    measurements = Measurement::MeasuredParamsService.call[params, menu_item](:measurements_attributes)&.reject do |m|
      m[:measured_id].nil?
    end

    changes.merge!('measurements_attributes' => measurements).except!('cannabinoids', 'terpenes')
  end

  def apply_autotag_attributes
    changes['cannabinoid_auto_tag'] = false if CANNABINOID_ATTRIBUTES.any? { changes[_1].present? }
    changes['strain_auto_association'] = false if changes['strain_id'].present?
  end

# If we cannot find the brand_id, remove the change so we don't overwrite merchandising

  def apply_changes_to_brand_id
    return unless changes.key?('brand_id')

    if (brand_id = locate_brand_id(changes['brand_id']))
      changes['brand_id'] = brand_id
    else
      changes.delete('brand_id')
    end
  end

  def apply_changes_to_tags
    return unless changes.key?('category_names')
    filtered_category_names = changes['category_names'].to_a.filter(&:presence)

    changes['tags'] = AutoTagger.call(text: "#{menu_item_payload['name']} #{filtered_category_names.join(' ')}")&.tags
  end

  def apply_changes_to_categorization
    return unless changes.key?('category_names')

    changes['categories'] = ExternalItemCategorizerService.call(
      category_names: changes['category_names'].to_a.filter(&:presence),
      name: menu_item_payload['name'],
      brand_name: menu_item_payload['brand_name']
    ).categories

    changes.delete('category_names')
  end

  def categorization_weight_name
    weighted_variants = menu_item_payload['variants'].to_a.filter { _1['weight'].present? }
    return if weighted_variants.length != 1

    converted_weight(weighted_variants.first).to_s
  end

# When ANY pricing or categories change, we want to reapply pricing no matter what

# This helps to account for situations where something went to/from a category sold by weight

# to/from a category that is not sold by weight

  def apply_changes_to_variants # rubocop:disable Metrics/AbcSize
    return unless apply_changes_to_variants?

    if apply_changes_to_weight_variants?
      changes['price'] = Price.empty
      changes['variants'] = converted_weight_variants
    else
      changes['variants'] = converted_each_variants
      changes['price'] = changes['variants'].first.price
    end
  end

  def apply_changes_to_variants?
    VARIANT_CHANGES_ATTRIBUTES.any? { changes.key?(_1) }
  end

  def apply_changes_to_weight_variants?
    return false unless assigned_category&.sold_by_weight?
    menu_item_payload['variants'].to_a.any?
  end

  def apply_changes_to_remaining_attributes
    ATTRIBUTES.each do |field_name|
      method_name = :"changes_for_#{field_name}"
      next unless respond_to?(method_name, true)
      send(method_name)
    end
  end

  def assigned_category
    @assigned_category ||= changes['categories'].to_a.find(&:root?) || root_category(menu_item)
  end

  def root_category(menu_item)
    return if menu_item.blank?
    DeprecatedRootCategoryService.call(menu_item)
  end

  def assigned_category_prefers_milligrams?
    assigned_category&.cannabinoid_measurement == 'milligrams'
  end

  def assigned_category_prefers_percentage?
    assigned_category&.cannabinoid_measurement == 'percentage'
  end

  def changes_for_cbd_milligrams
    return unless changes.key?('cbd_milligrams') || changes.key?('categories')
    return changes['cbd_milligrams'] = nil unless assigned_category_prefers_milligrams?

    changes['cbd_milligrams'] = menu_item_payload['cbd_milligrams']&.clamp(MILLIGRAM_RANGE)
  end

  def changes_for_cbd_percentage
    return unless changes.key?('cbd_percentage') || changes.key?('categories')
    return changes['cbd_percentage'] = nil unless assigned_category_prefers_percentage?

    changes['cbd_percentage'] = menu_item_payload['cbd_percentage']&.clamp(PERCENTAGE_RANGE)
  end

  def changes_for_genetics
    return unless changes.key?('genetics')

    changes['genetics'] = GENETICS_ENUM[changes['genetics']]
  end

  def changes_for_image_urls
    return unless changes.key?('image_urls')

    current_image_url = menu_item&.picture&.download_source_url
    provided_image_urls = changes['image_urls'].to_a
    image_url = provided_image_urls.include?(current_image_url) ? current_image_url : provided_image_urls.first

    changes.delete('image_urls')
    changes['image_url'] = image_url
  end

  def changes_for_inventory_quantity
    return unless changes.key?('inventory_quantity')

    changes['inventory_quantity'] = changes['inventory_quantity']&.clamp(MenuItem::INVENTORY_QUANTITY_RANGE)
  end

  def changes_for_license_type
    return unless changes.key?('license_type')

    changes['license_type'] = LICENSE_TYPE_ENUM[changes['license_type']]
  end

  def changes_for_name
    return unless changes.key?('name')

    changes['name'] = changes['name']&.truncate(MenuItem::MAXIMUM_NAME_LENGTH)
  end

  def changes_for_online_orderable
    return unless changes.key?('online_orderable')
    return if changes['online_orderable'].in?([true, false])

    changes.delete('online_orderable')
  end

  def changes_for_direct_sale_price
    return unless changes.key?('direct_sale_price')

    changes.delete('direct_sale_price')
  end

  def changes_for_published
    return unless changes.key?('published')
    return if changes['published'].in?([true, false])

    changes.delete('published')
  end

  def changes_for_price_rules
    return unless changes.key?('price_rules')

    changes['price_rules'] = converted_price_rules
  end

# If we cannot find the product_id, remove the change so we don't overwrite merchandising

  def changes_for_product_id
    return unless changes.key?('product_id')

    # Ensure we use the product's brand ID if it can be located
    if (product = located_product_from_id(changes['product_id']))
      changes['brand_id'] = product.brand_id
      changes['brand_product_id'] = product.id
    end

    changes.delete('product_id')
  end

  def changes_for_shared_inventory
    return unless changes.key?('shared_inventory')
    return if changes['shared_inventory'].in?([true, false])

    changes.delete('shared_inventory')
  end

  def changes_for_strain_name
    return unless changes.key?('strain_name')

    strain_name = changes['strain_name'].presence
    changes.delete('strain_name')

    return if strain_name.blank?
    return unless (strain_id = locate_strain_id_for_name(strain_name))

    changes['strain_id'] = strain_id
  end

  def changes_for_tag_names
    return unless changes.key?('tag_names')

    tag_names = changes['tag_names'].to_a.filter(&:presence)
    changes.delete('tag_names')

    return unless (tags = locate_tags_for_names(tag_names))

    changes['tags'] = tags
  end

  def changes_for_thc_milligrams
    return unless changes.key?('thc_milligrams') || changes.key?('categories')
    return changes['thc_milligrams'] = nil unless assigned_category_prefers_milligrams?

    changes['thc_milligrams'] = menu_item_payload['thc_milligrams']&.clamp(MILLIGRAM_RANGE)
  end

  def changes_for_thc_percentage
    return unless changes.key?('thc_percentage') || changes.key?('categories')
    return changes['thc_percentage'] = nil unless assigned_category_prefers_percentage?

    changes['thc_percentage'] = menu_item_payload['thc_percentage']&.clamp(PERCENTAGE_RANGE)
  end

  def converted_cart_quantity_multiplier(value)
    value.to_f.nonzero?
  end

  def converted_inventory_quantity(value)
    return if value.blank?

    value.clamp(MenuItem::Variant::INVENTORY_QUANTITY_RANGE)
  end

  def converted_price(hash)
    return Price.empty unless (amount = hash.dig('price', 'amount').to_f.nonzero?)
    return Price.empty unless (currency = CURRENCY_ENUM[hash.dig('price', 'currency')])

    Price.from_amount(amount, currency)
  end

  def converted_sale_price(hash)
    return Price.empty unless hash
    return Price.empty unless (amount = hash['amount'].to_f.nonzero?)
    return Price.empty unless (currency = CURRENCY_ENUM[hash['currency']])

    Price.from_amount(amount, currency)
  end

  def converted_price_rule(price_rule)
    return unless (adjustment_type = PRICE_RULE_ENUM[price_rule['adjustment_type']])
    return unless (adjustment_value = price_rule['adjustment_value'].to_f.nonzero?)
    return unless [true, false].include?(price_rule['active'])

    if adjustment_type == PRICE_RULE_ENUM['PERCENTAGE']
      return if MenuItem::PriceRule::PERCENTAGE_VALUE_RANGE.max <= adjustment_value
      return if MenuItem::PriceRule::PERCENTAGE_VALUE_RANGE.min >= adjustment_value
    end

    MenuItem::PriceRule.new(
      adjustment_type:,
      adjustment_value:,
      active: Bool[price_rule['active']],
      expires_at: price_rule['expires_at']
    )
  end

  def converted_price_rules
    changes['price_rules'].to_a.filter_map { converted_price_rule(_1) }
  end

  def converted_each_variants # rubocop:disable Metrics/AbcSize
    menu_item_variant = MenuItem::Variant.new(
      key: MenuItem::Variant::DEFAULT_EACH_NAME.parameterize,
      price: converted_price(menu_item_payload).presence,
      cart_quantity_multiplier: converted_cart_quantity_multiplier(menu_item_payload['cart_quantity_multiplier']),
      compliance_net_mg: menu_item_payload['compliance_net_mg'],
      compliance_net_precalc: menu_item_payload.fetch('compliance_net_precalc', false),
      external_id: menu_item_payload['external_id'],
      inventory_quantity: converted_inventory_quantity(menu_item_payload['inventory_quantity']),
      items_per_pack: menu_item_payload['items_per_pack'],
      integrator_metadata: menu_item_payload['integrator_metadata'],
      ratio: menu_item_payload['ratio']
    )

    maybe_apply_license_type(menu_item_variant)
    maybe_apply_online_orderable(menu_item_variant)
    maybe_apply_direct_sale_price(menu_item_variant, menu_item_payload['direct_sale_price'])

    [menu_item_variant]
  end

  def converted_weight_variant(variant) # rubocop:disable Metrics/AbcSize
    price = converted_price(variant)
    weight = converted_weight(variant)

    return if price.empty? || weight&.value.to_f.zero?

    MenuItem::Variant.new(
      price:,
      weight:,
      cart_quantity_multiplier: converted_cart_quantity_multiplier(variant['cart_quantity_multiplier']),
      compliance_net_mg: variant['compliance_net_mg'],
      compliance_net_precalc: variant.fetch('compliance_net_precalc', false),
      external_id: variant['external_id'].presence || menu_item_payload['external_id'],
      inventory_quantity: converted_inventory_quantity(variant['inventory_quantity']),
      integrator_metadata: variant['integrator_metadata'].presence || menu_item_payload['integrator_metadata']
    ).tap do |menu_item_variant|
      maybe_apply_license_type(menu_item_variant, variant)
      maybe_apply_online_orderable(menu_item_variant, variant)
      maybe_apply_direct_sale_price(menu_item_variant, variant['direct_sale_price'])
    end
  end

  def converted_weight_variants
    menu_item_payload['variants']
      .to_a
      .filter_map { converted_weight_variant(_1) }
      .sort_by(&:weight)
      .first(MenuItem::VariantValidator::MAX_VARIANTS)
  end

  def converted_weight(hash)
    return unless (unit = WEIGHT_ENUM[hash.dig('weight', 'unit')])
    return unless (value = hash.dig('weight', 'value').to_f.nonzero?)

    provided_weight = Weight.new(value, unit)

    case provided_weight.in_grams.value.to_f
    when 3.5 then Weight.new(0.125, :ounce)
    when 7.0 then Weight.new(0.25, :ounce)
    when 14.0 then Weight.new(0.5, :ounce)
    when 28.0 then Weight.new(1, :ounce)
    else provided_weight
    end
  end

  def locate_brand_id(brand_id)
    return if brand_id.blank?

    scope = Brand.published
    scope.exists?(id: brand_id) ? brand_id : nil
  end

  def locate_strain_id_for_name(strain_name)
    return if strain_name.blank?

    StrainIdLocatorService.call(strain_name)
  end

  def locate_tags_for_names(tag_names)
    return if tag_names.blank?

    tags = Tag.published.column_case_insensitive_any(:name, tag_names) +
           AutoTagger.call(text: tag_names).tags

    tags.uniq.presence
  end

  def located_product_from_id(product_id)
    return if product_id.blank?

    scope = BrandProduct.published.with_published_brand
    scope.select(:brand_id, :id).find_by(id: product_id)
  end

  def maybe_apply_license_type(variant_record, variant_payload = {})
    return unless variant_payload.key?('license_type') || menu_item_payload.key?('license_type')

    license_type = LICENSE_TYPE_ENUM[variant_payload['license_type']]
    license_type ||= LICENSE_TYPE_ENUM[menu_item_payload['license_type']]

    variant_record.license_type = license_type
  end

  def maybe_apply_online_orderable(variant_record, variant_payload = {})
    return unless variant_payload.key?('online_orderable') || menu_item_payload.key?('online_orderable')

    online_orderable = variant_payload['online_orderable']
    online_orderable = menu_item_payload['online_orderable'] if online_orderable.nil?
    return unless online_orderable.in?([true, false])

    variant_record.online_orderable = online_orderable
  end

  def maybe_apply_direct_sale_price(variant_record, direct_sale_price)
    return unless Flag.enabled?(:crawler_write_variant_direct_sale_price)
    return if direct_sale_price.blank?

    variant_record.direct_sale_price = converted_sale_price(direct_sale_price)
  end
end
```

This class is too procedural, too coupled, and violates SRP (Single Responsibility Principle) at almost every level. You have:

- Rule logic mixed with transformation logic
- Conditionals embedded in method names (changes_for_*)
- Implicit dependency on payload shape and MenuItem internals
- Hard-to-test branches spread across ~50 methods

Refactor strategy:

1. Extract Rule Objects

- Each `changes_for_*` and `apply_changes_to_*` becomes a `Rule` object.
- Input: payload and context. Output: a partial change hash.

```ruby
class Rule
  def applies?(context); ... end
  def changes(context); ... end
end
```

Group them into a RuleSet as explained earlier. Maintain execution order with priorities.
2. Model Context

- Centralize access to input data (`menu_item_payload`, `menu_item`, etc) in a plain struct:

```ruby
Context = Struct.new(:payload, :menu_item, :changed_keys, :category, keyword_init: true)
```

Enrich it once (e.g. resolved category, brand, product, etc). Use memoized query objects if needed.
3. Standardize Rule Output

- Rules return partial hashes.
- Merge with a merge policy: last-write-wins is safest.

Example:

```ruby
{
  'cbd_percentage' => 2.5,
  'cannabinoid_auto_tag' => false
}
```

4. Testing and Telemetry
Test each rule independently with context stubs. Trace tags rules by name.

5. Decompose Variant Conversion
Extract into its own converter class with clear inputs and outputs. Don’t mix variant logic into change logic.

6. Replace Conditional Cleanup with Filters
For things like `online_orderable`, `published`, etc., use reusable validators/normalizers:

```ruby
module FieldSanitizers
  def self.boolean(value)
    [true, false].include?(value) ? value : nil
  end
end
```

### Minimal API Surface After Refactor

```ruby
ruleset = MenuItemChangeRules.build
context = MenuItemContext.new(payload:, menu_item:, changed_keys:)
changes, fired_rules = ruleset.evaluate(context)
```

Break apart the class.
Group rules by domain (e.g., tags, measurements, variants) if needed.

## Specific Rule Example

`apply_changes_to_variants` is the worst.
It hides three decisions in one place:

- “should variants rebuild”
- “weight vs each”
- “how to derive price.”

Split it into a single rule with two strategies.

```ruby
# app/services/menu_item_payload/strategies/weight_variants.rb

class MenuItemPayload::Strategies::WeightVariants
  MAX = MenuItem::VariantValidator::MAX_VARIANTS

  def build(ctx)
    ctx.payload['variants']
      .to_a
      .filter_map { build_one(_1, ctx) }
      .sort_by!(&:weight)
      .first(MAX)
  end

  private

  def build_one(p, ctx)
    price  = price_from(p)
    weight = weight_from(p)
    return if price.empty? || weight&.value.to_f.to_i.zero?

    MenuItem::Variant.new(
      price:,
      weight:,
      cart_quantity_multiplier: p['cart_quantity_multiplier'].to_f.nonzero?,
      compliance_net_mg: p['compliance_net_mg'],
      compliance_net_precalc: p.fetch('compliance_net_precalc', false),
      external_id: p['external_id'].presence || ctx.payload['external_id'],
      inventory_quantity: p['inventory_quantity']&.clamp(MenuItem::Variant::INVENTORY_QUANTITY_RANGE),
      integrator_metadata: p['integrator_metadata'].presence || ctx.payload['integrator_metadata']
    ).tap do |v|
      maybe_apply_license_type(v, p, ctx)
      maybe_apply_online_orderable(v, p, ctx)
      maybe_apply_direct_sale_price(v, p['direct_sale_price'])
    end
  end

  def price_from(hash)
    amount   = hash.dig('price', 'amount').to_f
    currency = Crawler::MenuItemPayload::Converter::CURRENCY_ENUM[hash.dig('price', 'currency')]
    return Price.empty if amount.zero? || currency.nil?
    Price.from_amount(amount, currency)
  end

  WEIGHT_ENUM = Crawler::MenuItemPayload::Converter::WEIGHT_ENUM

  def weight_from(hash)
    unit  = WEIGHT_ENUM[hash.dig('weight', 'unit')]
    value = hash.dig('weight', 'value').to_f
    return unless unit && value.nonzero?

    provided = Weight.new(value, unit)
    case provided.in_grams.value.to_f
    when 3.5  then Weight.new(0.125, :ounce)
    when 7.0  then Weight.new(0.25,  :ounce)
    when 14.0 then Weight.new(0.5,   :ounce)
    when 28.0 then Weight.new(1,     :ounce)
    else provided
    end
  end

  def maybe_apply_license_type(v, p, ctx)
    code = p['license_type'] || ctx.payload['license_type']
    v.license_type = Crawler::MenuItemPayload::Converter::LICENSE_TYPE_ENUM[code] if code
  end

  def maybe_apply_online_orderable(v, p, ctx)
    val = p.key?('online_orderable') ? p['online_orderable'] : ctx.payload['online_orderable']
    v.online_orderable = val if [true, false].include?(val)
  end

  def maybe_apply_direct_sale_price(v, dsp)
    return unless Flag.enabled?(:crawler_write_variant_direct_sale_price)
    return if dsp.blank?
    amount   = dsp['amount'].to_f
    currency = Crawler::MenuItemPayload::Converter::CURRENCY_ENUM[dsp['currency']]
    v.direct_sale_price = (amount.zero? || currency.nil?) ? Price.empty : Price.from_amount(amount, currency)
  end
end
```

Wire it into your rule engine

```ruby
# app/services/menu_item_change_rules.rb

class MenuItemChangeRules
  def self.build
    RuleSet.new(
      [
        MenuItemPayload::Rules::VariantsRebuildRule.new,
        # ...other rules...
      ],
      conflict: :last_wins
    )
  end
end
Context object

# app/models/menu_item_context.rb

MenuItemContext = Struct.new(
  :payload, :menu_item, :changed_keys, :assigned_category,
  keyword_init: true
)
```

### Tests to add

- Rebuild triggers when any of VARIANT_TRIGGERS change.
- Weight category → clears price and builds sorted weight variants with ounce normalization.
- Each category → single variant and price mirrors variant price.
- Respect MAX_VARIANTS.
- License type and online_orderable precedence: variant payload over item payload.
- Direct sale price gated by flag.

Result: one rule expresses intent, strategies isolate format details, price derivation is deterministic, and you can unit test the variants independently.

If you want to highlight policy differences, “strategy” fits.
If you want to highlight data transformation, “mapper” fits.

Here they’re really mappers—pure functions from input payload → Variant objects. The rule is the policy layer that picks which mapper to use.

---

## With Resolvers for Associations

Preload once, inject read-only lookup maps into every rule context, then iterate.

- No DB in rules.
- Preload + single pass

```ruby
# items: [{id:, payload:, menu_item:, changed_keys:}, ...]

class RuleBatch
  Result = Struct.new(:id, :changes, :fired, :error, keyword_init: true)

  def initialize(ruleset:, resolvers:)
    @ruleset   = ruleset          # RuleSet (pure)
    @resolvers = resolvers        # Repos with preload APIs
  end

  def evaluate_many(items)
    lookups = preload_maps(items)
    items.map { |it| evaluate_one(it, lookups) }
  end

  private

  def preload_maps(items)
    brand_ids    = items.filter_map {_1[:payload]['brand_id'].presence }.uniq
    strain_names = items.filter_map {_1[:payload]['strain_name'].presence }.uniq
    tag_names    = items.flat_map { Array(_1[:payload]['tag_names']) }.compact.uniq

    {
      brands:  @resolvers.brands.by_ids(brand_ids),      # {id=>Brand or brand_id}
      strains: @resolvers.strains.by_names(strain_names),# {name=>strain_id}
      tags:    @resolvers.tags.by_names(tag_names)       # {name=>Tag}
    }
  end

  def evaluate_one(it, lookups)
    ctx = MenuItemContext.new(
      payload: it[:payload].freeze,
      menu_item: it[:menu_item],
      changed_keys: Array(it[:changed_keys]).freeze,
      assigned_category: nil
    )

    # Inject pure lookup helpers. No mutation. No threads.
    ctx.define_singleton_method(:brand_lookup)  { |id| lookups[:brands][id] }
    ctx.define_singleton_method(:strain_lookup) { |name| lookups[:strains][name] }
    ctx.define_singleton_method(:tag_lookup)    { |name| lookups[:tags][name] }

    changes, fired = @ruleset.evaluate(ctx)
    Result.new(id: it[:id], changes:, fired:)
  rescue => e
    Result.new(id: it[:id], changes: {}, fired: [], error: e)
  end
end

# Repos (preload only)

class BrandRepo
  def by_ids(ids)
    return {} if ids.empty?
    Brand.published.where(id: ids).pick(:id).index_with { |id| id } # or map to full brand if needed
  end
end

class StrainRepo
  def by_names(names)
    return {} if names.empty?
    Strain.where(name: names).pluck(:name, :id).to_h
  end
end

class TagRepo
  def by_names(names)
    return {} if names.empty?
    Tag.published.where(name: names).index_by(&:name)
  end
end
```

Rules consume lookups instead of hitting DB
Example refactors of your heaviest DB callers:

```ruby
# BrandIdRule: replaces apply_changes_to_brand_id

class BrandIdRule
  def applies?(ctx) = ctx.changed_keys.include?('brand_id')

  def changes(ctx)
    id = ctx.payload['brand_id']
    return {} if id.blank?
    ctx.brand_lookup(id) ? { 'brand_id' => id } : {}
  end
end

# StrainNameRule: replaces changes_for_strain_name

class StrainNameRule
  def applies?(ctx) = ctx.changed_keys.include?('strain_name')

  def changes(ctx)
    name = ctx.payload['strain_name'].to_s.strip
    return {} if name.empty?
    if (id = ctx.strain_lookup(name))
      { 'strain_id' => id }
    else
      {} # no DB write to clear merchandising
    end
  end
end

# TagNamesRule: replaces changes_for_tag_names, plus autotagger merge

class TagNamesRule
  def applies?(ctx) = ctx.changed_keys.include?('tag_names')

  def changes(ctx)
    names = Array(ctx.payload['tag_names']).filter_map(&:presence)
    return {} if names.empty?

    located = names.filter_map { |n| ctx.tag_lookup(n) }.uniq
    auto    = AutoTagger.call(text: names)&.tags || []
    tags    = (located + auto).uniq
    tags.empty? ? {} : { 'tags' => tags }
  end
end

# Wire once

ruleset = RuleSet.new(
  [
    VariantsRebuildRule.new(each_mapper: EachVariantMapper.new,
                            weight_mapper: WeightVariantsMapper.new),
    BrandIdRule.new,
    StrainNameRule.new,
    TagNamesRule.new,
    # ...other small rules...
  ],
  conflict: :last_wins
)

resolvers = OpenStruct.new(
  brands:  BrandRepo.new,
  strains: StrainRepo.new,
  tags:    TagRepo.new
)

batch = RuleBatch.new(ruleset:, resolvers:)
results = batch.evaluate_many(items)  # single-threaded
Optional memory control
If items is large, stream:
Enumerator.new do |y|
  items.each_slice(1_000) do |chunk|
    lookups = batch.send(:preload_maps, chunk)
    chunk.each { |it| y << batch.send(:evaluate_one, it, lookups) }
  end
end
```

### Summary

- Preload keys per chunk.
- Inject maps into context.
- Keep rules pure.
- Iterate once. No threads.

## Separating Update and Create

### Existing items

You already have foreign keys (brand_id, product_id, strain_id, tag names). Preload their mappings in bulk, inject into the ruleset, and evaluate changes. This is where the preload step saves you from N+1 lookups.

### New items

You don’t preload because nothing exists yet. You just run the transformation/mapping rules against the raw payload, then later insert records. In some cases you can still preload reference data (brands, strains, tags) so the new rows can be linked immediately, but you’re not preloading the items themselves.

#### In ETL Terms

##### Extract

- pull raw payloads from crawler.
- Transform:
- Preload reference lookups for all candidate rows (brands, strains, tags).
- Build contexts, run the ruleset one per row, get changes.

##### Load

- For existing rows → apply changes update.
- For new rows → insert with mapped attributes.

That means your “preload” stage is strictly a reference data hydration step. It fetches supporting lookups once per batch, not item data. Item-level resolution stays inside the ruleset.

Split create and update. Share mappers and normalizers. Keep separate rule policies.

- Create: aggressive inference and defaults. Fill blanks. Autotag. Categorize. Build variants. Assign FK ids using preload maps. Fail fast on missing required refs.

- Update: conservative. Only touch changed_keys. Never clobber merchandising. Guard writes with existence checks. Prefer no-ops to nulling.

Shape

```ruby
module MenuItemRules
  def self.shared(resolvers) = [
    MeasurementsRule.new,
    GeneticsRule.new,

  # pure normalizers that apply to both

  ]

  def self.for_create(resolvers) = shared(resolvers) + [
    CategorizationRule.new,          # builds categories from names
    TagDeriveRule.new(resolvers:),   # autotag + lookup
    BrandResolveRule.new(resolvers:),
    ProductResolveRule.new(resolvers:),
    VariantsRebuildRule.new(...),    # always build
    DefaultsRule.new,                # required defaults
  ]

  def self.for_update(resolvers) = shared(resolvers) + [
    CategorizationRule.new(only_if_changed: true),
    TagNamesRule.new(resolvers:),    # only if tag_names changed
    BrandIdRule.new(resolvers:),     # only if brand_id changed
    ProductIdRule.new(resolvers:),   # only if product_id changed
    VariantsRebuildRule.new(rebuild_on_triggers: true),
    GuardMerchRule.new,              # drop writes that overwrite merchandising
  ]
end

# Services

class CreateTransformer
  def initialize(resolvers:) = @ruleset = RuleSet.new(MenuItemRules.for_create(resolvers), conflict: :last_wins)
  def call(payload) = @ruleset.evaluate(MenuItemContext.new(payload:, changed_keys: :all))
end

class UpdateTransformer
  def initialize(resolvers:) = @ruleset = RuleSet.new(MenuItemRules.for_update(resolvers), conflict: :last_wins)
  def call(payload:, menu_item:, changed_keys:) =
    @ruleset.evaluate(MenuItemContext.new(payload:, menu_item:, changed_keys: Array(changed_keys)))
end

# Preload once, reuse both
Resolver = Struct.new(brands:, strains:, tags:, products:)
resolvers = Resolver.new(
  brands:  BrandRepo.new,
  strains: StrainRepo.new,
  tags:    TagRepo.new,
  products: ProductRepo.new
  )

# preload_maps(items) → pass into resolvers so both transformers read from the same caches
```

#### Shared subset

Put all pure mappers and validators in shared modules:
`EachVariantMapper`, `WeightVariantsMapper`
`PriceMapper`, `WeightMapper`
`BooleanSanitizer`, `ClampSanitizer`

Rules depend on these. No DB in rules; only resolvers.

#### Policy deltas to encode

Create:

- `VariantsRebuildRule`: always build.
- `Category/Tag rules`: run even if not provided when inferable from name or description.
- `Required fields rule`: raise on missing brand or category after resolution.

Update:

- `VariantsRebuildRule`: run only if triggers intersect changed_keys.
- `BrandIdRule/ProductIdRule`: if resolver misses, drop write, don’t null.
- `ImageUrlsRule`: keep current if present in provided list.
- `Cannabinoid rules`: clear or clamp only when category measurement matches.

##### Minimal example of a mode-specific rule

```ruby
class GuardMerchRule
  def applies?(ctx) = ctx.changed_keys == :all ? false : true
  def changes(ctx)
    out = {}
    out.delete('brand_id')   unless ctx.brand_lookup(ctx.payload['brand_id'])
    out.delete('product_id') # never write raw product_id on update
    out
  end
end
```

#### ETL usage

- Build resolvers with preload maps per batch.
- Use `CreateTransformer` for inserts, `UpdateTransformer` for upserts with `changed_keys`.
- Keep both idempotent so retries are safe.

This separation keeps intent clear, tests smaller, and lets you tune policies without collateral risk.

### Minimal Scaffold

```ruby
class Pipeline
  def run(rows)
    rows = dedupe(rows)
    valid, rejected = validate_input(rows)

  # Map

    mapped = valid.map { |r| [r, MenuItemMapper.call(r)] }

  # Preload refs

    lookups = preload_refs(mapped.map {_2 })

  # Transform

    transformed = mapped.map do |raw, payload|
      ctx = MenuItemContext.new(payload:, menu_item: find_by_external_id(payload), changed_keys: keys_changed(raw), now: @now)
      ctx.define_singleton_method(:brand_lookup)  { |id| lookups[:brands][id] }
      ctx.define_singleton_method(:strain_lookup) { |n|  lookups[:strains][n] }
      ctx.define_singleton_method(:tag_lookup)    { |n|  lookups[:tags][n] }
      changes, fired, desired = RULESET.evaluate(ctx)
      { raw:, payload:, changes:, desired:, fired: }
    end

  # Persist

    transformed.map { |t| persist_one(t) } + rejected.map { |r| {status: :rejected, violations: r[:errors]} }
  end

  def persist_one(t)
    ApplicationRecord.transaction do
      item = MenuItem.find_by(source_id: t[:raw][:source_id], external_id: t[:payload]['external_id'])
      return result(:deleted, t) if tombstone?(t[:payload])

      if item
        return result(:noop, t) if snapshot_equal?(item, t[:desired])
        apply_updates!(item, t[:changes], t[:desired])
        result(:updated, t)
      else
        item = create_with_children!(t[:payload], t[:desired])
        result(:created, t)
      end
    end
  rescue ActiveRecord::RecordInvalid => e
    { status: :rejected, violations: e.record.errors.full_messages, fired_rules: t[:fired] }
  end
end
```

Keys to implement:

- `variant_key`(variant): `external_id.presence || "#{variant.weight.in_grams}g"`.
- `price_rule_key`(rule): `"#{rule.adjustment_type}:#{rule.adjustment_value}:#{rule.expires_at&.to_i}"`.
- `snapshot_equal?`: compare projected DB snapshot to desired by keys, not IDs.
What is a desired snapshot?

```ruby
#Why
Upstream rules may produce arrays, associations, or lists (categories, tags, variants, price #rules, etc). If you apply them naively you risk:
#Duplicates (["flower","Flower"])
#Unstable ordering (nondeterministic diffs)
#Re-applying semantically identical changes (causing unnecessary updates)
#Sort + Deduplication
#Before writing to the DB, normalize every set-like field:
#Tags: downcase, strip, uniq, sort by canonical name or ID.
#Categories: pick unique IDs, sort by ID or by tree order.
#Variants: dedupe by key (e.g. weight, external_id), sort by deterministic attribute.
#Price rules: dedupe by adjustment_type+value, sort by type+expires_at.
#That ensures stable output across runs.
#Emit desired_snapshot structs
#Instead of handing a raw hash of changes straight to ActiveRecord, construct immutable value objects (Dry::Structs) that represent the “desired snapshot” of this record’s state after transformation. Example:
class DesiredMenuItem < Dry::Struct
  attribute :external_id, Types::String
  attribute :brand_id,    Types::String.optional
  attribute :categories,  Types::Array.of(Types::String)
  attribute :tags,        Types::Array.of(Types::String)
  attribute :variants,    Types::Array.of(DesiredVariant)
  attribute :price_rules, Types::Array.of(DesiredPriceRule)
end
# Each DesiredVariant and DesiredPriceRule itself is a typed struct.
# Rules fill these structs, then a final “normalizer” sorts and dedupes each array.
# Compare in persistence
# In the persistence step you compare the desired_snapshot with the current persisted snapshot. This is a structural comparison:
if current_snapshot == desired_snapshot
  :noop
else
  menu_item.update!(desired_snapshot.to_h)
  :updated
end
# This gives:
# Idempotency: same payload always yields same desired_snapshot.
# Clean diffs: no spurious updates from order or duplication noise.
# Easier auditing: you can serialize the desired_snapshot into the artifact pack.
# In short:
# Sort and de-dupe → make your sets deterministic and canonical.
# Emit desired_snapshot structs → typed, immutable representation of the end state.
# Compare in persistence → update only when the desired snapshot differs from the stored record.
```

- `keys_changed`(raw): for updates, computed from source diff; for creates, :all.
- Contracts: separate input vs transformed contracts; both must pass on create, only transformed on update.

## Fired Rules (:fired_rules)

Uses:

- Debugging why a field changed or didn’t.
- Auditing pipeline behavior without diffing raw payloads.
- Alerting when unexpected rules fire (e.g. “VariantsRebuildRule fired 10k times today”).
- Test assertions: “Given input X, only rule A and B should fire.”

It should never drive persistence. You don’t branch on it in the pipeline. Treat it like a log channel:

```ruby
changes, fired = ruleset.evaluate(ctx)

logger.info(event: "transform", item_id: ctx.payload['external_id'], fired_rules: fired)
```

If you want richer traces, store {rule_name, reason} instead of just names.

But core idea: diagnostics only, not state

---

## Refinements

Tighten a few things to make this robust at scale.

- Treat config as data. Load rules, priorities, and flags from a versioned YAML or DB table. Emit the active config version on every run.
- Version everything. Payload schema version, transform config version, and “now” version. Store with outcomes for replay.
- Deterministic time. Inject now once per batch. Never call clocks inside rules.
- Idempotency keys. Use (`source_id`, `external_id`, `payload_fingerprint`) for dedupe and retry safety.
- Snapshot diffs. Compare desired snapshot vs DB by natural keys before writes. Upsert by keys, delete-orphans by diff.
- Roll out by percentage of sources.
- Backpressure. Bound batch sizes, add a work queue, surface lag metrics. Refuse new work when DB pool < threshold.
- Dead-letter queue. On validation or referential misses, serialize row + violations. Make a replay tool.
- Error taxonomy. Schema_reject, transform_reject, referential_miss, conflict, persistence_error. Tag metrics with this.
- Contract tests at boundaries. One suite for input schema, one for transformed schema, one for DB upserts.
- Property-based tests. Order invariance, duplicate invariance, time invariance with injected now.
- Fuzz the autotagger. Cache its outputs per batch. Gate with a flag. Never block on it.
- Observability defaults. Emit `ruleset_name`, `ruleset_version`, `fired_rules_count`, and `outcome`. Sample per source.
- Canary sources. Pick 1–5% of high-volume sources for early rollout. Alert on delta in `updated_rate`, `noop_rate`, `error_rate`.
- Performance. Stream JSON parsing, avoid deep_dup on hot paths, freeze constants, precompute enums to symbols.
- Concurrency discipline. Single-thread is fine; ensure DB pool and queue workers align. One transaction per item.
- Governance. Require review for rule changes; generate a diff of rule DAG and publish weekly.

Minimal helpers:

```ruby
# deterministic clock

BatchContext = Struct.new(:now, :ruleset_version, :payload_schema_version, keyword_init: true)

# config-as-data

class RuleConfig
  def self.load(path:)
    cfg = YAML.load_file(path)
    OpenStruct.new(
      version: cfg.fetch('version'),
      priorities: cfg.fetch('priorities'),   # {"VariantsRebuildRule"=>100, ...}
      flags: cfg.fetch('flags')              # {"autotag"=>true, ...}
    )
  end
end

# Outcome record:
Outcome = Struct.new(:status, 
                     :reason, 
                     :fired_rules, 
                     :ruleset_version,
                     :payload_version, 
                     :now, 
                     :item_key, 
                     :errors, 
                     keyword_init: true
                     )
```

### Metrics

tags to standardize:

- `service:menu-etl`
- `stage:transform|persist`
- `outcome:noop|updated|created|deleted|rejected`,
- `error:taxonomy`
- `source:<bounded>`
- `ruleset:<name>`
- `ruleset_ver:<n>`.

Dashboards to build:

- Throughput and lag per source.
- No-op vs updated ratio trend.
- Rule fire heatmap (top 10 rules by count and latency).
- Error taxonomy distribution.
- Preload hit rates and sizes.

## Flag Snapshot

Pin a flag snapshot per batch. Read Flipper once. Freeze it. Use that snapshot for all items to keep transforms deterministic.

Version the snapshot. Hash the subset of flags your rules use. Emit `flags_version` on every span, metric, and outcome row.

Declare flag deps per rule. Each rule lists `required_flags`. Fail fast if unknown. Easier audits.

Namespace flags. `etl.* only`. Examples: `etl.autotag, etl`.`enable_variants_v2`, `etl.kill_switch`.

Sticky rollout

- Use Flipper’s `percentage-of` (`:source_id` or `:external_id`). - Always pass the same actor so sampling is stable.

Batch-time stickiness.

- If a flag changes mid-run, ignore until next batch. You already get Slack alerts.
- Cache evaluations. Memoize per batch to avoid hot Flipper calls.
- Record provenance. Store `{ruleset_version, flags_version, fired_rules}` with outcomes.

Minimal wiring:

```ruby
# app/lib/flag_provider.rb

class FlagProvider
  MANIFEST = %w[
    etl.autotag
    etl.variants_v2
    etl.kill_switch
  ].freeze

  Snapshot = Struct.new(:values, :version, keyword_init: true)

  def self.snapshot(context:)
    vals = MANIFEST.to_h { |k| [k, Flipper.enabled?(k, context)] }
    Snapshot.new(values: vals.freeze, version: digest(vals))
  end

  def self.enabled?(snap, key) = snap.values.fetch(key) { raise KeyError, "unknown flag #{key}" }
  
  def self.digest(vals) = Digest::SHA256.hexdigest[vals.sort.to_h.to_json](0,12)
end

# Use it at batch start:
batch_ctx   = BatchContext.new(now: Time.current.freeze, ruleset_version: RULESET_VERSION)

flags_snap  = FlagProvider.snapshot(context: ->(actor){ actor }) # pass actor proc if needed

items.each do |it|
  ctx = MenuItemContext.new(payload: it[:payload], menu_item: it[:menu_item], changed_keys: it[:changed_keys], now: batch_ctx.now)
  ctx.define_singleton_method(:flag?) { |k| FlagProvider.enabled?(flags_snap, k) }
  changes, fired = ruleset.evaluate(ctx)
  persist_one(it, changes, fired, batch_ctx:, flags_version: flags_snap.version)
end

# Rule example with declared deps:
class AutotagRule
  REQUIRED_FLAGS = ['etl.autotag'].freeze
  def name = 'autotag'
  
  def applies?(ctx) = ctx.flag?('etl.autotag') && ctx.payload['tag_names'].present?
  
  def changes(ctx)
    { 'tags' => (AutoTagger.call(text: ctx.payload['tag_names'])&.tags || []) }
  end
end
```

Datadog tags:
Spans:

- `ruleset.version:<RULESET_VERSION>`
- `flags.version:<flags_version>`
- `stage:transform`
- `outcome:*.`

Metrics:

- `flagset:<flags_version>`
- top-level `feature:*` gauges if you need on/off tracking.

Operational tips:

- Keep a manifest file of `etl.*` flags under version control.
- CI checks that every manifest flag exists in Flipper.
- Add a kill switch evaluated first in ruleset. If on, short-circuit to noop and tag `outcome:disabled`.
- Log the flag diff between consecutive batches. Alert if `flags_version` changes during a batch window.
- Avoid flag-driven priority changes at runtime. Gate features, not execution order. Keep priorities in versioned config.

This keeps Flipper in play, but turns it into immutable batch-scoped data with an auditable version.

---

## Config as Data

Keep which rules run, their order, priorities, flags, and parameters in data (YAML/DB). The rule code stays in Ruby. Config points to rules by class name and passes params. The manifest is the “constant reference” plus knobs.

“Rule DAG” = build a dependency graph from rule metadata (declared inputs/outputs/constraints), not from code parsing. Diff the DAG weekly and publish.

Minimal pattern

### Rule contract exposes metadata

```ruby
# every rule class implements
# - name
# - priority
# - metadata: reads, writes, flags, before, after

class VariantsRebuildRule
  def name = "variants_rebuild"
  def priority = @priority
  def initialize(priority: 50, **); @priority = priority; end

  def metadata
    {
      reads:  %w[categories variants price direct_sale_price integrator_metadata],
      writes: %w[variants price],
      flags:  %w[etl.variants_v2],
      before: [],  # explicit ordering constraints (names)
      after:  []   # explicit ordering constraints
    }
  end

# applies?(ctx), changes(ctx)

end
```

Do the same for every rule. Keep metadata pure data.

### Config-as-data manifest

```yaml
version: "2025-08-31"
ruleset: "menu_item_update"
flags_required:
  - etl.kill_switch
rules:
- class: "AutotagRule"
    enabled: true
    priority: 10
    params:
      max_tags: 5
- class: "CategorizationRule"
    enabled: true
    priority: 20
- class: "VariantsRebuildRule"
    enabled: true
    priority: 50
- class: "BrandIdRule"
    enabled: true
    priority: 70
```

Loader:

```ruby
class RulesetConfig
  def self.load(path)
    cfg = YAML.load_file(path)
    rules = cfg['rules'].filter {_1['enabled'] }.map do |r|
      klass = Object.const_get(r['class'])
      inst  = klass.new(**(r['params'] || {}), priority: r['priority'])
      [inst.name, inst]
    end
    OpenStruct.new(version: cfg['version'], rules: rules.to_h)
  end
end
```

Factory:

```ruby
cfg = RulesetConfig.load("config/rulesets/menu_item_update.yml")
rules = cfg.rules.values.sort_by(&:priority)
ruleset = RuleSet.new(rules, conflict: :last_wins, name: cfg.ruleset)
```

This lets you change ordering, enable/disable, and tune params without code deploys.

---

## Full Sketch After Config + DAG conversation

Here’s a compact, end-to-end scaffold. Drop-in structure, pure functions, preload, flags snapshot, Datadog, S3 packs, DAG validation (CI), create/update modes.

```shell
app/
  lib/
    artifact_store.rb
    flag_provider.rb
    tracing.rb
    dag/
      builder.rb
      validate.rb
  services/
    etl/
      pipeline.rb
      preload_refs.rb
      transformers/
        create_transformer.rb
        update_transformer.rb
      rules/
        rule_set.rb
        rule.rb
        ruleset_config.rb
        menu_item_rules.rb
        rules/
          autotag_rule.rb
          categorization_rule.rb
          brand_id_rule.rb
          variants_rebuild_rule.rb
          # add more small rules...
      mappers/
        each_variant_mapper.rb
        weight_variants_mapper.rb
config/
  rulesets/
    menu_item_create.yml
    menu_item_update.yml
lib/artifact_store.rb
```

```ruby
require 'aws-sdk-s3'
class ArtifactStore
  BUCKET = ENV.fetch("ETL_ARTIFACT_BUCKET")
  def self.write!(key:, body:, content_encoding: nil)
    opts = { bucket: BUCKET, key:, body:, content_type: 'application/json' }
    opts[:content_encoding] = content_encoding if content_encoding
    s3.put_object(**opts)
  end
  def self.read(key:) = JSON.parse(s3.get_object(bucket: BUCKET, key: key).body.read)
  def self.key_for(env:, date:, status:, source_id:, external_id:, ingest_id:, ruleset_version:)
    "env=#{env}/date=#{date}/status=#{status}/ruleset=#{ruleset_version}/#{source_id}/#{external_id}/#{ingest_id}.json.gz"
  end
  def self.s3 = (@s3 ||= Aws::S3::Client.new)
end

# lib/flag_provider.rb
class FlagProvider
  MANIFEST = %w[etl.autotag etl.variants_v2 etl.kill_switch].freeze
  Snapshot = Struct.new(:values, :version, keyword_init: true)
  def self.snapshot(actor_key:)
    vals = MANIFEST.to_h { |k| [k, Flipper.enabled?(k, actor_key)] }
    Snapshot.new(values: vals.freeze, version: digest(vals))
  end
  def self.enabled?(snap, key) = snap.values.fetch(key) { raise KeyError, "unknown flag #{key}" }
  def self.digest(vals) = Digest::SHA256.hexdigest[vals.sort.to_h.to_json](0,12)
end

# lib/tracing.rb
module Tracing
  STATSD = Datadog.statsd
  def trace(name, resource: nil, tags: {})
    Datadog::Tracing.trace(name, resource:) do |span|
      tags.each { |k,v| span.set_tag(k, v) }
      yield span
    end
  end
end

# services/etl/rules/rule.rb
class Rule
  attr_reader :name, :priority
  def initialize(name:, priority: 0,**opts)
    @name, @priority, @opts = name, priority, opts
  end
  def metadata = { reads: [], writes: [], flags: [], before: [], after: [] } # override
  def applies?(_ctx) = true            # override
  def changes(_ctx)  = {}              # override
end

# services/etl/rules/rule_set.rb
class RuleSet
  include Tracing
  attr_reader :rules, :name, :conflict
  def initialize(rules, conflict: :last_wins, name: 'default')
    @rules = rules.sort_by { |r| -r.priority }
    @conflict = conflict; @name = name
  end
  def evaluate(ctx)
    result = {}; fired = []
    trace('ruleset.evaluate', resource: name, tags: { 'ruleset.name'=>name }) do
      @rules.each do |r|
        trace('rule.apply', resource: r.name, tags: { 'rule.name'=>r.name, 'rule.pri'=>r.priority }) do
          next unless r.applies?(ctx)
          patch = r.changes(ctx) || {}
          fired << r.name
          result = merge(result, patch)
        end
      end
    end
    [result.freeze, fired.freeze]
  end
  private
  def merge(a,b)
    case conflict
    when :last_wins  then a.merge(b)
    when :first_wins then b.merge(a)
    else a.merge(b)
    end
  end
end

# services/etl/rules/ruleset_config.rb
class RulesetConfig
  def self.load(path)
    cfg = YAML.load_file(path)
    rules = cfg.fetch('rules').filter { |r| r['enabled'] }.map do |r|
      klass = Object.const_get(r.fetch('class'))
      inst  = klass.new(priority: r['priority'] || 0, **(r['params'] || {}))
    end
    OpenStruct.new(version: cfg.fetch('version'), name: cfg.fetch('ruleset'), rules:)
  end
end

#services/etl/rules/menu_item_rules.rb

module MenuItemRules
  def self.for_create(config) = RuleSet.new(config.rules, name: config.name, conflict: :last_wins)
  def self.for_update(config) = RuleSet.new(config.rules, name: config.name, conflict: :last_wins)
end

# services/etl/mappers/each_variant_mapper.rb
class EachVariantMapper
  def call(payload)
    v = MenuItem::Variant.new(
      key: MenuItem::Variant::DEFAULT_EACH_NAME.parameterize,
      price: price_from(payload),
      cart_quantity_multiplier: payload['cart_quantity_multiplier'].to_f.nonzero?,
      compliance_net_mg: payload['compliance_net_mg'],
      compliance_net_precalc: payload.fetch('compliance_net_precalc', false),
      external_id: payload['external_id'],
      inventory_quantity: payload['inventory_quantity']&.clamp(MenuItem::Variant::INVENTORY_QUANTITY_RANGE),
      items_per_pack: payload['items_per_pack'],
      integrator_metadata: payload['integrator_metadata'],
      ratio: payload['ratio']
    )
    v
  end
  private
  def price_from(h)
    amt = h.dig('price','amount').to_f; cur = Crawler::MenuItemPayload::Converter::CURRENCY_ENUM[h.dig('price','currency')]
    return Price.empty if amt.zero? || cur.nil?
    Price.from_amount(amt, cur)
  end
end

# services/etl/mappers/weight_variants_mapper.rb

class WeightVariantsMapper
  MAX = MenuItem::VariantValidator::MAX_VARIANTS
  WE = Crawler::MenuItemPayload::Converter::WEIGHT_ENUM
  def call(payload)
    payload['variants'].to_a.filter_map { build_one(_1, payload) }.sort_by!(&:weight).first(MAX)
  end
  private
  def build_one(p, parent)
    price = price_from(p); weight = weight_from(p)
    return if price.empty? || weight&.value.to_f.zero?
    MenuItem::Variant.new(
      price:, weight:,
      cart_quantity_multiplier: p['cart_quantity_multiplier'].to_f.nonzero?,
      compliance_net_mg: p['compliance_net_mg'],
      compliance_net_precalc: p.fetch('compliance_net_precalc', false),
      external_id: p['external_id'].presence || parent['external_id'],
      inventory_quantity: p['inventory_quantity']&.clamp(MenuItem::Variant::INVENTORY_QUANTITY_RANGE),
      integrator_metadata: p['integrator_metadata'].presence || parent['integrator_metadata']
    )
  end
  def price_from(h)
    amt = h.dig('price','amount').to_f; cur = Crawler::MenuItemPayload::Converter::CURRENCY_ENUM[h.dig('price','currency')]
    return Price.empty if amt.zero? || cur.nil?
    Price.from_amount(amt, cur)
  end
  def weight_from(h)
    unit = WE[h.dig('weight','unit')]; val = h.dig('weight','value').to_f; return unless unit && val.nonzero?
    provided = Weight.new(val, unit)
    case provided.in_grams.value.to_f
    when 3.5 then Weight.new(0.125, :ounce)
    when 7.0 then Weight.new(0.25, :ounce)
    when 14.0 then Weight.new(0.5, :ounce)
    when 28.0 then Weight.new(1, :ounce)
    else provided
    end
  end
end

# services/etl/rules/rules/autotag_rule.rb

class AutotagRule < Rule
  def initialize(priority: 10, max_tags: 5,**)
    super(name: 'autotag', priority:)
    @max = max_tags
  end

  def metadata = { reads: %w[name category_names], writes: %w[tags], flags: %w[etl.autotag] }

  def applies?(ctx)
    return unless ctx.flag_enabled?('etl.autotag')
    
    (ctx.payload['name'] || ctx.payload['category_names']).present?
  end

  def changes(ctx)
    text = "#{ctx.payload['name']} #{Array(ctx.payload['category_names']).join(' ')}"
    tags = AutoTagger.call(text:)&.tags || []
    
    { 'tags' => tags.uniq.first(@max) }
  end
end

# services/etl/rules/rules/categorization_rule.rb
class CategorizationRule < Rule
  def initialize(priority: 20, **); super(name: 'categorization', priority:); end
  def metadata = { reads: %w[category_names name brand_name], writes: %w[categories] }
  def changes(ctx)
    cats = ExternalItemCategorizerService.call(
      category_names: Array(ctx.payload['category_names']).filter(&:presence),
      name: ctx.payload['name'], brand_name: ctx.payload['brand_name']
    ).categories
    { 'categories' => cats }
  end
end

# services/etl/rules/rules/brand_id_rule.rb

class BrandIdRule < Rule
  def initialize(priority: 70,**); super(name: 'brand_id', priority:); end
  def metadata = { reads: %w[brand_id], writes: %w[brand_id] }
  def applies?(ctx) = ctx.payload.key?('brand_id')
  def changes(ctx)
    id = ctx.payload['brand_id']
    ctx.brand_lookup.call(id) ? { 'brand_id'=>id } : {}
  end
end

# services/etl/rules/rules/variants_rebuild_rule.rb
class VariantsRebuildRule < Rule
  VARIANT_TRIGGERS = (MenuItem::MIGRATED_EACH_VARIANT_ATTRIBUTES[MenuItem.name] + %w[categories direct_sale_price price integrator_metadata variants]).uniq.freeze
  def initialize(priority: 50, each_mapper: EachVariantMapper.new, weight_mapper: WeightVariantsMapper.new, **)
    super(name: 'variants_rebuild', priority:); @each = each_mapper; @weight = weight_mapper
  end
  def metadata = { reads: %w[categories variants price direct_sale_price integrator_metadata], writes: %w[variants price] }
  def applies?(ctx) = (Array(ctx.changed_keys) & VARIANT_TRIGGERS).any? || ctx.changed_keys == :all
  def changes(ctx)
    sold_by_weight = (assigned_category(ctx)&.sold_by_weight?) && ctx.payload['variants'].to_a.any?
    if sold_by_weight
      { 'variants' => @weight.call(ctx.payload), 'price' => Price.empty }
    else
      v = @each.call(ctx.payload)
      { 'variants' => [v], 'price' => v.price }
    end
  end
  private
  def assigned_category(ctx)
    ctx.assigned_category || (ctx.menu_item ? DeprecatedRootCategoryService.call(ctx.menu_item) : nil)
  end
end

# services/etl/preload_refs.rb
class PreloadRefs
  def initialize(brands:, strains:, tags:)
    @brands, @strains, @tags = brands, strains, tags
  end
  def call(items)
    brand_ids = items.filter_map {_1[:payload]['brand_id'].presence }.uniq
    strain_names = items.filter_map {_1[:payload]['strain_name'].presence }.uniq
    tag_names = items.flat_map { Array(_1[:payload]['tag_names']) }.compact.uniq
    {
      brands:  @brands.by_ids(brand_ids),         # {id=>true}
      strains: @strains.by_names(strain_names),   # {name=>id}
      tags:    @tags.by_names(tag_names)          # {name=>Tag}
    }
  end
end

# services/etl/transformers/create_transformer.rb
MenuItemContext = Struct.new(:payload, :menu_item, :changed_keys, :assigned_category, :now, :flag?, :brand_lookup, :strain_lookup, :tag_lookup, keyword_init: true)

class CreateTransformer
  def initialize(ruleset:) = (@ruleset = ruleset)
  def call(payload:, lookups:, flags_snap:, now:)
    ctx = MenuItemContext.new(
      payload:, menu_item: nil, changed_keys: :all, assigned_category: nil, now:,
      flag?: ->(k){ FlagProvider.enabled?(flags_snap, k) },
      brand_lookup: ->(id){ lookups[:brands][id] },
      strain_lookup: ->(n){ lookups[:strains][n] },
      tag_lookup: ->(n){ lookups[:tags][n] }
    )
    @ruleset.evaluate(ctx)
  end
end
# services/etl/transformers/update_transformer.rb
class UpdateTransformer
  def initialize(ruleset:) = (@ruleset = ruleset)
  def call(payload:, menu_item:, changed_keys:, lookups:, flags_snap:, now:)
    ctx = MenuItemContext.new(
      payload:, menu_item:, changed_keys: Array(changed_keys), assigned_category: nil, now:,
      flag?: ->(k){ FlagProvider.enabled?(flags_snap, k) },
      brand_lookup: ->(id){ lookups[:brands][id] },
      strain_lookup: ->(n){ lookups[:strains][n] },
      tag_lookup: ->(n){ lookups[:tags][n] }
    )
    @ruleset.evaluate(ctx)
  end
end

# services/etl/pipeline.rb
class Pipeline
  include Tracing
  def initialize(create_cfg_path:, update_cfg_path:, repos:)
    @create_rules = MenuItemRules.for_create(RulesetConfig.load(create_cfg_path))
    @update_rules = MenuItemRules.for_update(RulesetConfig.load(update_cfg_path))
    @repos = repos
  end

  def run(items, env:, source_id:, batch_now: Time.current)
    flags = FlagProvider.snapshot(actor_key: source_id)
    lookups = PreloadRefs.new(**@repos).call(items)

    items.map do |it|
      trace('etl.item', resource: 'menu_item', tags: { 'env'=>env, 'op'=>it[:menu_item] ? 'update' : 'create' }) do
        changes, fired = transformer_for(it).call(
          payload: it[:payload], menu_item: it[:menu_item], changed_keys: it[:changed_keys],
          lookups:, flags_snap: flags, now: batch_now
        )
        status = persist(it, changes)
        write_pack(env:, now: batch_now, it:, changes:, fired:, status:, flags_version: flags.version)
        { id: it[:payload]['external_id'], status:, fired_rules: fired }
      end
    end
  end

  private

  def transformer_for(it)
    it[:menu_item] ? UpdateTransformer.new(ruleset: @update_rules) : CreateTransformer.new(ruleset: @create_rules)
  end

  def persist(it, changes)
    return :noop if changes.empty?
    if it[:menu_item]
      it[:menu_item].update!(changes); :updated
    else
      MenuItem.create!(changes.merge(external_id: it[:payload]['external_id'])); :created
    end
  rescue ActiveRecord::RecordInvalid
    :rejected
  end

  def write_pack(env:, now:, it:, changes:, fired:, status:, flags_version:)
    pack = {
      pack_version: 1, produced_at: now.to_i, env:, ruleset_version: 'v1',
      flags_version:, source_id: it[:source_id], external_id: it[:payload]['external_id'],
      ingest_id: it[:ingest_id], status:, fired_rules: fired, mapped_payload: it[:payload], changes:
    }
    body = StringIO.new.tap { |s| gz = Zlib::GzipWriter.new(s); gz.write(JSON.dump(pack)); gz.close }.string
    key = ArtifactStore.key_for(env:, date: now.strftime('%F'), status:, source_id: it[:source_id],
                                external_id: it[:payload]['external_id'], ingest_id: it[:ingest_id], ruleset_version: 'v1')
    ArtifactStore.write!(key:, body:, content_encoding: 'gzip')
  end
end
```

config/rulesets/menu_item_update.yml (example)

```yaml
version: "2025-08-31"
ruleset: "menu_item_update"
rules:

- class: "AutotagRule"
    enabled: true
    priority: 10
    params: { max_tags: 5 }
- class: "CategorizationRule"
    enabled: true
    priority: 20
- class: "VariantsRebuildRule"
    enabled: true
    priority: 50
- class: "BrandIdRule"
    enabled: true
    priority: 70
```

lib/dag/builder.rb (CI use)

```ruby
module DAG
  Node = Struct.new(:name, :reads, :writes, :before, :after)
  def self.build(rules)
    nodes = rules.map { |r| m=r.metadata; Node.new(r.name, m[:reads]||[], m[:writes]||[], m[:before]||[], m[:after]||[]) }
    edges = []
    nodes.combination(2) do |a,b|
      edges << [a.name,b.name,:data] if (a.writes & b.reads).any?
      edges << [b.name,a.name,:data] if (b.writes & a.reads).any?
    end
    nodes.each do |n|
      n.before.each { |b| edges << [n.name,b,:explicit] }
      n.after.each  { |a| edges << [a,n.name,:explicit] }
    end
    { nodes:, edges: edges.uniq }
  end
end

# lib/dag/validate.rb (CI use)
module DAG::Validate
  def self.acyclic!(dag)
    indeg = Hash.new(0); adj = Hash.new { |h,k| h[k]=[] }
    dag[:edges].each { |a,b,_| adj[a] << b; indeg[b] += 1; indeg[a] ||= 0 }
    q = dag[:nodes].map(&:name).select { |n| indeg[n].zero? }
    out = []
    until q.empty?
      u = q.shift; out << u
      adj[u].each { |v| indeg[v]-=1; q << v if indeg[v].zero? }
    end
    raise "DAG cycle" unless out.size == dag[:nodes].size
  end
  def self.write_conflicts!(rules)
    pairs = rules.combination(2).select { |a,b| (a.metadata[:writes] & b.metadata[:writes]).any? }
    raise "Write conflicts: #{pairs.map{ |a,b| "#{a.name}<->#{b.name}" }.join(', ')}" if pairs.any?
  end
end
```

This is the whole path: manifest-driven rules, preload lookups, batch flags snapshot, pure evaluation, persistence, artifacts, tracing, and CI DAG checks.
