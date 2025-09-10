# Setting up the Integration MenuSync Pipeline

## Configuration

On boot we need to do some upfront configuration of the Pipeline so that we can just run the items through it.

### Compile Rulesets

We can load the configuration file and compile the ruleset for update + create (and maybe partner specific ones?)

```ruby
MENU_SYNC_PIPELINE = Integration::MenuSync::Pipeline::Builder.for(:menu_sync)
```

```ruby
class Integration::MenuSync::Pipeline::Builder
  PERMITTED_ACTIONS = %i[menu_sync].freeze

  def self.for(action)
    send(action) if action.present? && PERMITTED_ACTIONS.include(action)
  end

  private

  def self.menu_sync
    new(
      create_tx: Integration::MenuSync::Transformer.for(:create),
      update_tx: Integration::MenuSync::Transformer.for(:update),
      resolvers: assoc_resolvers(:menu_sync)
    )
  end

  def self.assoc_resolvers(action)
    Integration::AssociationResolver.for(action)
  end
end


class Integration::MenuSync::TransformerBuilder
    def self.for(action)
        send(:"#{action}_transformer")
    end

    private

    def self.create_transformer
        ruleset = Rules::Set::Configure(:menu_item_create)
        Integration::MenuSync::CreateService.new(ruleset:)
    end

    def self.update_transformer
        ruleset = Rules::Set::Configure(:menu_item_update)
        Integration::MenuSync::UpdateService.new(ruleset:)
    end
end


```

Load + Compile

```ruby
# config/menu_sync/ruleset.yml
shared:
  version: "2025-08-31"
  rules:
    - name: SharedRule
      key: some-shared-rule
      description: "This rule will always run"
      priority: 5
      reads:
        - name
      writes:
        - name
      before: []
      after: []
update:
  version: "2025-08-31"
  rules:
    - name: UpdateOnlyRule
      key: update-only-rule
      description: "This rule is only for a menu item going through an update"
      priority: 100
      reads:
        - category_names
      writes:
        - categories
      before: []
      after: []
create:
  version: "2025-08-31"
  rules:
    - name: CreateOnlyRule
      key: create-only-rule
      description: "This rule is only for a menu item being created"
      priority: 10
      reads:
        - strain_name
      writes:
        - strain_id
      before: []
      after: []
```

```ruby

class Rules::Set::Configure
  PATH = {
    menu_sync: "config/menu_sync/ruleset.yaml",
    discount_sync: "config/discount_sync/ruleset.yaml"
  }.freeze

  PERMITTED_ACTIONS = %i[menu_sync_create menu_sync_update].freeze
  def self.for(ruleset_name)
    if PERMITTED_ACTIONS.include(ruleset_name)
      send(ruleset_name)
    else
      "No ruleset found for: #{ruleset_name}"
    end
  end

  private

  def self.load_config(key)
    YAML.load_file(PATH, symbolize_names: true)
  end

  def self.menu_sync_create
    load_config(:menu_sync) => {
      rulesets: { shared: { version: shared_version, rules: shared_rules}
        create: { version: create_version, rules: create_rules } }
    }
    versions = create_version + shared_version
    rules = create_rules + shared_rules
    compile_ruleset(versions: , rules:)
  end

  def self.menu_sync_update
    load_config(:menu_sync) => {
      rulesets: { shared: { version: shared_version, rules: shared_rules}
        update: { version: update_version, rules: update_rules } }
    }
    versions = update_version + shared_version
    rules = create_rules + shared_rules
    compile_ruleset(versions: , rules:)
  end

  def self.compile_ruleset(name:, versions:, rules:)
    rules =
      rules.map do |rule|
        meta = RuleMeta.new(rule)
        klass = Object.const_get(meta.name)
        klass.new(meta:)
      end

    RuleSet.compile(versions:, rules:)
  end
end

```

## Pipeline

```ruby

module Integration
  module MenuSync
    class Pipeline
      extend Dry::Initializer

      option :create_tx, reader: :private
      option :update_tx, reader: :private
      option :resolver, reader: :private
      option :env, reader: :private

      def call(item_ctxs, source_id:)
        resolver = resolver.preload(item_ctxs:, source_id:)
        sync_ctx = build_menu_sync_context(source: resolver.source)
          

        update = UpdateProcessor.new(sync_ctx:, update_tx:)
        create = CreateProcessor.new(sync_ctx:, create_tx:)

        item_ctxs.map do |item_ctx|

            menu_item, resolved_associations = resolver.call(item_ctx)
            item_ctx = item_ctx.with(menu_item:, resolved_associations:)
            menu_item.present? ? update.call(item_ctx) : create.call(item_ctx)
        end
      end

      private

      def build_menu_sync_context(source:)
      flags_snap = FlagProvider.snapshot(actor_key: source.id, namespace: :menu_sync)
      
      MenuSync::Context.new(
            now: Time.current.freeze,
            flags_snap:,
            env:,
            source:
            raw_payload_contract: Integration::MenuSync::RawPayloadContract.for(source.title)
          )
      end
    end
  end
end

module Integration
  
  module MenuSync
    class Processor
        extend Dry::Initializer
        option :menu_sync_ctx
        option :source, proc { menu_sync_context.source }
        option :transformed_contract,
             default: -> { CanonicalContract.new },
             reader: :private
        
        def run(item_ctx)
            raise "No STEPS defined" unless Object.const_defined?(STEPS)

            STEPS.each_with_object(item_ctx) do |ctx, step|
              ctx = send(step)
              return result(item_ctx) unless item_ctx.violations.empty?
              ctx
            end
        end
    end

    class CreateProcessor < Processor
      option :raw_payload_contract, default: proc { Integration::MenuSync::RawPayloadContract.for(source: source.title)}

      STEPS = [:validate_params, :transform, :validate_transformed].freeze

      def call(item_ctx:)
        run(item_ctx)
      end

      def transform(item_ctx)
          changes, fired = create_tx.call(
            payload:,
            lookups:,
            flags_snap:,
            now: 
          )
        
        item_ctx.with(changes:, fired:)
      end

      def validate_params(item_ctx)
        fired = [:raw_validation]
        validated = raw_payload_contract.call(item_ctx.payload)

        if validated.success?
          item_ctx.with(status: :processing, fired:)
        else
          violations = validated.errors.to_h
          item_ctx.with(status: :rejected, fired:, violations:)
        end
      end
    end

    class UpdateProcessor < Processor
      option :raw_payload_contract, default: proc { Integration::MenuSync::RawPayloadContract.for(source: source.title)}

      STEPS = [:validate_params, :transform, :validate_transformed].freeze

      def call(item_ctx:)
        run(item_ctx)
      end

      def transform(item_ctx)
      changed_keys = 
        changes, fired = update_tx.call(
            payload:,
            menu_item:,
            changed_keys: Changeset.diff(item_ctx, menu_item),
            lookups:,
            flags_snap:,
            now:
          )

        item_ctx.with(changes:, fired:)
      end

      def validate_params(item_ctx)
        fired = ["raw-validation"]
        validated = raw_payload_contract.call(item_ctx.payload)

        if validated.success?
          item_ctx.with(status: :processing, fired:)
        else
          violations = validated.errors.to_h
          item_ctx.with(status: :rejected, fired:, violations:)
        end
      end
    
      def result(item)
        item => { external_id: id, step_status: status, fired_rules:, violations: }
        
        { id:, status:, fired_rules:, violations: }
      end
    end
  end
end
```

We receive an external payload from either:

- crawl
- webhook

We verify if it is associated to an integrated menu.
We trim the payload to just the fields that we require.

We wrap the payload into an `MenuSync::Context` and send it off to work.

```ruby
module Integration
  module MenuSync
    class Context < Dry::Struct
      attribute :source
      attribute :now,            Types::Time
      attribute :flag_snapshot,  Types::Hash.map(Types::Symbol, Types::Bool)
      attribute :resolvers,       Types::Hash.symbolized  # {brands_by_key:, strains_by_key:, tags_by_key:}
      def flag?(key) = flag_snapshot[key.to_sym] == true
    end
  end
end
```

### Filter

We verify that we have the required fields by leveraging a partner specific `RawPayloadContract`.

```ruby
class Contract::Treez::RawPayload < Dry::Validation::Contract
  params do
    required(:external_id).filled(:string)
    required(:name).filled(:string)
    optional(:brand).maybe(:string)
    optional(:strain).maybe(:string)
    optional(:tags).array(:string)
    optional(:price_cents).maybe(:integer)
    optional(:status).filled(:string, included_in?: %w[active inactive])
  end
end
```

We deduplicate items by their `external_id`, i.e. `items.uniq(&:external_id)`

### Map

We transform + map the values from the `RawPayload`.

### Transform

We run the `ItemContext` through the transform service depending on if it is being updated or created.

Each rule that is fired returns a `MenuItem` fragment. These fragments will reduce together to become the `ItemPayload`.

These rules can take virtual attributes from the context, resolve associations, transform or normalize values, or react to changing circumstances (such as a category change affecting variants).

The rulesets are different for create and update because some circumstances will never happen for a new menu item, so we can properly reflect our merchandising rules on update actions and do routine rules for creates.

### Validate

We validate the `ItemPayload` against a `CanonicalMenuItemContract`. This will include checking things like prohibited content, that the categories are in the right taxonomy, and that all the required fields for create/update are present.

### Persist

If the menu item is being created it is a simple create call.

If the menu item is being updated, we have to determine if the changed attributes are entirely silently updated attributes.

If they are silent? leverage `#update_columns` if not, leverage `#update`.

### Observe

This is where we want to make sure we are tagging our traces, collecting metrics, and persisting the `integration_menu_item_sync` records.

We are going to persist these attributes from the `IngestContext` and the `ItemContext`

```ruby
{
  raw_payload_normalized: normalized_raw,
  mapped_payload: mapped,
  changed_keys: changed_keys,
  batch_ctx: { 
    now: batch_now.to_i,
    ruleset_versions:,
    flags_version: flags.version 
  },
  flags_snapshot: flags.values,
  resolver_snapshot: { 
    brands: brand_map, 
    strains: strain_map, 
    tags: tag_map 
  },
  rules_order: ruleset.ordered_keys,
  outcome: { status:, violations:, fired_rules: }
}
```
