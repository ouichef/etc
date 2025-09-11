class Rule
  attr_reader :meta

  def initialize(meta)
    @meta = meta
  end

  def name = meta.name
  def priority = meta.priority
  def reads = meta.reads
  def writes = meta.writes
  def before = meta.before
  def after = meta.after

  def applies?(_ctx) = raise NotImplementedError
  def apply(_ctx) = raise NotImplementedError
end

class RuleMeta < Dry::Struct
  attribute :name, Types::String
  attribute :priority, Types::Integer
  attribute :reads, Types::Array.of(Types::String)
  attribute :writes, Types::Array.of(Types::String)
  attribute :before, Types::Array.of(Types::String).default([].freeze)
  attribute :after, Types::Array.of(Types::String).default([].freeze)
end
class BrandNameRule < Rule
  def initialize
    super RuleMeta.new(
            name: "brand-name-rule",
            priority: 1,
            reads: %w[brand_name],
            writes: %w[brand_name]
          )
  end

  def applies?(ctx)
    ctx.payload.key?(:brand_name)
  end

  def apply(ctx)
    ctx => { payload: { brand_name:, brand_id: }, brand_lookup: }

    id = brand_lookup.call(brand_name)
    return {} if id == brand_id || id.nil?

    { brand_id: id }
  end
end

class CreateActionRule < Rule
  def initialize
    super RuleMeta.new(
            name: "create-action-rule",
            priority: 99,
            flags: %i[crawler_set_action],
            reads: %w[menu_item],
            writes: %w[action, processor]
          )
  end

  def applies?(ctx)
    ctx => { flags:, menu_item: }

    flags[:crawler_set_action] && menu_item.blank?
  end

  def apply(ctx)
    ctx.with(action: :create, processor: Processors::CreateItem)
  end
end

class DestroyActionRule < Rule
  def initialize
    super RuleMeta.new(
            name: "destroy-action-rule",
            priority: 99,
            flags: %i[crawler_set_action],
            reads: %w[menu_item],
            writes: %w[action, processor]
          )
  end

  def applies?(ctx)
    ctx => { flags:, menu_item:, destroy_pointer:, payload: }

    flags[:crawler_set_action] && menu_item.present? &&
      destroy_pointer.call(payload)
  end

  def apply(ctx)
    ctx.with(action: :destroy, processor: Processors::DestroyItem)
  end
end

class UpdateActionRule < Rule
  def initialize
    super RuleMeta.new(
            name: "update-action-rule",
            priority: 99,
            flags: %i[crawler_set_action],
            reads: %w[menu_item],
            writes: %w[action, processor]
          )
  end

  def applies?(ctx)
    ctx => { flags:, menu_item:, destroy_pointer:, payload: }
    marked = destroy_pointer.call(payload)
    flags[:crawler_set_action] && menu_item.present? && !marked
  end

  def apply(ctx)
    ctx.with(action: :destroy, processor: Processors::DestroyItem)
  end
end

class Contracts::RawPayload::Treez < Dry::Validation::Contract
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

class Canonical::MenuItem < Dry::Struct
  attribute :external_id, Types::String
  attribute :name, Types::String
  attribute :brand_name, Types::String.optional
  attribute :strain_name, Types::String.optional
  attribute :tags, Types::Array.of(Types::String).default([].freeze)
  attribute :price_cents, Types::Integer.optional
  attribute :status, Types::String.enum("active", "inactive")
end
