module Ingest
  class NormalizedMenuItem < Dry::Struct
    attribute :external_id, Types::String
    attribute :name, Types::String
    attribute :brand_name, Types::String.optional
    attribute :strain_name, Types::String.optional
    attribute :tags, Types::Array.of(Types::String).default([].freeze)
    attribute :price_cents, Types::Integer.optional
    attribute :status, Types::String.enum("active", "inactive")
  end
end
