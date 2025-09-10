module Ingest
  class RuleMeta < Dry::Struct
    attribute :name, Types::String
    attribute :priority, Types::Integer
    attribute :reads, Types::Array.of(Types::String)
    attribute :writes, Types::Array.of(Types::String)
    attribute :before, Types::Array.of(Types::String).default([].freeze)
    attribute :after, Types::Array.of(Types::String).default([].freeze)
  end
end
