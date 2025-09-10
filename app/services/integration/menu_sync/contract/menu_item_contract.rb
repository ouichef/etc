module Ingest
  module Contracts
    class MenuItem < Dry::Validation::Contract
      params do
        required(:external_id).filled(:string)
        required(:name).filled(:string)
        optional(:brand_id).maybe(:integer)
        optional(:strain_id).maybe(:integer)
        optional(:tag_ids).array(:integer)
        optional(:price_cents).maybe(:integer, gt?: 0)
        required(:status).filled(:string, included_in?: %w[active inactive])
      end
    end
  end
end
