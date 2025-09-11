class Integration::MenuSync::Webhook::Treez
  class RawPayloadContract < Dry::Validation::Contract
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

  EXTERNAL_TX = TreezTransformer.compile!
  def external_tx
  end
end
