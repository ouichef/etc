module Ingest
  module Transformer
    module Canonical
      module MenuItem
        class BrandNameRule < Rule
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
      end
    end
  end
end
