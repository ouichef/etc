module Integration
  module Ingest
    class Pipeline
      extend Dry::Initializer

      # These options are preloaded on boot. The updater, creator, and destroyer are service classes that wrap rulesets and persistence and return an item_ctx
      options :filter # could be as simple as a proc that just uniques by external_id
      option :normalized_payload_contract #dry-validation
      option :updater # will run on action: :update
      option :creator # will run on action: :create
      option :destroyer # will run on action: :destroy
      option :reporter

      # Item contexts have helper predicates valid? + invalid?
      # Contracts must be wrapped to return an item_context with any errors populated
      # in the violations section.
      def call(items_ctxs, ctx:)
        ctx => { external_tx:, raw_payload_contract: }
        filtered_items = filter.call(item_ctx)

        processed_items =
          filtered_items.map do |item_ctx|
            item_ctx = raw_payload_contract.call(item_ctx)
            return result(item_ctx) if item_ctx.invalid?

            # The external transformer will set the item context `:action` attribute.
            # This will be based on an ActionRule defined in a integration ruleset.
            item_ctx = external_tx.call(item_ctx)
            return result(item_ctx) if item_ctx.invalid?

            item_ctx = process_one(item_ctx, ctx:)
            result(item_ctx)
          end

        # This class represents any additional tagging, metrics, and result persistence
        reporter.call(processed_items)

        processed_items
      end

      def process_one(item_ctx, ctx)
        processed_item_ctx =
          case item_ctx
          in action: :create
            creator.call(item_ctx:, ctx:)
          in action: :update
            updater.call(item_ctx:, ctx:)
          in action: :destroy
            destroyer.call(item_ctx:, ctx:)
          end

        processed_item_ctx
      end

      def failed_result(item_ctx)
        item_ctx => {
          menu_item:, external_id:, status:, fired_rules:, violations:
        }

        {
          external_id:,
          menu_item_id: menu_item.id,
          status:,
          fired_rules:,
          violations:
        }
      end
    end
  end
end
