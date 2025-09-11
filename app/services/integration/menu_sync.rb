module Integration
  module MenuSync
    class MenuItemContract < Dry::Validation::Contract
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

    class CreateItem
    end
    class UpdateItem
    end

    class DestroyItem
    end

    # Each one of these will compile their specific ruleset on boot.
    # These encapsulate the canonical menu item transformation and persistence paths.
    # The rulesets will perform the last transformation, then the MenuItemContract will perform the last validations (before rails hooks that is)
    # Then they use ActiveRecord to create, update, or (soft) destroy the item.
    UPDATE_TX = UpdateItem.compile!
    CREATE_TX = CreateItem.compile!

    def self.build_pipeline
      Integration::Ingest::Pipeline.new(
        filter: ->(items) { items.uniq(&:external_id) },
        canonical_menu_item_contract: MenuItemContract,
        update_tx: UPDATE_TX,
        create_tx: CREATE_TX,
        destroyer: DestroyItem,
        reporter: Integration::MenuSync::ReportSync
      )
    end

    # ctx: resolvers, flags, time
    # raw_payload_contract: wrapped dry-validation
    # external_tx: this ruleset will map and transform and mark the action.
    def self.build_pipeline_ctx_for(source:)
      flags_snap = Flag.snapshot(:menu_sync)

      Integration::MenuSync::Context.new(
        now: Time.current.freeze,
        flags_snap:,
        env: Rails.env,
        source:,
        raw_payload_contract: source.raw_payload_contract,
        external_tx: source.external_tx
      )
    end
  end
end
