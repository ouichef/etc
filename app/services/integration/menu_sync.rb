module Integration
  module MenuSync
    def self.build_pipeline
      Integration::Ingest::Pipeline.new(
        filter: ->(items) { items.uniq(&:external_id) },
        normalized_payload_contract:
          Integration::MenuSync::Registry.contract_for(:canonical),
        updater: Integration::MenuSync::UpdateService, # contains ruleset for update, and persistence.
        creator: Integration::MenuSync::CreateService, # contains ruleset for create, and persistence.
        destroyer: Integration::MenuSync::DestroyService, # contains ruleset for destroy, and persistence.
        reporter: Integration::MenuSync::ReportService
      )
    end

    # ctx: resolvers, flags, time
    # raw_payload_contract: wrapped dry-validation
    # external_tx: this ruleset will map and transform and mark the action.
    def self.build_pipeline_ctx_for(source:)
      flags_snap = Flag.snapshot(:menu_sync)

      raw_payload_contract =
        Integration::MenuSync::Registry.contract_for(source:)

      external_tx = Integration::MenuSync::Registry.external_tx_for(source:)

      Integration::MenuSync::Context.new(
        now: Time.current.freeze,
        flags_snap:,
        env: Rails.env,
        source:,
        raw_payload_contract:,
        external_tx:
      )
    end
  end
end
