module Integration
  module MenuSync
    class Registry
      CONTRACT_MANIFEST = {
        treez: Integration::MenuSync::Contracts::Treez
      }.freeze
      def self.contract_for(source:)
        CONTRACT_MANIFEST[source] if CONTRACT_MANIFEST.key?(source)
      end

      EXTERNAL_TX_MANIFEST = {
        treez: Integration::MenuSync::Transformers::Treez
      }.freeze
      def self.external_tx_for(source:)
        EXTERNAL_TX_MANIFEST[source] if EXTERNAL_TX_MANIFEST.key?(source)
      end
    end
  end
end
