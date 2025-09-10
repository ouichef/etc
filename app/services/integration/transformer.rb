module Integration
  module Transformer
    RULESET_MANIFEST = {
      canonical_menu_item:
        Integration::Transformer::Canonical::MenuItem.for(:canonical)
    }.freeze

    def self.for(action:)
      RULESET_MANIFEST.key?(action) ? RULESET_MANIFEST[action] : nil
    end
  end
end
