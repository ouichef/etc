require "tsort"
require "dry-types"
require "dry-initializer"
require "dry-struct"
require "yaml"

module Types
  include Dry.Types()
  Time = Types::Params::Time
  SymbolSet =
    Types::Array.of(Types::Symbol).constructor { |a| a.to_a.uniq.freeze }
end

MENU_SYNC_PIPELINE = Integration::MenuSync.build_pipeline

# At call site
menu_sync_ctx = Integration::MenuSync.build_pipeline_ctx_for(source:)
MENU_SYNC_PIPELINE.call(menu_sync_ctx)
