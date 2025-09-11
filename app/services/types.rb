module Types
  include Dry.Types()
  Time = Types::Params::Time
  SymbolSet =
    Types::Array.of(Types::Symbol).constructor { |a| a.to_a.uniq.freeze }
  SymbolArray = Types::Array.of(Types::Symbol)
  StringArray = Types::Array.of(Types::String)
end
