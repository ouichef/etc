module Rules
  module Config
    module T
      include Dry.Types()
      SymbolArray = Types::Array.of(Types::Symbol)
      StringArray = Types::Array.of(Types::String)
    end

    class Publishability < Dry::Struct
      attribute :silent_default, T::SymbolArray.default([].freeze)
      attribute :silent_sources,
                T::Hash.map(T::Symbol, T::SymbolArray).default({}.freeze)

      def silent_keys_for(source)
        (silent_default + (silent_sources[source.to_sym] || [])).uniq.freeze
      end
    end

    class References < Dry::Struct
      attribute :mode, T::String.enum("lookup_only")
      attribute :default_action, T::String.enum("reject", "ignore")
      attribute :normalize, T::Hash
      attribute :source_actions, T::Hash.default({}.freeze)

      def action_for(source:, kind:)
        source_actions.dig(source.to_s, kind.to_s) || default_action
      end

      def norm(kind, name)
        s = name.to_s
        cfg = normalize[kind.to_s] || {}
        s = s.strip if cfg["strip"]
        s = s.downcase if cfg["case"] == "downcase"
        s = s.split.map(&:capitalize).join(" ") if cfg["case"] == "title"
        s
      end
    end

    class Root < Dry::Struct
      attribute :ruleset_version, T::String
      attribute :flags, T::Hash
      attribute :filtering, T::Hash
      attribute :mapping, T::Hash
      attribute :publishability, Publishability
      attribute :deletes, T::Hash
      attribute :references, References
      attribute :observability, T::Hash

      def self.load(path = "config/ingest.yml")
        raw = YAML.load_file(path)
        new(
          ruleset_version: raw.dig("ruleset", "version"),
          flags: raw["flags"] || {},
          filtering: raw["filtering"] || {},
          mapping: raw["mapping"] || {},
          publishability:
            Publishability.new(
              silent_default:
                (
                  raw.dig("publishability", "silent_attributes", "default") ||
                    []
                ).map!(&:to_sym),
              silent_sources:
                (
                  raw.dig("publishability", "silent_attributes", "sources") ||
                    {}
                )
                  .transform_keys!(&:to_sym)
                  .transform_values! { |arr| arr.map!(&:to_sym) }
            ),
          deletes: raw["deletes"] || {},
          references:
            References.new(
              mode: raw.dig("references", "mode"),
              default_action: raw.dig("references", "default_action"),
              normalize: raw.dig("references", "normalize") || {},
              source_actions: raw.dig("references", "sources") || {}
            ),
          observability: raw["observability"] || {}
        )
      end
    end
  end
end
