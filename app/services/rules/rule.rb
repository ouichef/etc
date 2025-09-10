module Rules
  class Rule
    extend Dry::Initializer
    option :meta, type: RuleMeta

    def name = meta.name
    def priority = meta.priority
    def reads = meta.reads
    def writes = meta.writes
    def before = meta.before
    def after = meta.after

    def applies?(_ctx) = raise NotImplementedError
    def apply(_ctx) = raise NotImplementedError
  end
end
