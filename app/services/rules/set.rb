class Rules::Set
  extend Dry::Initializer
  include TSort

  option :rules, type: Types::Array
  option :version, Types::String
  option :conflict, type: Types::Symbol, default: proc { :last_wins }
  option :ordered_keys, proc { compute_order.freeze }

  def self.compile(rules:, version:, conflict:)
    rs = new(rules:, version:, conflict:)
    rs.validate!
    rs.freeze
  end

  # runtime: O(n) walk, no tsort calls
  def evaluate(ctx)
    changes = {}
    fired = []

    ordered_keys.each do |key|
      r = rules[key]
      res = r.call(ctx)
      next unless res[:fired]

      overlap = (r.writes & changes.keys)
      raise "write conflict: #{r.key} -> #{overlap.inspect}" if overlap.any?

      changes.merge!(res[:changes])
      fired << r.key
      ctx = ctx.with(changed_keys: (ctx.changed_keys + r.writes).uniq.freeze)
    end

    [changes.freeze, fired.freeze]
  end

  # --- CI/CD validation helpers ---
  def validate!
    detect_cycles!
    conflicts = detect_write_conflicts
    raise "write conflicts: #{conflicts.inspect}" unless conflicts.empty?
    self
  end

  def detect_write_conflicts
    rules
      .values
      .combination(2)
      .filter_map do |a, b|
        ov = (a.writes & b.writes)
        ov.empty? ? nil : [a.key, b.key, ov]
      end
  end

  # edges for visualization
  def graph_edges
    build_edges
  end

  # --- TSort hooks built from metadata ---
  def tsort_each_node(&blk) = rules.each_key(&blk)
  def tsort_each_child(key, &blk)
    edges = build_edges
    edges[key].each(&blk)
  end

  private

  def merge(a, b)
    conflict == :first_wins ? b.merge(a) : a.merge(b)
  end

  # Combine before/after into edges, add stable tie-break by priority then key
  def build_edges
    @edges ||=
      begin
        h = Hash.new { |m, k| m[k] = Set.new }
        rules.each_value do |r|
          # r.before: run current before targets => edge: current -> target
          r.before.each { |t| h[r.key] << t }
          # r.after: run current after deps => edge: dep -> current
          r.after.each { |d| h[d] << r.key }
        end
        h.each_value(&:freeze)
        h.freeze
      end
  end

  def detect_cycles!
    comps = []
    TSort.each_strongly_connected_component(self) { |c| comps << c }
    bad = comps.select { |c| c.size > 1 }
    raise "cycle: #{bad.inspect}" if bad.any?
  end

  # Precompute topo order once, with a priority-aware stable sort for nodes
  def compute_order
    pri = rules.transform_values(&:priority)
    # TSort doesn’t accept a comparator, so we do: topo order on a graph that
    # we walk deterministically by popping the lowest (priority, key).
    edges = build_edges
    indeg = Hash.new(0)
    edges.each do |u, vs|
      vs.each { |v| indeg[v] += 1 }
      indeg[u] ||= indeg[u] # ensure key exists
    end

    ready = rules.keys.select { |k| indeg[k].to_i == 0 }
    ready.sort_by! { |k| [pri[k] || 0, k.to_s] } # stable

    order = []
    while (u = ready.shift)
      order << u
      edges[u].each do |v|
        indeg[v] -= 1
        if indeg[v] == 0
          ready << v
          ready.sort_by! { |k| [pri[k] || 0, k.to_s] }
        end
      end
    end

    if order.size != rules.size
      raise "cycle during compute_order: got=#{order.size} expected=#{rules.size}"
    end

    order.freeze
  end
end
