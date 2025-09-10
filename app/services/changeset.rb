module Changesetable
  # Expects to be included in a Data class, i.e.
  # Measure = Data.define(:amount, :unit){ include Changesetable }
  def self.included(base)
    unless base.const_defined?(:Changeset, false)
      base.const_set(:Changeset, Data.define(:changes, :data))
    end
  end

  def changeset(other)
    raise TypeError, "expected #{self.class}" unless other.is_a?(self.class)
    self.class::Changeset.new(changes: diff(other), data: to_h)
  end

  def diff(other)
    raise TypeError, "expected #{self.class}" unless other.is_a?(self.class)
    self
      .class
      .members
      .each_with_object({}) do |m, h|
        from = public_send(m)
        to = other.public_send(m)
        next if from == to
        h[m] = { from:, to: }
      end
  end

  # Optional convenience: strict optimistic check, then delegate to #with.
  def apply(cs, strict: false)
    unless cs.is_a?(self.class::Changeset)
      raise TypeError, "expected #{self.class}::Changeset"
    end
    if strict
      cs.changes.each do |k, v|
        next unless v.is_a?(Hash) && v.key?(:from)
        if public_send(k) != v[:from]
          raise "stale #{k}: have #{public_send(k).inspect}, expected #{v[:from].inspect}"
        end
      end
    end
    with(**changes_kwargs(cs))
  end

  private

  # Convert a {from,to} map to kwargs for #with.
  def changes_kwargs(cs)
    cs
      .changes
      .each_with_object({}) do |(k, v), h|
        h[k] = v.is_a?(Hash) && v.key?(:to) ? v[:to] : v
      end
  end
end
