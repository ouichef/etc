module Ingest
end
Measure =
  Data.define(:amount, :unit) do
    Changeset = Data.define(:changes, :data)

    def changeset(other)
      return unless other.is_a?(self.class)
      return Changeset.new(changes: {}, data: self.to_h) if self == other
      changes = self.-(other)
      Changeset.new(changes:, data: self.to_h)
    end

    def -(other)
      return unless other.is_a?(self.class)

      members.each_with_object({}) do |member, acc|
        val = other.send(member)
        next if self.send(member) == val
        acc[member] = val
      end
    end
  end

a = Measure.new(amount: 50, unit: "kg")
b = Measure.new(amount: 50, unit: "kg")
c = Measure.new(amount: 50, unit: "mg")
d = Measure.new(amount: 25, unit: "kg")
e = Measure.new(amount: 25, unit: nil)
