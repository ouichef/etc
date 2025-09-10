require "rails_helper"

RSpec.describe "Changesetable mixin on Data classes" do
  before do
    stub_const("Measure", Data.define(:amount, :unit) { include Changesetable })
    stub_const("Person", Data.define(:name, :age) { include Changesetable })
  end

  let(:a) { Measure.new(amount: 50, unit: "kg") }
  let(:b) { Measure.new(amount: 50, unit: "kg") }
  let(:c) { Measure.new(amount: 50, unit: "mg") }
  let(:d) { Measure.new(amount: 25, unit: "kg") }
  let(:e) { Measure.new(amount: 25, unit: nil) }

  describe "#diff / #changeset" do
    it "returns empty changes for identical values" do
      cs = a.changeset(b)
      expect(cs).to be_a(Measure::Changeset)
      expect(cs.changes).to eq({})
      expect(cs.data).to eq({ amount: 50, unit: "kg" })
    end

    it "captures per-field {from,to} and preserves member order" do
      cs = a.changeset(e)
      expect(cs.changes.keys).to eq(%i[amount unit]) # insertion order
      expect(cs.changes[:amount]).to eq(from: 50, to: 25)
      expect(cs.changes[:unit]).to eq(from: "kg", to: nil)
    end

    it "raises on mismatched types" do
      other = Person.new(name: "Ada", age: 36)
      expect { a.changeset(other) }.to raise_error(TypeError)
      expect { a.diff(other) }.to raise_error(TypeError)
    end
  end

  describe "#apply (delegates to #with semantics)" do
    it "applies changes non-strict" do
      cs = a.changeset(e)
      expect(a.apply(cs)).to eq(Measure.new(amount: 25, unit: nil))
    end

    it "applies changes strict and raises on stale field" do
      cs = a.changeset(e)
      mutated = a.with(amount: 60) # simulate concurrent write
      expect { mutated.apply(cs, strict: true) }.to raise_error(
        RuntimeError,
        /stale amount/i
      )
    end
  end

  describe "pattern matching on Changeset" do
    it "matches by structure via deconstruct_keys" do
      cs = a.changeset(c)
      res =
        case cs
        in changes: { unit: { from: "kg", to: "mg" } }
          :unit_changed
        else
          :nope
        end
      expect(res).to eq(:unit_changed)
    end

    it "captures values" do
      cs = a.changeset(d)
      # hash pattern capture
      cs => { changes: { amount: { from:, to: } }, data: }
      expect([from, to, data]).to eq([50, 25, { amount: 50, unit: "kg" }])
    end

    it "can assert key order deterministically when both fields change" do
      cs = a.changeset(e)
      case cs
      in changes: { amount: { to: 25 }, unit: { to: nil } }
        order = :amount_then_unit
      in changes: { unit: { to: nil }, amount: { to: 25 } }
        order = :unit_then_amount
      end
      expect(order).to eq(:amount_then_unit)
    end
  end

  describe "reuse across classes" do
    it "installs a namespaced Changeset per class" do
      p1 = Person.new(name: "Ada", age: 36)
      p2 = Person.new(name: "Ada", age: 37)
      cs = p1.changeset(p2)
      expect(cs).to be_a(Person::Changeset)
      expect(cs.changes).to eq(age: { from: 36, to: 37 })
    end
  end
end
