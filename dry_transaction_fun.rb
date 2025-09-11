require "tsort"
require "dry-types"
require "dry-initializer"
require "dry-validation"
require "dry-transaction"
require "dry-struct"
require "yaml"
require "dry/transaction/operation"
require "dry/core"

NOTIFICATIONS = []
USERS = []

# module UserCreationListener
#   extend self

#   def on_step(event)
#     event.payload => { args: [{ email: }] }
#     NOTIFICATIONS << "Started creation of #{email}"
#   end

#   def on_step_succeeded(event)
#     user = event[:value]
#     NOTIFICATIONS << "#{user.email} created"
#   end

#   def on_step_failed(event)
#     user = event[:value]
#     NOTIFICATIONS << "#{user.email} creation failed"
#   end
# end

module Types
  include Dry.Types()
end
class User < Dry::Struct
  attribute :name, Types::String
  attribute :email, Types::String
end

class UserContract < Dry::Validation::Contract
  params do
    required(:name).filled(:string)
    required(:email).filled(:string)
  end
end

class Pipeline
  include Dry::Transaction

  step :validate_raw_payload
  step :map_raw_payload
  step :transform_to_menu_item
  step :validate_menu_item
  step :persist

  private

  def validate_raw_payload(item_ctx:, sync_ctx:)
    result = sync_ctx.contract.call(item_ctx.payload)
    result.success? ? Success(item_ctx) : Failure(result.errors.to_h)
  end

  def external_ruleset(item_ctx:)
    item_ctx => { payload:, external_tx: }
    item_ctx.with(normalized_payload: external_tx.call(payload))
  end

  def canonical_ruleset(item_ctx:, sync_ctx:, processors:)
    processors[item_ctx.action].transform(item_ctx:, sync_ctx:)
  end

  def validate_menu_item(item_ctx:, contract:)
    result = contract.call(item_ctx.canonical_payload)
    result.success? ? Success(item_ctx) : Failure(result.errors.to_h)
  end

  def persist(item_ctx:, processors:)
    processors[item_ctx.action].persist(item_ctx:, sync_ctx:)
  end
end

def run(success)
  create_user = CreateUser.new
  contract = UserContract.new
end

module Operations
  extend self

  def validate
    proc do |thing|
      include Dry::Transaction::Operation
      thing.things? ? Success(:ok) : Failure(:noop)
    end
  end
end
class Container
  extend Dry::Core::Container::Mixin

  namespace "users" do
    register "validate" do
      Operations.validate
    end
  end
end

Thing =
  Struct.new(:thing) do
    def things?
      !self[:thing].nil?
    end
  end

class ThingUser
  include Dry.Transaction(container: Container)

  step :validate, with: "users.validate"
end

def thing_it
  s = Thing.new("x")
  n = Thing.new(nil)
  tu = ThingUser.new
  [s, n, tu]
end
