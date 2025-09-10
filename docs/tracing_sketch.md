# Tracing with DataDog

Add Datadog APM spans around the ruleset and each rule, plus metrics. Keep tags low-cardinality.

## Wiring

```ruby

# config/initializers/datadog.rb

require 'datadog'
Datadog.configure do |c|
  c.service = 'menu-etl'
  c.env     = ENV['DD_ENV'] || 'prod'
  c.version = ENV['APP_VERSION']
end
STATSD = Datadog.statsd
Traced ruleset
module TracedRuleset
  def evaluate(ctx)
    Datadog::Tracing.trace('ruleset.evaluate', resource: ctx.ruleset_name || 'default') do |span|
      span.set_tag('etl.stage', 'transform')
      span.set_tag('item.source', ctx.source_id)               # low-card
      span.set_tag('item.external_id', hash_id(ctx.external_id)) # hash to avoid cardinality/PII
      span.set_tag('changed.count', Array(ctx.changed_keys).size)

      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      result, fired = traced_rules(ctx)                        # see below
      dur = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0

      span.set_tag('rules.fired_count', fired.size)
      STATSD.distribution('etl.ruleset.ms', dur, tags: ['stage:transform'])
      STATSD.count('etl.rules.fired', fired.size, tags: ['stage:transform'])

      [result, fired]
    rescue => e
      span.set_error(e)
      STATSD.increment('etl.ruleset.errors', tags: ['stage:transform'])
      raise
    end
  end

  private

  def traced_rules(ctx)
    result = {}
    fired  = []

    @rules.each do |rule|
      Datadog::Tracing.trace('rule.apply', resource: rule.name) do |span|
        span.set_tag('rule.name', rule.name)
        span.set_tag('rule.priority', rule.priority)
        span.set_tag('rule.type', rule.class.name)
        span.set_tag('category', ctx.assigned_category&.key || 'unknown')

        if rule.applies?(ctx)
          fired << rule.name
          patch = rule.changes(ctx)
          # optional: conflict detection details
          conflict = (result.keys & patch.keys)
          span.set_tag('rule.applied', true)
          span.set_tag('rule.conflicts', conflict.join(',')) unless conflict.empty?
          STATSD.increment('etl.rule.fired', tags: ["rule:#{rule.name}"])
          result = merge(result, patch)                       # your merge policy
        else
          span.set_tag('rule.applied', false)
        end
      rescue => e
        Datadog::Tracing.active_span&.set_error(e)
        STATSD.increment('etl.rule.errors', tags: ["rule:#{rule.name}"])
        raise
      end
    end

    [result, fired]
  end

  def hash_id(v) = v ? Digest::SHA256.hexdigest[v.to_s](0,16) : nil
end

# Apply it:
class RuleSet
  include TracedRuleset
  attr_reader :rules
  def initialize(rules, conflict: :last_wins, name: 'default')
    @rules, @conflict, @name = rules, conflict, name
  end
  def ruleset_name = @name

# merge(...) as before

end
```

Trace the ETL item
Wrap the whole item transform so spans nest:

```ruby
def process_item(item)
  Datadog::Tracing.trace('etl.item', resource: 'menu_item') do |span|
    span.set_tag('source', item[:source_id])
    span.set_tag('external_id', hash_id(item[:payload]['external_id']))
    span.set_tag('op', item[:menu_item] ? 'update' : 'create')

    changes, fired = ruleset.evaluate(ctx_for(item))
    outcome = persist(item, changes)
    
    # noop|updated|created|deleted|rejected
    span.set_tag('outcome', outcome)
    STATSD.increment('etl.item', tags: ["outcome:#{outcome}"])
    [outcome, fired]
  end
end
```

Rule “reasons” as tags
Extend rules to report why they fired:

```ruby
class SomeRule
  def applies?(ctx) = (cond = ctx.changed_keys.include?('categories'); @reason = cond ? 'categories_changed' : nil; cond)
  def reason = @reason
end

# In tracer:
span.set_tag('rule.reason', rule.reason) if rule.respond_to?(:reason)
```

## Log correlation

Use Datadog’s logger formatter to inject trace_id/span_id:

```ruby
logger = Logger.new($stdout)
logger.formatter = proc do |sev, t, _p, msg|
  tid = Datadog::Tracing.correlation.trace_id
  sid = Datadog::Tracing.correlation.span_id
  "#{t.utc.iso8601} #{sev} trace_id=#{tid} span_id=#{sid} #{msg}\n"
end
```

## Metrics to emit

- `etl.ruleset.ms` (distribution) per item.
- `etl.rule.ms` if you want finer timing: measure within rule.apply.
- `etl.rule.fired` (count) tagged `rule:<name>`.
- `etl.rule.errors` and etl.ruleset.errors.
- `etl.item with` outcome:*.

Optional gauges: batch preload counts: `etl.preload.brands`, `etl.preload.tags`.

## Tag hygiene

Prefer tags with bounded cardinality: rule, outcome, category, env, service.

Avoid free-text reasons unless from a controlled set.
