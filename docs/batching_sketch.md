# Pipeline Batching Sketch

Batch the rule eval and share lookups. Keep rules pure. Preload, then fan out.
Batch API

## Evaluate many items with shared caches and per-item isolation

```ruby
class RuleBatch
  Result = Struct.new(:index, :id, :changes, :fired, :error, keyword_init: true)

  def initialize(ruleset:, resolvers:)
    @ruleset   = ruleset              # RuleSet
    @resolvers = resolvers.freeze     # e.g. BrandRepo, TagRepo, StrainRepo
  end

# items: array of {id:, payload:, menu_item:, changed_keys:}

  def evaluate_many(items, concurrency: 8, fail_fast: false)
    items = items.each_with_index.map { |it, idx| it.merge(index: idx) }

    # 1) Preload all references once
    preload!(items)

    # 2) Parallel map with order preservation
    q = Queue.new
    items.each { |it| q << it }
    workers = [items.size, concurrency].min
    out = Array.new(items.size)

    threads = workers.times.map do
      Thread.new do
        while (it = q.pop(true) rescue nil)
          out[it[:index]] = evaluate_one(it)
        end
      rescue => e
        raise e if fail_fast
      end
    end
    threads.each(&:join)
    out
  end

  private

  def preload!(items)
    @brand_ids   = items.filter_map { |it| it[:payload]['brand_id'].presence }.uniq
    @strain_names= items.filter_map { |it| it[:payload]['strain_name'].presence }.uniq
    @tag_names   = items.flat_map { |it| Array(it[:payload]['tag_names']) }.compact.uniq

    @brand_map   = @resolvers.brands.by_ids(@brand_ids)         # {id=>Brand}
    @strain_map  = @resolvers.strains.by_names(@strain_names)   # {name=>id}
    @tag_map     = @resolvers.tags.by_names(@tag_names)         # {name=>Tag}
  end

  def evaluate_one(it)
    ctx = MenuItemContext.new(
      payload:        it[:payload].freeze,
      menu_item:      it[:menu_item],
      changed_keys:   Array(it[:changed_keys]).freeze,
      assigned_category: nil
    )

    # Inject shared caches via lightweight resolver on context
    ctx.define_singleton_method(:brand_lookup)  { |id| @brand_map[id] }
    ctx.define_singleton_method(:strain_lookup) { |name| @strain_map[name] }
    ctx.define_singleton_method(:tag_lookup)    { |name| @tag_map[name] }

    changes, fired = @ruleset.evaluate(ctx)
    Result.new(index: it[:index], id: it[:id], changes:, fired:)
  rescue => e
    Result.new(index: it[:index], id: it[:id], changes: {}, fired: [], error: e)
  end
end

# Repositories (no N+1)

class BrandRepo
  def by_ids(ids)
    return {} if ids.empty?
    Brand.published.where(id: ids).pluck(:id, :brand_name).to_h # shape as needed
  end
end

class StrainRepo
  def by_names(names)
    return {} if names.empty?
    Strain.where(name: names).pluck(:name, :id).to_h
  end
end

class TagRepo
  def by_names(names)
    return {} if names.empty?
    Tag.published.where(name: names).index_by(&:name)
  end
end
```

## Wiring

```ruby
ruleset   = MenuItemChangeRules.build
resolvers = OpenStruct.new(
  brands:  BrandRepo.new,
  strains: StrainRepo.new,
  tags:    TagRepo.new
)

batch = RuleBatch.new(ruleset:, resolvers:)
results = batch.evaluate_many(items, concurrency: 8)
Persistence pattern
Apply per item in its own transaction. No cross-item locks.
results.each do |r|
  next if r.error
  MenuItem.transaction do
    item = MenuItem.find(r.id)
    item.update!(r.changes)
  end
end
```

### Notes

- Preserve input order by index.
- Use thread-safe read-only caches. Do not memoize inside rules.
- Keep rules deterministic. No DB calls from rules; only via injected lookups.
- Collect errors per item. Optional `fail_fast`.
- Tune concurrency to DB pool size.
