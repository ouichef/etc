namespace :rules do
  task :validate do
    rs = Ingest::RuleSet.compile(rules: RULES, version: RULESET_VERSION)
    File.write("tmp/rules_order.txt", rs.ordered_keys.join("\n"))
  end
end
