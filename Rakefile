require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = "--color"
end

task "db:test:prepare" do
  require "cql"

  client = Cql::Client.connect(:host => 'localhost')

  begin
    client.execute("DROP KEYSPACE cassandra_cql_test")
  rescue Cql::QueryError => e
    nil
  end

  client.execute <<-CQL
    CREATE KEYSPACE cassandra_cql_test
               WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
  CQL

  client.use("cassandra_cql_test")

  client.execute <<-CQL
    CREATE TABLE test (
      ascii ascii,
      bigint bigint,
      blob blob,
      boolean boolean,
      decimal decimal,
      double double,
      float float,
      inet inet,
      int int,
      text text,
      timestamp timestamp,
      timeuuid timeuuid,
      uuid uuid,
      varchar varchar,
      varint varint,
      nil int,
      map map<text, int>,
      list list<timestamp>,
      cet set<decimal>,
      PRIMARY KEY (ascii)
    );
  CQL

  client.execute("CREATE INDEX ON test(int)")

  500.times do |i|
    i += 1
    client.execute <<-CQL
      INSERT INTO test (ascii, bigint, blob, boolean, decimal, double, float, inet, int, text, timestamp, timeuuid, uuid, varchar, varint, list, map, cet)
      VALUES ('#{i}', 1, 0x68656c6c, true, 12.345, 1.5, 1.5, '8.8.8.8', 1, 'text', dateOf(now()), now(), 1766a37c-e58e-11e2-8aa1-6e97c8bbb552, 'text', 1, [dateOf(now()), dateOf(now())], { 'one' : 1, 'two' : 2 }, { 12.345, 123.45 })
    CQL
    client.execute <<-CQL
      INSERT INTO test (ascii, bigint, blob, boolean, decimal, double, float, inet, int, text, timestamp, timeuuid, uuid, varchar, varint, list, map, cet)
      VALUES ('-#{i}', -1, 0x68656c6c, true, -12.345, -1.5, -1.5, '1050:0:0:0:5:600:300c:326b', -1, 'text', dateOf(now()), now(), 1766a37c-e58e-11e2-8aa1-6e97c8bbb552, 'text', -1, [dateOf(now()), dateOf(now())], { 'one' : 1, 'two' : 2 }, { 12.345, 123.45 })
    CQL
  end

end

task "performance" do

  Bundler.require
  require "benchmark"
  require "cassandra_cql"
  require "cql"
  require "cassandra-cql"
  require "pry"

  c1 = CassandraCql::Client.new
  c2 = Cql::Client.connect
  c3 = CassandraCQL::Database.new('127.0.0.1:9160')

  c1.query("USE cassandra_cql_test")
  c2.execute("USE cassandra_cql_test")
  c3.execute("USE cassandra_cql_test")

  puts "\nselecting 1000 rows 1 time"
  cql = "SELECT * FROM test LIMIT 1000"
  Benchmark.bm(16) do |bm|
    bm.report("cassandra_cql"){ c1.query(cql) }
    bm.report("cql-rb"){ c2.execute(cql) }
    bm.report("cassandra-cql"){ c3.execute(cql) }
  end

  puts "\nselecting 1 row 1000 times"
  cql = "SELECT * FROM test LIMIT 1"
  Benchmark.bm(16) do |bm|
    bm.report("cassandra_cql"){ 1000.times{ c1.query(cql) } }
    bm.report("cql-rb"){ 1000.times{ c2.execute(cql) } }
    bm.report("cassandra-cql"){ 1000.times{ c3.execute(cql) } }
  end

end

task "profile" do

  # Must be set before environment is loaded.
  ENV["CPUPROFILE_FREQUENCY"] = "4000"

  Bundler.require
  require "cassandra_cql"
  require 'perftools'

  c1 = CassandraCql::Client.new
  c1.query("USE cassandra_cql_test")

  cql = "SELECT * FROM test LIMIT 1000"
  PerfTools::CpuProfiler.start("profile1") do
    c1.query(cql)
  end

  system("bundle exec pprof.rb --text profile1")
  system("rm profile1")

end
