require "optparse"
require "benchmark"
require "cassandra_cql"
require "cql"
require "pry"

DEFAULTS = {
  :threads            => 20,
  :keyspaces          => 20,
  :documents          => 1000,
  :items              => 50,
  :host               => ["localhost"],
  :port               => 9042,
  :replication_factor => 1,
  :adapter            => "cassandra_cql"
}

options = Struct.new(*DEFAULTS.keys).new(*DEFAULTS.values)

parser = OptionParser.new do |op|
  op.banner = "Usage: ruby stress.rb [options] create|run"

  op.on("-t", "--threads=N", "Number of threads (default 20)", Integer) do |v|
    options.threads = v
  end

  op.on("-k", "--keyspaces=N", "Number of keyspaces (default 20)", Integer) do |v|
    options.keyspaces = v
  end

  op.on("-d", "--documents=N", "Number of documents per keyspace (default 1000)", Integer) do |v|
    options.documents = v
  end

  op.on("-i", "--items=N", "Number of items per document (default 50)", Integer) do |v|
    options.items = v
  end

  op.on("-h", "--host=H1[,H2,H3]", "One or most hosts to connect to (default localhost)", Array) do |v|
    options.host = v
  end

  op.on("-p", "--port=PORT", "Port to connect to (default 9042)", Integer) do |v|
    options.port = v
  end

  op.on("-r", "--replication-factor", "Replication factor for creating tables (default 1)", Integer) do |v|
    options.replication_factor = v
  end

  op.on("-a", "--adapter=NAME", "Driver to use: cassandra_cql|cql-rb (default cassandra_cql)", String) do |v|
    options.adapter = v
  end

end

parser.parse!

if ARGV.length != 1 or !%w[create run].include?(ARGV[0])
  puts parser
  exit(-1)
end

Thread.abort_on_exception = true

class Stressor

  BASE_VERSION        = Time.mktime(2013, "oct", 1).to_i * 1000

  attr_reader :mutex, :options
  attr_accessor :latencies, :errors

  def initialize(options)
    @options = options
    @mutex = Mutex.new
    @latencies = []
    @errors = 0
  end

  def run
    options.threads.times do
      Thread.new do
        job = StressorJob.new(self)
        while true
          job.run
        end
      end
    end

    while true
      sleep(5)
      cnt, max, max_type, avg, min, min_type, err = nil
      mutex.synchronize do
        cnt = latencies.count
        max, max_type = latencies.max
        max = (max.to_f * 1000).round(2)
        max_type ||= "-"
        min, min_type = latencies.min
        min = (min.to_f * 1000).round(2)
        min_type ||= "-"
        avg = (latencies.inject(0){ |memo, latency| memo += latency[0]; memo } / cnt.to_f * 1000).round(2)
        err = errors
        @latencies = []
        @errors = 0
      end
      puts "%10d (cnt) %10d (err) %10.2f %s (min) %10.2f (avg) %10.2f %s (max)" % [cnt, err, min, min_type, avg, max, max_type]
    end
  end

  def create
    options.keyspaces.times do |i|
      keyspace_name = "stress_#{i+1}"

      begin
        connection.query <<-CQL
          CREATE KEYSPACE #{keyspace_name}
                     WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : #{options.replication_factor}};
        CQL
      rescue CassandraCql::Error::AlreadyExists => e
        nil
      end

      connection.query("use #{keyspace_name}")

      begin
        connection.query <<-CQL
          CREATE TABLE documents (
            id INT,
            version TIMESTAMP,
            items map<INT, TEXT>,
            PRIMARY KEY (id, version)
          );
        CQL
      rescue CassandraCql::Error::AlreadyExists => e
        nil
      end
    end
  end

  def connection
    @connection ||= CassandraCql::Client.new
  end

  class StressorJob

    attr_reader :stressor, :options
    attr_accessor :latencies, :errors

    def initialize(stressor = nil)
      @stressor = stressor
      @options = stressor.options
    end

    def run
      reset
      begin
        query("use #{keyspace}")
        query("select version, items from documents where id = #{id} and version = #{version}")
        query <<-CQL
          UPDATE documents
             SET items = items + #{items}
           WHERE id = #{id}
             AND version = #{version}
        CQL
      rescue StandardError => e
        raise
        self.errors += 1
        @connection = nil
      end
      record
    end

    def query(cql)
      latency = Benchmark.realtime do
        case options.adapter
        when "cassandra_cql"
          result = connection.query(cql)
        when "cql-rb"
          result = connection.execute(cql)
        end

        case result
        when CassandraCql::Frame::Result::Rows
          result.rows
        when Cql::Client::QueryResult
          result.to_a
        end
      end

      case cql.strip.split.first.downcase
      when "update"
        type = "I"
      when "select"
        type = "S"
      when "use"
        type = "U"
      end
      latencies << [latency, type]
    end

    def record
      if stressor
        stressor.mutex.synchronize do
          stressor.latencies.concat(latencies)
          stressor.errors += errors
        end
      end
    end

    def reset
      @id, @version, @keyspace, @items = nil
      @latencies = []
      @errors = 0
    end

    def id
      @id ||= rand(options.documents) + 1
    end

    def version
      @version ||= BASE_VERSION + (id * 1000)
    end

    def keyspace
      @keyspace ||= "stress_#{rand(options.keyspaces) + 1}"
    end

    def items
      @items ||= begin
        items = {}
        (rand(options.items) + 1).times do
          key = rand(options.items) + 1
          items[key] = "blah"
        end
        json_inspired_syntax(items)
      end
    end

    def json_inspired_syntax(hash)
      strings = hash.collect do |k, v|
        v = v.gsub("'", "''")
        "#{k.inspect} : '#{v}'"
      end
      "{ " + strings.join(", ") + " }"
    end

    def connection
      @connection ||= begin
      case options.adapter
        when "cassandra_cql"
          CassandraCql::Client.new(:host => options.host.sample, :port => options.port)
        when "cql-rb"
          Cql::Client.connect(:host => options.host.sample, :port => options.port)
        end
      end
    end

  end
end

case ARGV[0]
when "create"
  Stressor.new(options).create
when "run"
  Stressor.new(options).run
end
