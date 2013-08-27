require "cassandra_cql/connection"
require "cassandra_cql/notifications"

module CassandraCql
  class Client

    attr_reader :host, :port, :options, :connection, :supported, :last_request, :last_response

    def initialize(options = {})
      @options = { :host => "localhost", :port => 9042 }.merge(options)
      @options[:compression] = "snappy" if @options[:compression] == true

      @host = @options.delete(:host)
      @port = @options.delete(:port)
      @last_request = nil
      @last_response = nil

      reset_connection
    end

    def reset_connection
      @connection.close if @connection
      @connection = Connection.new(host, port, options[:timeout])

      get_supported
      check_and_load_compression_library
      startup
      query("USE #{@keyspace}") if @keyspace
    end

    def get_supported
      @supported = comm(Request::Options).options
    end

    def startup
      params = { "CQL_VERSION" => supported["CQL_VERSION"].first }
      params["COMPRESSION"] = options[:compression] if options[:compression]
      comm(Request::Startup, params)
    end

    def query(query, consistency = :quorum)
      comm(Request::Query, query, Helper.consistency_code(consistency))
    end

    def prepare(query)
      comm(Request::Prepare, query)
    end

    def execute(statement, *args)
      raise ArgumentError, "invalide statement (must be return value of #prepare)" unless statement.instance_of?(Frame::Result::Prepared)
      case args.length
      when 0
        binds = []
        consistency = :quorum
      when 1
        binds = args[0]
        consistency = :quorum
      when 2
        binds, consistency = args
      else
        raise ArgumentError, "expected 0..2 arguments"
      end
      comm(Request::Execute, statement, binds, Helper.consistency_code(consistency))
    end

  private

    def comm(klass, *args)
      Notifications.instrument("#{klass::EVENT_NAME}.cassandra_cql") do |payload|

        # Handle the request phase
        @last_request = request = klass.new(*args)
        request.set_notification_payload(payload)
        request.compression = options[:compression]
        connection.write(request.bytes)

        # Handle the response phase
        @last_response = response = Frame.recv(connection)
        raise(response.to_exception) if response.error?

        # If they set the keyspace, record that so we can set it on reconnect
        @keyspace = response.name if response.instance_of?(Frame::Result::SetKeyspace)

        # Return response
        response
      end
    end

    def comm_with_reconnect(*args)
      reconnected = false
      begin
        comm_without_reconnect(*args)
      rescue Errno::EPIPE => e
        if reconnected
          raise
        else
          reset_connection
          reconnected = true
          retry
        end
      end
    end

    alias_method :comm_without_reconnect, :comm
    alias_method :comm, :comm_with_reconnect

    def check_and_load_compression_library
      return unless options[:compression]
      compression_supported = supported["COMPRESSION"].include?(options[:compression])
      error_message = "unsupported compression: #{options[:compression]}"
      raise ArgumentError, error_message unless compression_supported

      case options[:compression]
      when COMPRESSION_SNAPPY
        require "snappy"
      end
    end

  end
end
