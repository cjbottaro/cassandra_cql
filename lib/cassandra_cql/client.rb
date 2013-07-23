require "socket"
require "cassandra_cql/notifications"

module CassandraCql
  class Client

    attr_reader :host, :port, :options, :socket, :last_request, :last_response

    def initialize(options = {})
      @options = { :host => "localhost", :port => 9042 }.merge(options)
      @options[:compression] = "snappy" if @options[:compression] == true

      @host = @options.delete(:host)
      @port = @options.delete(:port)
      @socket = TCPSocket.new(host, port)
      @last_request = nil
      @last_response = nil

      check_and_load_compression_library if @options[:compression]

      startup
    end

    def supported
      @supported ||= comm(Request::Options)
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
        request = klass.new(*args)
        request.set_notification_payload(payload)
        request.compression = options[:compression]
        socket.sendmsg(request.bytes)
        @last_request  = request

        # Handle the response phase
        response = Frame.recv(socket)
        raise(Helper.frame_error_to_execption(response)) if response.error?
        @last_response = response
      end
    end

    def check_and_load_compression_library
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
