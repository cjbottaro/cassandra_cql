module CassandraCql
  module Frame
    class Startup
      include Request

      attr_reader :options

      def initialize(options = {})
        @options = { "CQL_VERSION" => "3.0.0" }.merge(options)
      end

      def buffer
        Buffer.new.tap do |buffer|
          buffer.write_cql_string_map(options)
        end
      end

    end
  end
end
