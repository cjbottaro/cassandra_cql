module CassandraCql
  module Request
    class Startup
      include Request

      attr_reader :options

      def initialize(options = {})
        @options = { "CQL_VERSION" => "3.0.0" }.merge(options)
      end

      def compressable?
        false
      end

      def buffer
        Frame::Buffer.new.tap do |buffer|
          buffer.write_cql_string_map(options)
        end
      end

      def set_notification_payload(payload)
        payload[:options] = options
      end

    end
  end
end
