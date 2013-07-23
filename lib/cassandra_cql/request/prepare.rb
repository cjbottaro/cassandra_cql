module CassandraCql
  module Request
    class Prepare
      include Request

      attr_reader :query

      def initialize(query)
        @query = query
      end

      def buffer
        Frame::Buffer.new.tap do |buffer|
          buffer.write_cql_long_string(query)
        end
      end

      def set_notification_payload(payload)
        payload[:query] = query
      end

    end
  end
end
