module CassandraCql
  module Request
    class Query
      include Request

      attr_reader :query, :consistency

      def initialize(query, consistency)
        @query = query
        @consistency = consistency
      end

      def buffer
        Frame::Buffer.new.tap do |buffer|
          buffer.write_cql_long_string(query)
          buffer.write_cql_consistency(consistency)
        end
      end

      def set_notification_payload(payload)
        payload[:query] = query
        payload[:consistency] = consistency
      end
    end
  end
end
