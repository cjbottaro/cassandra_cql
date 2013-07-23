module CassandraCql
  module Request
    class Execute
      include Request

      attr_reader :statement, :binds, :consistency

      def initialize(statement, binds, consistency)
        @statement = statement
        @binds = binds
        @consistency = consistency
      end

      def buffer
        Frame::Buffer.new.tap do |buffer|
          buffer.write_cql_short_bytes(statement.id)
          buffer.write_cql_short(binds.length)
          binds.each_with_index do |bind, i|
            col_spec = statement.metadata[:col_spec][i]
            type, collection_type = col_spec[:type], col_spec[:collection_type]
            bytes = Caster.to_bytes(bind, type, collection_type)
            buffer.write_cql_bytes(bytes)
          end
          buffer.write_cql_consistency(consistency)
        end
      end

      def set_notification_payload(payload)
        payload[:query_id] = statement.id
        payload[:binds] = binds
        payload[:consistency] = consistency
      end

    end
  end
end
