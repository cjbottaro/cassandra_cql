module CassandraCql
  module Frame
    class Prepare
      include Request

      attr_reader :query

      def initialize(query)
        @query = query
      end

      def buffer
        Buffer.new.tap do |buffer|
          buffer.write_cql_long_string(query)
        end
      end

    end
  end
end
