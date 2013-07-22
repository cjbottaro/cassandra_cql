module CassandraCql
  module Frame
    class Error
      include Response

      attr_reader :code, :message, :header

      def self.read(buffer, header)
        new(buffer, header).downcast
      end

      def initialize(buffer, header)
        @header = header
        @code = buffer.read_cql_int
        @message = buffer.read_cql_string
      end

      def downcast

      end

    end
  end
end
