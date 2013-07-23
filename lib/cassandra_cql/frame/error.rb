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

      # TODO implement this
      def downcast
        self
      end

      def to_exception
        CassandraCql::Error::MAP[code].new(code, message)
      end

    end
  end
end
