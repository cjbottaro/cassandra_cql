module CassandraCql
  module Frame
    class Supported
      include Response

      attr_reader :options, :header

      def initialize(buffer, header)
        @header = header
        @options = buffer.read_cql_string_multi_map
      end

      def [](key)
        options[key]
      end

    end
  end
end
