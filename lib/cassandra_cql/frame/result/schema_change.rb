module CassandraCql
  module Frame
    class Result
      class SchemaChange

        include Response

        attr_reader :change, :keyspace, :table, :header

        def self.read(buffer, header)
          new(buffer, header)
        end

        def initialize(buffer, header)
          @header   = header
          @change   = buffer.read_cql_string
          @keyspace = buffer.read_cql_string
          @table    = buffer.read_cql_string
        end

      end
    end
  end
end
