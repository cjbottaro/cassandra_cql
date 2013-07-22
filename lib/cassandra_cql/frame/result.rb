require "cassandra_cql/frame/result/shared/metadata"
require "cassandra_cql/frame/result/void"
require "cassandra_cql/frame/result/rows"
require "cassandra_cql/frame/result/set_keyspace"
require "cassandra_cql/frame/result/prepared"
require "cassandra_cql/frame/result/schema_change"

module CassandraCql
  module Frame
    class Result

      attr_reader :kind, :buffer, :header

      def self.read(buffer, header)
        new(buffer, header).downcast
      end

      def initialize(buffer, header)
        @kind = buffer.read_cql_int
        @buffer = buffer
        @header = header
      end

      def downcast
        case kind
        when KIND_VOID
          Void.new(buffer, header)
        when KIND_SET_KEYSPACE
          SetKeyspace.read(buffer, header)
        when KIND_ROWS
          Rows.read(buffer, header)
        when KIND_PREPARED
          Prepared.read(buffer, header)
        when KIND_SCHEMA_CHANGE
          SchemaChange.read(buffer, header)
        else
          raise "unexpected kind: #{kind}"
        end
      end

    end
  end
end
