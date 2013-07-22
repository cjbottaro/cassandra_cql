module CassandraCql
  module Frame
    class Result
      class SetKeyspace
        include Response

        attr_reader :name, :header

        def initialize(buffer, header)
          @header = header
          @name = buffer.read_cql_string
        end

      end
    end
  end
end
