module CassandraCql
  module Frame
    class Result
      class Rows
        include Response
        include Enumerable
        include Shared::Metadata

        attr_reader :metadata, :rows_count, :rows_content, :rows, :header

        def initialize(buffer, header)
          @header = header
          @metadata = read_metadata(buffer)
          @rows_count = buffer.read_cql_int
          @rows_content = @rows_count.times.collect do |i|
            metadata[:columns_count].times.collect do |memo, j|
              buffer.read_cql_bytes
            end
          end
          rows
        end

        def rows
          @rows ||= begin
            @rows_content.collect do |row|
              i = -1
              row.inject({}) do |memo, value|
                i += 1
                name            = metadata[:col_spec][i][:name]
                type            = metadata[:col_spec][i][:type]
                collection_type = metadata[:col_spec][i][:collection_type]
                memo[name] = Caster.from_bytes(value, type, collection_type)
                memo
              end
            end
          end
        end

        def each(&block)
          rows.each(&block)
        end

        def length
          rows.length
        end

        def [](i)
          rows[i]
        end

      end
    end
  end
end
