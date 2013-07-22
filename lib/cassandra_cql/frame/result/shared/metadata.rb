module CassandraCql
  module Frame
    class Result
      module Shared
        module Metadata

          def read_metadata(buffer)
            {}.tap do |hash|
              hash[:flags] = buffer.read_cql_int
              hash[:columns_count] = buffer.read_cql_int
              has_global_table_spec = hash[:flags] & 0x0001 != 0
              if has_global_table_spec
                hash[:global_table_spec] = { :keyspace => buffer.read_cql_string, :table => buffer.read_cql_string }
              else
                hash[:global_table_spec] = {}
              end
              hash[:col_spec] = hash[:columns_count].times.collect do
                {}.tap do |spec|
                  if not has_global_table_spec
                    spec[:keyspace] = buffer.read_cql_string
                    spec[:table] = buffer.read_cql_string
                  end
                  spec[:name] = buffer.read_cql_string
                  spec[:type] = buffer.read_cql_short
                  case spec[:type]
                  when COLUMN_TYPE_SET, COLUMN_TYPE_LIST
                    spec[:collection_type] = buffer.read_cql_short
                  when COLUMN_TYPE_MAP
                    k_type = buffer.read_cql_short
                    v_type = buffer.read_cql_short
                    spec[:collection_type] = [k_type, v_type]
                  when COLUMN_TYPE_CUSTOM
                    spec[:custom_type] = buffer.read_cql_string
                  end
                end
              end
            end
          end

        end
      end
    end
  end
end
