module CassandraCql
  module Frame
    class Result
      class Prepared
        include Response
        include Shared::Metadata

        attr_reader :id, :metadata, :header

        def initialize(buffer, header)
          @header = header
          @id = buffer.read_cql_short_bytes
          @metadata = read_metadata(buffer)
        end

      end
    end
  end
end
