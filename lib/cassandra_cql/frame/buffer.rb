module CassandraCql
  module Frame
    class Buffer

      attr_reader :bytes, :pos

      def initialize(bytes = "")
        @bytes = bytes
        @pos = 0
      end

      def length
        @bytes.length - @pos
      end

      def seek(pos, how = :abs)
        pos += @pos if [:relative, :rel].include?(how)
        raise ArgumentError, "seek out of range: #{pos}" unless (0...@bytes.length).include?(pos)
        @pos = pos
      end

      def to_s
        @bytes.inspect
      end

      def read_byte
        @bytes.getbyte(@pos).tap{ @pos += 1 }
      end

      def read_cql_int
        @bytes[@pos, 4].unpack("l>").first.tap{ @pos += 4 }
      end

      def read_cql_short
        @bytes[@pos, 2].unpack("n").first.tap{ @pos += 2 }
      end

      def read_cql_string
        length = read_cql_short
        @bytes[@pos, length].tap{ @pos += length }
      end

      def read_cql_long_string
        length = read_cql_int
        @bytes[@pos, length].tap{ @pos += length }
      end

      def read_cql_string_list
        read_cql_short.times.collect{ read_cql_string }
      end

      def read_cql_string_multi_map
        read_cql_short.times.inject({}) do |memo, _|
          memo[read_cql_string] = read_cql_string_list
          memo
        end
      end

      def read_cql_uuid
        raise "not implemented"
      end

      def read_cql_string_list
        read_cql_short.times.collect{ read_cql_string }
      end

      def read_cql_bytes
        length = read_cql_int
        if length <= 0
          nil
        else
          @bytes[@pos, length].tap{ @pos += length }
        end
      end

      def read_cql_short_bytes
        length = read_cql_short
        if length <= 0
          nil
        else
          @bytes[@pos, length].tap{ @pos += length }
        end
      end

      def write_cql_int(n)
        @bytes += [n].pack("l>")
      end

      def write_cql_short(n)
        @bytes += [n].pack("n")
      end

      def write_cql_string(string)
        write_cql_short(string.length)
        @bytes += string
      end

      def write_cql_long_string(string)
        write_cql_int(string.length)
        @bytes += string
      end

      def write_cql_short_bytes(bytes)
        write_cql_string(bytes.to_s.dup.force_encoding(Encoding::BINARY))
      end

      def write_cql_bytes(bytes)
        write_cql_long_string(bytes.to_s.dup.force_encoding(Encoding::BINARY))
      end

      def write_cql_consistency(consistency)
        write_cql_short(consistency)
      end

      def write_cql_string_map(hash)
        write_cql_short(hash.length)
        hash.each do |k, v|
          write_cql_string(k)
          write_cql_string(v)
        end
      end

    end
  end
end
