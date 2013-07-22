module CassandraCql
  module Frame
    class Header

      attr_reader :version, :flags, :stream, :opcode, :length

      def self.recv(io)
        read(io.read(8))
      end

      def self.read(string)
        version, flags, stream, opcode, length = string.unpack("CCcCN")
        new :version  => version,
            :flags    => flags,
            :stream   => stream,
            :opcode   => opcode,
            :length   => length
      end

      def initialize(attributes = {})
        @version = attributes[:version] || 0x01
        @flags   = attributes[:flags]   || 0
        @stream  = attributes[:stream]  || 0
        @opcode  = attributes[:opcode]  || 0
        @length  = attributes[:length]  || 0
      end

      def bytes
        [version, flags, stream, opcode, length].pack("CCcCN")
      end

      def compression?
        flags & HEADER_FLAG_COMPRESSION != 0
      end

    end
  end
end
