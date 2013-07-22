module CassandraCql
  module Request

    def self.included(mod)
      mod.extend(ClassMethods)
    end

    def opcode
      @opcode ||= CassandraCql.const_get("OPCODE_%s" % self.class.name.split("::").last.upcase)
    end

    def buffer
      Frame::Buffer.new
    end

    def bytes
      payload = buffer
      header = Frame::Header.new(:opcode => opcode, :length => payload.length)
      header.bytes + payload.bytes
    end

    module ClassMethods

    end

  end
end
