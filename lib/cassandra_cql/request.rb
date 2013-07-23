module CassandraCql
  module Request

    def self.included(mod)
      mod.extend(ClassMethods)
      mod.class_eval do
        const_set(:OPCODE, CassandraCql.const_get("OPCODE_%s" % name.split("::").last.upcase))
        const_set(:EVENT_NAME, name.split("::").last.downcase)
        attr_accessor :compression
      end
    end

    def set_notification_payload(payload)
      payload
    end

    def header
      flags = 0
      flags |= HEADER_FLAG_COMPRESSION if compression and compressable?
      Frame::Header.new(:opcode => self.class::OPCODE, :length => body.length, :flags => flags)
    end

    def body
      if compressable?
        deflate(buffer.bytes)
      else
        buffer.bytes
      end
    end

    def buffer
      Frame::Buffer.new
    end

    def bytes
      header.bytes + body
    end

    def compressable?
      self.class.compressable?
    end

    def deflate(bytes)
      case compression
      when COMPRESSION_SNAPPY
        Snappy.deflate(bytes)
      else
        bytes
      end
    end

    module ClassMethods

      def compressable?
        true
      end

    end

  end
end

require "cassandra_cql/request/options"
require "cassandra_cql/request/startup"
require "cassandra_cql/request/query"
require "cassandra_cql/request/prepare"
require "cassandra_cql/request/execute"
