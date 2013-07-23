require "cassandra_cql/frame/buffer"
require "cassandra_cql/frame/header"
require "cassandra_cql/frame/error"
require "cassandra_cql/frame/supported"
require "cassandra_cql/frame/ready"
require "cassandra_cql/frame/result"

module CassandraCql
  module Frame

    def self.recv(io)
      header = Header.recv(io)
      
      payload = io.read(header.length)
      payload = Snappy.inflate(payload) if header.compression?
      buffer = Buffer.new(payload)

      case header.opcode
      when OPCODE_READY
        Ready.new(buffer, header)
      when OPCODE_RESULT
        Result.read(buffer, header)
      when OPCODE_ERROR
        Error.read(buffer, header)
      when OPCODE_SUPPORTED
        Supported.read(buffer, header)
      else
        raise "unexpected opcode: #{header.opcode}"
      end
    end

  end
end
