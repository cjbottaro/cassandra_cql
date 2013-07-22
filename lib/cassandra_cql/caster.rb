require "bigdecimal"
require "simple_uuid"
require "ipaddr"
require "set"

module CassandraCql
  module Caster
    extend self

    def to_bytes(value, type, collection_type = nil)
      case type
      when COLUMN_TYPE_ASCII
        value.force_encoding(Encoding::BINARY)
      when COLUMN_TYPE_BIGINT
        [value].pack("q>")
      when COLUMN_TYPE_BLOB
        hex = value[2..-1] if value.index("0x") == 0 or value.index("0X") == 0
        raise ArgumentError, "Invalid hex for blob: #{value} (bad length)" if hex.length % 2 != 0
        [hex].pack("H*")
      when COLUMN_TYPE_BOOLEAN
        value ? "\x01" : "\x00"
      when COLUMN_TYPE_COUNTER
        [value].pack("q>")
      when COLUMN_TYPE_DECIMAL
        case value
        when BigDecimal
          string = value.to_s("F")
        else
          string = value.to_s
        end
        scale = string.length - string.index(".") - 1
        number = string.sub(".", "").to_i
        to_bytes(scale, COLUMN_TYPE_INT) + to_bytes(number, COLUMN_TYPE_VARINT)
      when COLUMN_TYPE_DOUBLE
        [value].pack("G")
      when COLUMN_TYPE_FLOAT
        [value].pack("g")
      when COLUMN_TYPE_INT
        [value].pack("l>")
      when COLUMN_TYPE_TEXT
        value.dup.force_encoding("UTF-8")
      when COLUMN_TYPE_TIMESTAMP
        case value
        when String
          time_in_milli = (Time.parse(value).to_f * 1000).to_i
        when Integer
          time_in_milli = value
        when Time
          time_in_milli = (value.to_f * 1000).to_i
        else
          raise ArgumentError, "invalid timestamp input: #{value}"
        end
        [time_in_milli].pack("Q>")
      when COLUMN_TYPE_UUID
        SimpleUUID::UUID.new(value).bytes
      when COLUMN_TYPE_VARCHAR
        value.dup.force_encoding("UTF-8")
      when COLUMN_TYPE_VARINT
        bytes = []
        while value != 0 and value != -1
          bytes.unshift(value & 0xff)
          value = value >> 8
        end
        bytes.pack("C*")
      when COLUMN_TYPE_TIMEUUID
        SimpleUUID::UUID.new(value).bytes
      when COLUMN_TYPE_INET
        inet = IPAddr.new(value)
        if inet.ipv4?
          [inet.to_i].pack("L>")
        else
          [inet.to_i.to_s(16)].pack("H*")
        end
      when COLUMN_TYPE_LIST
        buffer = Frame::Buffer.new
        buffer.write_cql_short(value.length)
        value.each do |item|
          bytes = to_bytes(item, collection_type)
          buffer.write_cql_short_bytes(bytes)
        end
        buffer.bytes
      when COLUMN_TYPE_MAP
        buffer = Frame::Buffer.new
        buffer.write_cql_short(value.length)
        value.each do |k, v|
          k_bytes = to_bytes(k, collection_type[0])
          v_bytes = to_bytes(v, collection_type[1])
          buffer.write_cql_short_bytes(k_bytes)
          buffer.write_cql_short_bytes(v_bytes)
        end
        buffer.bytes
      when COLUMN_TYPE_SET
        buffer = Frame::Buffer.new
        buffer.write_cql_short(value.length)
        value.each do |item|
          bytes = to_bytes(item, collection_type)
          buffer.write_cql_short_bytes(bytes)
        end
        buffer.bytes
      else
        raise ArgumentError, "unexpected type: #{type}"
      end
    end

    def from_bytes(value, type, collection_type = nil)
      return nil if value.nil?

      case type
      when COLUMN_TYPE_ASCII
        value.force_encoding("ASCII-8BIT")
      when COLUMN_TYPE_BIGINT
        value.unpack("q>").first
      when COLUMN_TYPE_BLOB
        "0x" + value.unpack("H*").first
      when COLUMN_TYPE_BOOLEAN
        value == "\x01" ? true : false
      when COLUMN_TYPE_COUNTER
        value.unpack("q>").first
      when COLUMN_TYPE_DECIMAL
        i = from_bytes(value, COLUMN_TYPE_INT) # Tells us where in the string to put the decimal point
        string = from_bytes(value[4..-1], COLUMN_TYPE_VARINT).to_s
        decimal = string[0, string.length - i] + "." + string[string.length - i..-1]
        BigDecimal.new(decimal)
      when COLUMN_TYPE_DOUBLE
        value.unpack("G").first
      when COLUMN_TYPE_FLOAT
        value.unpack("g").first
      when COLUMN_TYPE_INT
        value.unpack("l>").first
      when COLUMN_TYPE_TEXT
        value.force_encoding("UTF-8")
      when COLUMN_TYPE_TIMESTAMP
        Time.at(value.unpack("Q>").first.to_f/1000)
      when COLUMN_TYPE_UUID
        SimpleUUID::UUID.new(value).to_guid
      when COLUMN_TYPE_VARCHAR
        value.force_encoding("UTF-8")
      when COLUMN_TYPE_VARINT
        varint = value.length.times.inject(0){ |memo, i| memo += value.getbyte(value.length - i - 1) << (8 * i) }
        varint -= 2**(value.length * 8) if value.getbyte(0) & 0x80 == 0x80
        varint
      when COLUMN_TYPE_TIMEUUID
        SimpleUUID::UUID.new(value).to_time
      when COLUMN_TYPE_INET
        IPAddr.ntop(value)
      when COLUMN_TYPE_LIST
        buffer = Frame::Buffer.new(value)
        buffer.read_cql_short.times.collect do
          bytes = buffer.read_cql_short_bytes
          from_bytes(bytes, collection_type)
        end
      when COLUMN_TYPE_MAP
        buffer = Frame::Buffer.new(value)
        buffer.read_cql_short.times.inject({}) do |memo|
          k = from_bytes(buffer.read_cql_short_bytes, collection_type[0])
          v = from_bytes(buffer.read_cql_short_bytes, collection_type[1])
          memo[k] = v
          memo
        end
      when COLUMN_TYPE_SET
        buffer = Frame::Buffer.new(value)
        buffer.read_cql_short.times.inject(Set.new) do |memo|
          bytes = buffer.read_cql_short_bytes
          memo << from_bytes(bytes, collection_type)
          memo
        end
      else
        raise ArgumentError, "unexpected type: #{type}"
      end
    end

  end
end
