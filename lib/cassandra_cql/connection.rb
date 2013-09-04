require "socket"

module CassandraCql
  class Connection

    class ClosedError < StandardError; end

    attr_reader :socket, :timeout

    def initialize(host, port, timeout)
      @timeout = timeout
      @socket = TCPSocket.new(host, port)
      singleton_class.class_eval do
        if timeout
          alias_method :read,  :read_with_timeout
          alias_method :write, :write_with_timeout
        else
          alias_method :read,  :read_without_timeout
          alias_method :write, :write_without_timeout
        end
      end
    end

    def read_without_timeout(length)
      socket.read(length).tap do |data|
        raise(ClosedError, "connection closed") if data.nil?
      end
    end

    def write_without_timeout(bytes)
      socket.sendmsg(bytes)
    rescue Errno::EPIPE => e
      raise(ClosedError, "connection closed")
    end

    def read_with_timeout(length)
      if length == 0
        nil
      elsif IO.select([socket], nil, nil, timeout)
        read_without_timeout(length)
      else
        raise Errno::ETIMEDOUT
      end
    end

    def write_with_timeout(bytes)
      if bytes.empty?
        0
      elsif IO.select(nil, [socket], nil, timeout)
        write_without_timeout(bytes)
      else
        raise Errno::ETIMEDOUT, "write timeout"
      end
    end

    def close
      @socket.close
    end

  end
end
