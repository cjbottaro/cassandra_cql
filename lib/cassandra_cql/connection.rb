require "socket"

module CassandraCql
  class Connection

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
      socket.read(length)
    end

    def write_without_timeout(bytes)
      socket.sendmsg(bytes)
    end

    def read_with_timeout(length)
      if length == 0
        nil
      elsif IO.select([socket], nil, nil, timeout)
        socket.recv(length)
      else
        raise Errno::ETIMEDOUT
      end
    end

    def write_with_timeout(bytes)
      if bytes.empty?
        0
      elsif IO.select(nil, [socket], nil, timeout)
        socket.sendmsg(bytes)
      else
        raise Errno::ETIMEDOUT, "write timeout"
      end
    end

    def close
      @socket.close
    end

  end
end
