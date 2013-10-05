module CassandraCql
  class Error < StandardError

    attr_reader :code

    def initialize(code, message)
      super(message)
      @code = code
    end

    class Server < self; end
    class Protocol < self; end
    class BadCredentials < self; end
    class Unavailable < self; end
    class Overloaded < self; end
    class IsBootstrapping < self; end
    class Truncate < self; end
    class WriteTimeout < self; end
    class ReadTimeout < self; end
    class Syntax < self; end
    class Unauthorized < self; end
    class Invalid < self; end
    class Config < self; end
    class AlreadyExists < self; end
    class Unprepared < self; end

    MAP = begin
      CassandraCql.constants.inject({}) do |memo, name|
        name = name.to_s
        if name.index("ERROR_") == 0
          _code = CassandraCql.const_get(name) # Don't shadow enclosing scope's code
          parts = name.split("_")
          parts.shift
          class_name = parts.collect{ |part| part.capitalize }.join("")
          memo[_code] = const_get(class_name)
        end
        memo
      end
    end


  end
end
