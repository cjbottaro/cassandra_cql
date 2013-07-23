module CassandraCql
  module Error

    class Base < RuntimeError

      attr_reader :code

      def initialize(code, message)
        super(message)
        @code = code
      end

    end

    class Server < Base; end
    class Protocol < Base; end
    class BadCredentials < Base; end
    class Unavailable < Base; end
    class Overloaded < Base; end
    class IsBootstrapping < Base; end
    class Truncate < Base; end
    class WriteTimeout < Base; end
    class ReadTimeout < Base; end
    class Syntax < Base; end
    class Unauthorized < Base; end
    class Invalid < Base; end
    class Config < Base; end
    class AlreadyExists < Base; end
    class Unprepared < Base; end


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
