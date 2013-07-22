module CassandraCql
  module Helper
    extend self

    def consistency_code(symbol)
      CassandraCql.const_get("CONSISTENCY_#{symbol.to_s.upcase}")
    end

    def frame_error_to_execption(frame)
      constant_name = CassandraCql.constants.detect{ |name| CassandraCql.const_get(name) == frame.code }
      exception_name = constant_name.to_s.split("_").tap{ |parts| parts.shift }.collect{ |part| part.capitalize }.join("")
      Error.const_get(exception_name).new(frame.code, frame.message)
    end

  end
end
