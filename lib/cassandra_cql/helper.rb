module CassandraCql
  module Helper
    extend self

    def consistency_code(symbol)
      CassandraCql.const_get("CONSISTENCY_#{symbol.to_s.upcase}")
    end

  end
end
