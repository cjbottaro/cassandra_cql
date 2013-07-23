module CassandraCql
  module Request
    class Options
      include Request

      def compressable?
        false
      end

    end
  end
end
