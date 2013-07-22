module CassandraCql
  module Response

    def self.included(mod)
      mod.extend(ClassMethods)
    end

    def initialize(buffer, header = nil)
      @header = header
    end

    def response?
      true
    end

    def request?
      false
    end

    def error?
      instance_of?(Frame::Error)
    end

    module ClassMethods

      def read(buffer, header)
        new(buffer, header)
      end

    end

  end
end
