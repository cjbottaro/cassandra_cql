module CassandraCql
  module Notifications
    extend(self)

    def instrument_active_support(name, payload = {}, &block)
      ActiveSupport::Notifications.instrument(name, payload, &block)
    end

    def instrument_self(name, payload = {}, &block)
      block.call(payload)
    end

    module_eval do
      if defined?(ActiveSupport::Notifications)
        alias_method(:instrument, :instrument_active_support)
      else
        alias_method(:instrument, :instrument_self)
      end
    end

  end
end
