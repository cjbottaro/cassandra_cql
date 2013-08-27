require "spec_helper"

module CassandraCql

  describe Connection do

    context "when initialized with a timeout" do

      it "raises an exception if takes too long" do
        thread = Thread.new do
          socket = TCPServer.new("localhost", 9043).accept
          sleep
        end

        connection = described_class.new("localhost", 9043, 0.1)
        expect{ connection.read(1) }.to raise_error(Errno::ETIMEDOUT)

        thread.kill
        thread.join
      end

      it "doesn't raise an error if server responds in time" do

        thread = Thread.new do
          socket = TCPServer.new("localhost", 9044).accept
          socket.sendmsg("hi")
          sleep
        end

        connection = described_class.new("localhost", 9044, 0.1)
        connection.read(2).should == "hi"

        thread.kill
        thread.join
      end

    end

  end

end
