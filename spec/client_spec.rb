require "spec_helper"

module CassandraCql

  describe Client do

    let(:client){ described_class.new.tap{ |client| client.query("USE cassandra_cql_test") } }

    context "#comm" do

      it "reconnects if there is an error" do
        connection = client.connection
        mock(client).comm_without_reconnect(anything, anything, anything){ raise(Errno::EPIPE) }
        stub.proxy(client).comm_without_reconnect
        mock.proxy(client).reset_connection
        client.query("SELECT * FROM test")
        client.connection.should_not == connection
      end

    end

    context "#new" do

      it "sends a startup frame and receives a ready frame" do
        client = nil
        expect{ client = described_class.new }.to_not raise_error
        client.last_request.should be_kind_of(Request::Startup)
        client.last_response.should be_an_instance_of(Frame::Ready)
      end

    end

    context "#supported" do

      it "gets the server supported options" do
        response = client.instance_eval{ comm(Request::Options) }
        response.should be_instance_of(Frame::Supported)
        response.options["CQL_VERSION"].should_not be_empty
        response.options["COMPRESSION"].should_not be_empty
      end

    end

    context "#query" do

      context "use" do

        let(:client){ described_class.new }

        it "sets the keyspace" do
          response = client.query("use cassandra_cql_test")
          response.should_not be_error
          response.should be_instance_of(Frame::Result::SetKeyspace)
          response.name.should == "cassandra_cql_test"
        end

      end

      context "select" do

        it "casts data types properly (1)" do
          row = client.query("SELECT * FROM test WHERE ascii = '1'").rows.first
          row["ascii"].should == "1"
          row["bigint"].should == 1
          row["blob"].should == "0x68656c6c"
          row["boolean"].should == true
          row["decimal"].should == BigDecimal.new("12.345")
          row["double"].should be_within(0.00000001).of(1.5)
          row["float"].should be_within(0.00000001).of(1.5)
          row["inet"].should == "8.8.8.8"
          row["text"].should == "text"
          row["timestamp"].should be_a(Time)
          row["timeuuid"].should be_a(Time)
          row["uuid"].should == "1766a37c-e58e-11e2-8aa1-6e97c8bbb552"
          row["varchar"].should == "text"
          row["varint"].should == 1
          row["nil"].should be_nil
        end

        it "casts data types properly (2)" do
          row = client.query("SELECT * FROM test WHERE ascii = '-1'").rows.first
          row["ascii"].should == "-1"
          row["bigint"].should == -1
          row["blob"].should == "0x68656c6c"
          row["boolean"].should == true
          row["decimal"].should == BigDecimal.new("-12.345")
          row["double"].should be_within(0.00000001).of(-1.5)
          row["float"].should be_within(0.00000001).of(-1.5)
          row["inet"].should == "1050:0000:0000:0000:0005:0600:300c:326b"
          row["text"].should == "text"
          row["timestamp"].should be_a(Time)
          row["timeuuid"].should be_a(Time)
          row["uuid"].should == "1766a37c-e58e-11e2-8aa1-6e97c8bbb552"
          row["varchar"].should == "text"
          row["varint"].should == -1
          row["nil"].should be_nil
        end

        it "works with compression" do
          client = CassandraCql::Client.new(:compression => true)
          client.query("USE cassandra_cql_test")
          rows = client.query("SELECT * FROM test LIMIT 1000")
          rows.length.should == 1000
          rows.header.should be_compression
        end

      end

      context "add/drop index" do

        before(:each) do
          begin
            client.query("DROP INDEX test_nil_idx")
          rescue Error::Invalid => e
            nil
          end
        end

        it "returns a schema change result" do
          response = client.query("CREATE INDEX ON test(nil)")
          response.should be_instance_of(Frame::Result::SchemaChange)
          response.change.should == "UPDATED"
          response.keyspace.should == "cassandra_cql_test"
          response.table.should == "test"
        end

      end

    end

    context "#prepare" do

      it "prepares a statement to be executed" do
        response = client.prepare("select * from test limit 1")
        response.should_not be_error
        response.should be_instance_of(Frame::Result::Prepared)
      end

    end

    context "#execute" do

      it "works without bind variables" do
        statement = client.prepare("select * from test limit 1")
        response = client.execute(statement)
        response.should_not be_error
        response.should be_instance_of(Frame::Result::Rows)
        response.rows.length.should == 1
      end

      it "works with with a single bind variable" do
        statement = client.prepare("select * from test where ascii = ? limit 1")
        response = client.execute(statement, ["1"])
        response.should_not be_error
        response.should be_instance_of(Frame::Result::Rows)
        response.rows.length.should == 1
      end

      it "works with with multiple bind variables" do
        statement = client.prepare("select * from test where ascii = ? and int = ? limit 1")
        response = client.execute(statement, ["1", 1])
        response.should_not be_error
        response.should be_instance_of(Frame::Result::Rows)
        response.rows.length.should == 1
      end

      context "bind variables for" do

        let(:guid){ SimpleUUID::UUID.new.to_guid }

        after(:each) do
          client.query("DELETE FROM test WHERE ascii = '#{guid}'")
        end

        def in_out(value, name = nil)
          name ||= example.description
          statement = client.prepare("INSERT INTO test (ascii, #{name}) VALUES ('#{guid}', ?)")
          client.execute(statement, [value])
          rows = client.query("SELECT #{name} FROM test WHERE ascii = '#{guid}'")
          rows[0][name].tap do |value|
            client.query("DELETE FROM test WHERE ascii = '#{guid}'")
          end
        end

        def expect_symmetry(value, name = nil)
          in_out(value, name).should == value
        end

        def expect_asymmetry(value, expected, name = nil)
          in_out(value, name).should == expected
        end

        def expect_within(value, delta, name = nil)
          in_out(value, name).should be_within(delta).of(value)
        end

        it "bigint" do
          expect_symmetry(123456789123456789)
        end

        it "blob" do
          expect_symmetry("0x0123456789abcdef")
        end

        it "boolean" do
          expect_symmetry(true)
          expect_symmetry(false)
          expect_asymmetry("", true)
          expect_asymmetry(nil, false)
        end

        it "decimal" do
          expect_symmetry(BigDecimal.new("123.45"))
          expect_symmetry(BigDecimal.new("12.345"))
          expect_asymmetry(123.45, BigDecimal.new("123.45"))
          expect_asymmetry(12.345, BigDecimal.new("12.345"))

          expect_symmetry(BigDecimal.new("-123.45"))
          expect_symmetry(BigDecimal.new("-12.345"))
          expect_asymmetry(-123.45, BigDecimal.new("-123.45"))
          expect_asymmetry(-12.345, BigDecimal.new("-12.345"))
        end

        it "double" do
          expect_within(1234.56789, 0.0000001)
          expect_within(-1234.56789, 0.0000001)
        end

        it "float" do
          expect_within(1.2345, 0.0001)
          expect_within(-1.2345, 0.0001)
        end

        it "int" do
          expect_symmetry(123456789)
          expect_symmetry(-123456789)
        end

        it "text" do
          expect_symmetry("blahtest")
        end

        it "timestamp" do
          time = Time.now
          expected = Time.at((time.to_f * 1000).to_i.to_f / 1000)
          expect_asymmetry(time, expected)
        end

        it "uuid" do
          expect_symmetry(SimpleUUID::UUID.new.to_guid)
        end

        it "varchar" do
          expect_symmetry("blahtest")
        end

        it "varint" do
          expect_symmetry(123456789)
          expect_symmetry(-123456789)
        end

        it "timeuuid" do
          uuid = SimpleUUID::UUID.new
          expect_asymmetry(uuid.to_guid, uuid.to_time)
        end

        it "inet" do
          expect_symmetry("1.3.4.5", "inet")
          expect_symmetry("1111:2222:3333:4444:5555:6666:7777:8888", "inet")
        end

        it "list" do
          i1 = Time.now
          i2 = Time.now
          o1 = Time.at((i1.to_f * 1000).to_i.to_f / 1000)
          o2 = Time.at((i2.to_f * 1000).to_i.to_f / 1000)
          expect_asymmetry([i1, i2], [o1, o2])
        end

        it "map" do
          expect_symmetry("won" => 1, "too" => 2)
        end

        it "set" do
          _in = [12.345, 123.45]
          out = Set.new([BigDecimal.new("12.345"), BigDecimal.new("123.45")])
          expect_asymmetry(_in, out, "cet")
        end

      end

    end

    context "errors" do

      it "are raised as exceptions" do
        expect{ client.query("this is not cql") }.to raise_error(Error::Syntax)
      end

    end

  end

end
