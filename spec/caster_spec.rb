require "spec_helper"

module CassandraCql
  describe Caster do

    def expect_symmetry(input)
      cast(input).should == input
    end

    def cast(input, collection_type = nil)
      type = CassandraCql.const_get(example.description)
      bytes = described_class.to_bytes(input, type, collection_type)
      described_class.from_bytes(bytes, type, collection_type)
    end

    context "works for" do

      it "COLUMN_TYPE_ASCII" do
        result = cast("blahtest")
        result.should == "blahtest"
        result.encoding.should == Encoding::BINARY
      end


      it "COLUMN_TYPE_BIGINT" do
        expect_symmetry(1234567890123456789)
      end


      it "COLUMN_TYPE_BOOLEAN" do
        expect_symmetry(true)
        expect_symmetry(false)

        cast(1).should be_true
        cast("").should be_true

        cast(nil).should be_false
      end

      it "COLUMN_TYPE_DECIMAL" do
        expect_symmetry(BigDecimal.new("123.45"))
        cast(12.345).should == BigDecimal.new("12.345")

        expect_symmetry(BigDecimal.new("-123.45"))
        cast(-123.45).should == BigDecimal.new("-123.45")
      end

      it "COLUMN_TYPE_VARINT" do
        expect_symmetry(1234567890)
        expect_symmetry(-1234567890)
      end

    end

  end
end
