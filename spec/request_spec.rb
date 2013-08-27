# encoding: UTF-8

require "spec_helper"

describe CassandraCql::Request do

  context "#header" do

    it "calculates the correct length when body contains multi-byte UTF-8 characters" do
      multi_byte_string = "I’ve"
      length_in_bytes = multi_byte_string.dup.force_encoding(Encoding::BINARY).length
      request = CassandraCql::Request::Query.new(multi_byte_string, CassandraCql::CONSISTENCY_QUORUM)
      request.header.length.should == 4+2+length_in_bytes
    end

  end
end
