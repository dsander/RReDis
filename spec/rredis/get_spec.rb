require 'spec_helper'

describe RReDis do
  describe "getting data" do
    let(:rrd) { RReDis.new }
    let(:r) { Redis.new }
    before(:all) do
      r.keys('rrd_*').each do |key|
        r.del key if key != 'rrd_default_config'
      end
      rrd.config("test", {:steps => 1, :rows => 3, :rra => [{:steps => 2, :rows => 3, :aggregation => 'average', :xff => 0.1},
                                                             {:steps => 3, :rows => 3, :aggregation => 'average', :xff => 0.1}]})
      1.upto(9) do |x|
        rrd.store('test', x+10, x)
      end
    end

    it "should work with more then one rra" do 
      rrd.get("test", 7, 9).should == ["17", "7", "18", "8", "19", "9"]
    end

  end
end