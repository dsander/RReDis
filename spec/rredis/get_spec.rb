require 'spec_helper'

describe RReDis do
  describe "getting data" do
    let(:rrd) { RReDis.new }
    let(:r) { Redis.new }
    before(:all) do
      r.keys('rrd_*').each do |key|
        r.del key if key != 'rrd_default_config'
      end
      rrd.config("test", {:steps => 10, :rows => 3, :rra => [{:steps => 30, :rows => 3, :aggregation => 'average', :xff => 0.5},
                                                             {:steps => 60, :rows => 3, :aggregation => 'average', :xff => 0.5}]})
      value = 1
      (10..270).step(10).each_slice(3) do |a|
        a.each do |ts|
          rrd.store('test', value, ts)
        end
        value += 1
      end
    end

    it "should work with more then one rra" do 
      rrd.get("test", 250, 270).should == [9, 250, 9, 260, 9, 270]
    end

    it "should stuff" do
      rrd.get("test", 210, 270).should == [7, 210, 8, 240, 9, 270]
    end

    it "stuff" do
      rrd.get("test", 0, 270).should == [3, 120, 5, 180, 7, 240]
    end

  end
end