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
          rrd.store('test', ts, value)
        end
        value += 1
      end
    end

    it "should work with more then one rra" do 
      rrd.get("test", 250, 270).should == [[250, 260, 270], [9, 9, 9]]
    end

    it "should stuff" do
      rrd.get("test", 210, 270).should == [[195, 225, 255], [7, 8, 9]]
    end

    it "stuff" do
      rrd.get("test", 0, 270).should == [[90, 150, 210], [3, 5, 7]]
    end

  end
end