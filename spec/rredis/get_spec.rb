require 'spec_helper'

describe RReDis do
  describe "getting data" do
    let(:rrd) { RReDis.new }
    let(:r) { Redis.new }
    before(:all) do
      r.keys('rrd_*').each do |key|
        r.del key if key != 'rrd_default_config'
      end
      rrd.config("test", {:steps => 10, :rows => 3, :aggregations => ['average', 'min'], :rra => [{:steps => 30, :rows => 3, :xff => 0.5},
                                                             {:steps => 60, :rows => 3, :xff => 0.5}]})
      value = 1
      (10..270).step(10).each_slice(3) do |a|
        a.each do |ts|
          rrd.store('test', ts, value)
        end
        value += 1
      end
    end

    it "should return values in the native resolution" do 
      rrd.get("test", 250, 270).should == [[250, 260, 270], [9, 9, 9]]
    end

    it "should return values of the first rra" do
      rrd.get("test", 210, 270).should == [[195, 225, 255], [7, 8, 9]]
    end

    it "should return values of the second rra" do
      rrd.get("test", 0, 270).should == [[30, 90, 150, 210], [1.5, 3.5, 5.5, 7.5]]
    end

    it "should return the min values if requested" do
      rrd.get("test", 0, 270, 'min').should == [[30, 90, 150, 210], [1.0, 3.0, 5.0, 7.0]]
    end
    
    it "should return floats" do 
      rrd.config("floats", {:steps => 1, :rows => 3})    
      rrd.store('floats', 20, 1.1)
      rrd.store('floats', 21, 2.2)
      rrd.store('floats', 22, 3.312345678)
      r.zcard('rrd_floats_1').should == 3
      rrd.get("floats", 20, 22).should == [[20, 21, 22], [1.1, 2.2, 3.312345678]]
    end
  end
end