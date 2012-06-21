require 'spec_helper'

describe RReDis do
  describe "storing data" do
    let(:rrd) { RReDis.new }
    let(:r) { Redis.new }
    before(:all) do
      r.keys('rrd_*').each do |key|
        r.del key if key != 'rrd_default_config'
      end
    end
    it "should set the default config correctly" do
      JSON.parse(r.get('rrd_default_config'), :symbolize_names => true).should == rrd.default_config
    end

    it "should round the timestamp correctly" do
      rrd.config("timestamp", {:steps => 10, :rows => 3})
      rrd.store('timestamp', 100,   1)
      rrd.store('timestamp', 104.4, 2)
      rrd.store('timestamp', 106.6, 3)
      r.zrange('rrd_timestamp_10', 0, -1).should == ["100_2", "110_3"]
      r.zcard('rrd_timestamp_10').should == 2
    end

    it "should correctly store three points" do 
      rrd.config("test2", {:steps => 1, :rows => 3})
      rrd.store('test2', Time.now,   1)
      rrd.store('test2', Time.now+1, 2)
      rrd.store('test2', Time.now+2, 3)
      r.zcard('rrd_test2_1').should == 3
    end

    it "should not store a point which timestamp is too far in the past" do
      rrd.config("past", {:steps => 1, :rows => 3})
      rrd.store('past', 10, 1)
      rrd.store('past', 11, 2)
      rrd.store('past',  8, 3)
      r.zcard('rrd_past_1').should == 2
    end

    it "should store no more then three points" do 
      rrd.config("test3", {:steps => 1, :rows => 3})    
      rrd.store('test3', 20, 1)
      rrd.store('test3', 21, 2)
      rrd.store('test3', 22, 3)
      rrd.store('test3', 23, 4)
      r.zcard('rrd_test3_1').should == 3
      r.zrange('rrd_test3_1', 0, -1).should == ["21_2", "22_3", "23_4"]
    end
    
    it "should store floats" do 
      rrd.config("floats", {:steps => 1, :rows => 3})    
      rrd.store('floats', 20, 1.1)
      rrd.store('floats', 21, 2.2)
      rrd.store('floats', 22, 3.312345678)
      rrd.store('floats', 23, 4.444455)
      r.zcard('rrd_floats_1').should == 3
      r.zrange('rrd_floats_1', 0, -1).should == ["21_2.2", "22_3.312345678", "23_4.444455"]
    end

    it "should generate the averages correctly" do 
      rrd.config("test4", {:steps => 1, :rows => 3, :aggregations => ['average'], :rra => [{:steps => 3, :rows => 10, :xff => 0.0}]})    
      rrd.store('test4', 1, 1)
      r.zrange('rrd_test4_3_average', 0, 0).first.should == "3_1"
      rrd.store('test4', 2, 2)
      r.zrange('rrd_test4_3_average', 0, 0).first.should == "3_1.5"
      rrd.store('test4', 3, 3)
      r.zrange('rrd_test4_3_average', 0, 0).first.should == "3_2"
      r.zcard('rrd_test4_1').should == 3
      r.zcard('rrd_test4_3_average').should == 1
      rrd.store('test4', 4, 1)
      r.zrange('rrd_test4_3_average', 0, 0).first.should == "3_2"
      r.zrange('rrd_test4_3_average', 1, 1).first.should == "6_1"
      r.zcard('rrd_test4_3_average').should == 2
    end

    it "should work with more then one rra" do 
      rrd.config("test5", {:steps => 1, :rows => 3, :aggregations => ['average'], 
                              :rra => [{:steps => 3, :rows => 10, :xff => 0.1},
                                       {:steps => 6, :rows => 10, :xff => 0.1}]})
      1.upto(6) do |x|
        rrd.store('test5', x, x)
      end
      r.zcard('rrd_test5_1').should == 3
      r.zcard('rrd_test5_3_average').should == 2
      r.zrange('rrd_test5_3_average', 0, 0).first.should == "3_2"
      r.zrange('rrd_test5_3_average', 1, 1).first.should == "6_5"
      r.zcard('rrd_test5_6_average').should == 1
      r.zrange('rrd_test5_6_average', 0, 0).first.should == "6_3.5"
      r.zcard('rrd_test5_3_average').should == 2
    end

    it "should pipeline multiple store commands" do
      rrd.config("pipeline", {:steps => 1, :rows => 3})
      rrd.pipelined do |rrd|
        rrd.store('pipeline', Time.now,   1)
        rrd.store('pipeline', Time.now+1, 2)
        rrd.store('pipeline', Time.now+2, 3)
        rrd.store('pipeline', Time.now+3, 4)
      end
      r.zcard('rrd_pipeline_1').should == 3    
    end

    it "should find the minima correctly" do 
      rrd.config("min", {:steps => 1, :rows => 3, :aggregations => ['min'], :rra => [{:steps => 3, :rows => 10, :xff => 0.1}]})    
      rrd.store('min', 1, 5)
      rrd.store('min', 2, 2)
      rrd.store('min', 3, 7)
      r.zcard('rrd_min_1').should == 3
      r.zcard('rrd_min_3_min').should == 1
      rrd.store('min', 4, 5)
      r.zrange('rrd_min_3_min', 0, 0).first.should == "3_2"
      r.zrange('rrd_min_3_min', 1, 1).first.should == "6_5"
      r.zcard('rrd_min_3_min').should == 2      
    end

    it "should find the maxima correctly" do 
      rrd.config("max", {:steps => 1, :rows => 3, :aggregations => ['max'], :rra => [{:steps => 3, :rows => 10, :xff => 0.1}]})    
      rrd.store('max', 1, 5)
      rrd.store('max', 2, 2)
      rrd.store('max', 3, 7)
      r.zcard('rrd_max_1').should == 3
      r.zcard('rrd_max_3_max').should == 1
      rrd.store('max', 4, 5)
      r.zrange('rrd_max_3_max', 0, 0).first.should == "3_7"
      r.zrange('rrd_max_3_max', 1, 1).first.should == "6_5"
      r.zcard('rrd_max_3_max').should == 2
    end

    it "should find the sum correctly" do 
      rrd.config("sum", {:steps => 1, :rows => 3, :aggregations => ['sum'], :rra => [{:steps => 3, :rows => 10, :xff => 0.1}]})    
      rrd.store('sum', 1, 5)
      rrd.store('sum', 2, 2)
      rrd.store('sum', 3, 7)
      r.zcard('rrd_sum_1').should == 3
      r.zcard('rrd_sum_3_sum').should == 1
      rrd.store('sum', 4, 5)
      r.zrange('rrd_sum_3_sum', 0, 0).first.should == "3_14"
      r.zrange('rrd_sum_3_sum', 1, 1).first.should == "6_5"
      r.zcard('rrd_sum_3_sum').should == 2
    end

    it "should handle multiple aggregation methods correctly" do
      rrd.config("mult", {:steps => 1, :rows => 3, :aggregations => ['average', 'max', 'min'], :rra => [{:steps => 3, :rows => 10, :xff => 0.1},
                                                            {:steps => 3, :rows => 10, :xff => 0.1}]})
      rrd.store('mult', 1, 1)
      rrd.store('mult', 2, 1)
      rrd.store('mult', 3, 2)
      r.zcard("rrd_mult_1").should == 3
      r.zcard("rrd_mult_3_average").should == 1
      r.zrange('rrd_mult_3_average', 0, 0).first.should == "3_1.3333333333333"
      r.zcard("rrd_mult_3_min").should == 1
      r.zrange('rrd_mult_3_min', 0, 0).first.should == "3_1"



    end
  end
end