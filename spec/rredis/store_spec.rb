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

    it "should store a data point" do
      i = 0
      60.times do |i|
      #  rrd.store('test', i, Time.now+i)
      end
      #r.zcard('rrd_test_1').should == 1
    end

    it "should round the timestamp correctly" do
      rrd.config("timestamp", {:steps => 10, :rows => 3})
      rrd.store('timestamp', 1, 100)
      rrd.store('timestamp', 2, 104.4)
      rrd.store('timestamp', 3, 106.6)
      r.zrange('rrd_timestamp_10', 0, -1).should == ["2", "3"]
      r.zcard('rrd_timestamp_10').should == 2
    end

    it "should round the timestamp correctly 2" do
      rrd.config("timestamp2", {:rows => 3})
      rrd.store('timestamp2', 1, 100)
      rrd.store('timestamp2', 2, 104.4)
      rrd.store('timestamp2', 3, 106.5)
      r.zrange('rrd_timestamp2_free', 0, -1, :with_scores => true).should == ["1", "100", "2", "104", "3", "107"]
      r.zcard('rrd_timestamp2_free').should == 3
    end

    it "should correctly store three points" do 
      rrd.config("test2", {:steps => 1, :rows => 3})
      rrd.store('test2', 1, Time.now)
      rrd.store('test2', 2, Time.now+1)
      rrd.store('test2', 3, Time.now+2)
      r.zcard('rrd_test2_1').should == 3
    end

    it "should not store a point which timestamp is too far in the past" do
      rrd.config("past", {:steps => 1, :rows => 3})
      rrd.store('past', 1, 10)
      rrd.store('past', 2, 11)
      rrd.store('past', 3, 8)
      r.zcard('rrd_past_1').should == 2
    end

    it "should not store a point which timestamp is too far in the past with not steps" do
      rrd.config("past2", {:rows => 3})
      rrd.store('past2', 1, 10)
      rrd.store('past2', 2, 11)
      rrd.store('past2', 3, 12)
      rrd.store('past2', 4, 8)
      r.zcard('rrd_past2_free').should == 3
    end

    it "should store no more then three points" do 
      rrd.config("test3", {:steps => 1, :rows => 3})    
      rrd.store('test3', 1, Time.now)
      rrd.store('test3', 2, Time.now+1)
      rrd.store('test3', 3, Time.now+2)
      rrd.store('test3', 4, Time.now+3)
      r.zcard('rrd_test3_1').should == 3
    end

    it "should generate the averages correctly" do 
      rrd.config("test4", {:steps => 1, :rows => 3, :rra => [{:steps => 3, :rows => 10, :aggregation => 'average', :xff => 0.1}]})    
      rrd.store('test4', 1, 60)
      r.zrange('rrd_test4_3_average', 0, 0).first.to_f.should == 1
      rrd.store('test4', 2, 61)
      r.zrange('rrd_test4_3_average', 0, 0).first.to_f.should == 1.5
      rrd.store('test4', 3, 62)
      r.zrange('rrd_test4_3_average', 0, 0).first.to_f.should == 2
      r.zcard('rrd_test4_1').should == 3
      r.zcard('rrd_test4_3_average').should == 1
      rrd.store('test4', 1, 63)
      r.zrange('rrd_test4_3_average', 1, 1).first.to_f.should == 1
      r.zcard('rrd_test4_3_average').should == 2
      r.zrange('rrd_test4_3_average', 0, 0).first.to_f.should == 2
    end

    it "should work with more then one rra" do 
      rrd.config("test5", {:steps => 1, :rows => 3, :rra => [{:steps => 3, :rows => 10, :aggregation => 'average', :xff => 0.1},
                                                             {:steps => 6, :rows => 10, :aggregation => 'average', :xff => 0.1}]})
      60.upto(65) do |x|
        rrd.store('test5', x-59, x)
      end
      r.zcard('rrd_test5_1').should == 3
      r.zcard('rrd_test5_3_average').should == 2
      r.zrange('rrd_test5_3_average', 0, 0).first.to_f.should == 2
      r.zrange('rrd_test5_3_average', 1, 1).first.to_f.should == 5
      r.zcard('rrd_test5_6_average').should == 1
      r.zrange('rrd_test5_6_average', 0, 0).first.to_f.should == 3.5
      r.zcard('rrd_test5_3_average').should == 2
    end

    it "should work without specifying steps" do
      rrd.config("test6", {:rows => 3, :rra => [{:steps => 3, :rows => 10, :aggregation => 'average', :xff => 0.1}]})    
      rrd.store('test6', 1, 60)
      rrd.store('test6', 2, 61)
      rrd.store('test6', 3, 62)
      r.zcard('rrd_test6_free').should == 3
      r.zcard('rrd_test6_3_average').should == 1
      rrd.store('test6', 1, 63)
      r.zcard('rrd_test6_3_average').should == 2
    end

    it "should pipeline multiple store commands" do
      rrd.config("pipeline", {:steps => 1, :rows => 3})
      rrd.pipeline do |rrd|
        rrd.store('pipeline', 1, Time.now)
        rrd.store('pipeline', 2, Time.now+1)
        rrd.store('pipeline', 3, Time.now+2)
        rrd.store('pipeline', 4, Time.now+3)
      end
      r.zcard('rrd_pipeline_1').should == 3    
    end

    it "should find the minima correctly" do 
      rrd.config("min", {:steps => 1, :rows => 3, :rra => [{:steps => 3, :rows => 10, :aggregation => 'min', :xff => 0.1}]})    
      rrd.store('min', 5, 60)
      rrd.store('min', 2, 61)
      rrd.store('min', 7, 62)
      r.zcard('rrd_min_1').should == 3
      r.zcard('rrd_min_3_min').should == 1
      rrd.store('min', 5, 63)
      r.zrange('rrd_min_3_min', 1, 1).first.to_f.should == 5
      r.zcard('rrd_min_3_min').should == 2
      r.zrange('rrd_min_3_min', 0, 0).first.to_f.should == 2
    end

    it "should find the maxima correctly" do 
      rrd.config("max", {:steps => 1, :rows => 3, :rra => [{:steps => 3, :rows => 10, :aggregation => 'max', :xff => 0.1}]})    
      rrd.store('max', 5, 60)
      rrd.store('max', 2, 61)
      rrd.store('max', 7, 62)
      r.zcard('rrd_max_1').should == 3
      r.zcard('rrd_max_3_max').should == 1
      rrd.store('max', 5, 63)
      r.zrange('rrd_max_3_max', 1, 1).first.to_f.should == 5
      r.zcard('rrd_max_3_max').should == 2
      r.zrange('rrd_max_3_max', 0, 0).first.to_f.should == 7
    end
  end
end