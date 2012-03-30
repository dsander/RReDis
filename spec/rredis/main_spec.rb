require 'spec_helper'

describe RReDis do
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

  it "should correctly store three points" do 
    rrd.config("test2", {:steps => 1, :rows => 3})
    rrd.store('test2', 1, Time.now)
    rrd.store('test2', 2, Time.now+1)
    rrd.store('test2', 3, Time.now+2)
    r.zcard('rrd_test2_1').should == 3
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
      puts x-59
    end
    r.zcard('rrd_test5_1').should == 3
    r.zcard('rrd_test5_3_average').should == 2
    r.zrange('rrd_test5_3_average', 0, 0).first.to_f.should == 2
    r.zrange('rrd_test5_3_average', 1, 1).first.to_f.should == 5
    r.zcard('rrd_test5_6_average').should == 1
    r.zrange('rrd_test5_6_average', 0, 0).first.to_f.should == 3.5
    r.zcard('rrd_test5_3_average').should == 2
  end



end