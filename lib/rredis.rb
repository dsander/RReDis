require 'json'
require 'benchmark'
class RReDis
  VERSION = '1.0.0'
  attr_reader :default_config
  SCRIPT_STORE = File.open(File.join(File.dirname(__FILE__), 'store.lua')).read
  def initialize
    @default_config = {:next_epoch => nil, :steps => 1, :rows => 10, 
                       :rra => [:aggregation => 'average', :xff => 0.5, :next_epoch => nil, :steps => 60, :rows => 6]}
    @r = Redis.new
    @r.set 'rrd_default_config', JSON.dump(@default_config)
    @script_store = @r.script('LOAD', SCRIPT_STORE)
  end
  def config(metric, config)
    @r.set "rrd_#{metric}_config", JSON.dump(config)
  end
  def store(metric, value, timestamp=nil)
    #res = Benchmark.realtime do
      #@r.eval(RReDis::SCRIPT_STORE, 1, 'rrd_'+metric, value, timestamp.to_f)
      @r.evalsha(@script_store, 1, 'rrd_'+metric, value, timestamp.to_f)
    #end
    #puts res
  end
end
