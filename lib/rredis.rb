require 'json'
require 'benchmark'
class RReDis
  VERSION = '1.0.0'
  attr_reader :default_config
  
  def initialize
    @default_config = [{:steps=>60, :rows=>1440},
                       {:steps=>60, :rows=>10080, :aggregation=>"average", :xff=>0.5},
                       {:steps=>900, :rows=>2976, :aggregation=>"average", :xff=>0.5},
                       {:steps=>3600, :rows=>8760, :aggregation=>"average", :xff=>0.5}]

    @r = Redis.new
    @r.set 'rrd_default_config', JSON.dump(@default_config)
    @script_cache = {}
    @sha_cache = {}
    Dir.glob(File.join(File.dirname(__FILE__), '*.lua')).each do |file|
      name = File.basename(file, File.extname(file))
      @script_cache[name.to_sym] = File.open(file).read
      @sha_cache[name.to_sym] = @r.script('LOAD', @script_cache[name.to_sym])
    end
    
  end

  def config(metric, config)
    @r.set "rrd_#{metric}_config", JSON.dump(config)
  end

  def store(metric, value, timestamp=nil)
    @r.evalsha(@sha_cache[:store], 1, 'rrd_'+metric, value, timestamp.to_f)
  end

  def get(metric, start, stop)
    @r.evalsha(@sha_cache[:get], 1, 'rrd_'+metric, start.to_i, stop.to_i)
  end

  def pipeline(&block)
    @r.pipelined do
      yield self
    end
  end
end
