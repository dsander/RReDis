require 'json'
require 'redis'

class RReDis
  VERSION = '0.1.0'
  attr_reader :default_config
  
  def initialize(options = {})
    @default_config = {:steps=>10, :rows=>1440, :aggregations=>["average", "min", "max"], 
                       :rra => [ {:steps=>60, :rows=>10080, :xff=>0.5},
                                 {:steps=>900, :rows=>2976, :xff=>0.5},
                                 {:steps=>3600, :rows=>8760, :xff=>0.5}]}

    @r = Redis.new
    @r.set 'rrd_default_config', JSON.dump(@default_config)
    @script_cache = {}
    @sha_cache = {}
    Dir.glob(File.join(File.dirname(__FILE__), '/lua/*.lua')).each do |file|
      name = File.basename(file, File.extname(file))
      @script_cache[name.to_sym] = File.open(file).read
      @sha_cache[name.to_sym] = @r.script(:load, @script_cache[name.to_sym])
    end
    
  end

  def config(metric, config)
    config[:rra] = config[:rra].sort {|a,b| a[:steps] <=> b[:steps] } if config[:rra]
    @r.set "rrd_#{metric}_config", JSON.dump(config)
    @r.sadd "rrd_metrics_set", metric
  end

  def store(metric, timestamp, value=nil)
    if value.nil?
      value = timestamp
      timestamp = Time.now
    end
    @r.evalsha(@sha_cache[:store], :keys => ['rrd_'+metric], :argv => [value, timestamp.to_f])
  end

  def get(metric, start, stop, method = nil)
    resp = @r.evalsha(@sha_cache[:get], :keys => ['rrd_'+metric], :argv => [start.to_i, stop.to_i, method])
    if resp
      resp[1].collect! { |x| x.to_f} 
      resp
    else
      [[],[]]
    end
  end

  def pipelined(&block)
    @r.pipelined do
      yield self
    end
  end
end
