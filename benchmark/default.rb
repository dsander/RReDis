require 'rredis'
require 'benchmark'

r = Redis.new
r.flushall
r.config 'resetstat'

rrd      = RReDis.new
memory   = r.info["used_memory"].to_i
commands = r.info["total_commands_processed"].to_i

# Define who much data we want to store
days  = 365
steps = 30

# Calculate the amount of measurements we need to store
day   = 3600*24
start = Time.now-(day*days)
n     = day*days/steps

# We regenerate the data to just measure the inserts
data = []
n.times do |i|
  data << [(start+(i*steps)).to_f, rand(100_0)]
end

# Do the actual work
res = Benchmark.realtime do
  #rrd.pipelined do
  n.times do |i|
    d = data.pop
    rrd.store "bench", d[0], d[1]
  end
  #end
end

puts "#{res/n}ms per op, #{1/(res/n)} op/s"

# Calculate the amount of measurements we actually stored
stored_meassuremnts = 0
r.keys("rrd_bench*").each do |k|
  next if k.include? 'config'
  if !k.include? 'min' and !k.include? 'max'
    stored_meassuremnts += r.zcard(k) 
  end
end

used_memory               = r.info["used_memory"].to_i - memory
commands_processed        = r.info["total_commands_processed"].to_i - commands

bytes_per_measurement     = used_memory/stored_meassuremnts
stored_measuremnts_per_gb = (1*1024*1024*1024).to_f/bytes_per_measurement
stored_metrics_per_gb     = (1*1024*1024*1024).to_f/used_memory
commands_per_measurement  =  commands_processed/n

puts "#{res}s for for #{n} inserts"
puts "#{stored_meassuremnts} stored measurements/aggregations"
puts "#{bytes_per_measurement} bytes used per stored measurement"
puts "#{stored_measuremnts_per_gb.to_i} measurements storable per gb of ram"
puts "#{stored_metrics_per_gb.to_i} metrics storable per gb of ram"
puts "#{commands_processed} redis commands performed"
puts "#{commands_per_measurement} redis commands performed per stored measurement"
