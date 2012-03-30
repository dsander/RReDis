-- Check if there is a config for this metric
if redis.call("exists",KEYS[1] .. "_config") == 0 then
  -- Create a config based on the default one
  config = redis.call("get", "rrd_default_config")
  redis.call("set", KEYS[1] .. "_config", config)
end
-- Load the config
config = cjson.decode(redis.call("get", KEYS[1] .. "_config"))

start = tonumber(ARGV[1])
stop = tonumber(ARGV[2])

if config["steps"] then
  higher_key = KEYS[1]..'_'..config["steps"]
else
  higher_key = KEYS[1]..'_free'
end
redis.log(redis.LOG_NOTICE, "ZRANGEBYSCORE", higher_key, start, stop, 'WITHSCORES')
data = redis.call("ZRANGEBYSCORE", higher_key, start, stop, 'WITHSCORES' )
if table.getn(data) > 0 then
  return data
end

if config["rra"] then
  higher = config
  for i, rra in ipairs(config["rra"]) do
    -- Get all entries from the higher precision bucket
    key = KEYS[1]..'_'..rra["steps"]..'_'..rra["aggregation"]
    data = redis.call("ZRANGEBYSCORE", key, start, stop, 'WITHSCORES' )
    if table.getn(data) > 0 then
      return data
    end
  end
end
