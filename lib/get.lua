function get_value(value, ts) 
  n = string.find(value, '_');
  if n then
    return tonumber(string.sub(value, n+1, -1))
  else
    return tonumber(value)
  end
end
function get_data(data)
  t = {}
  for i, d in ipairs(data) do
    --redis.log(redis.LOG_NOTICE, i, d, get_value(d, i))
    table.insert(t, get_value(d, i))
  end
  --redis.log(redis.LOG_NOTICE, cjson.encode(data))
  return t
end

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

higher_key = KEYS[1]..'_'..config["steps"]

oldest = redis.call("ZRANGE", higher_key, 0, 0, 'WITHSCORES')
if not oldest then
  return {}
end

oldest = tonumber(oldest[2])
if oldest <= start then
  --redis.log(redis.LOG_NOTICE, "yeah")
  --redis.log(redis.LOG_NOTICE, start, stop, oldest, config.steps, config.rows)
  if oldest+(config.steps*(config.rows-1)) <= stop then
    data = redis.call("ZRANGEBYSCORE", higher_key, start, stop, 'WITHSCORES' )
    return get_data(data)
  end
end

if config["rra"] then
  higher = config
  rra_count = table.getn(config.rra)
  for i, rra in ipairs(config["rra"]) do
    -- Get all entries from the higher precision bucket
    key = KEYS[1]..'_'..rra["steps"]..'_'..rra["aggregation"]
    oldest = redis.call("ZRANGE", key, 0, 0, 'WITHSCORES')
    if not oldest then
      return {}
    end

    oldest = tonumber(oldest[2])
    if oldest <= start or i == rra_count then
      --redis.log(redis.LOG_NOTICE, "yeah")
      --redis.log(redis.LOG_NOTICE, start, stop, oldest, rra.steps, rra.rows)
      if oldest+(rra.steps*(rra.rows-1)) <= stop then
        data = redis.call("ZRANGEBYSCORE", key, start, stop, 'WITHSCORES' )
        return get_data(data)
      end
    end
  end
end
