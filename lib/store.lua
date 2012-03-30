-- Check if there is a config for this metric
if redis.call("exists",KEYS[1] .. "_config") == 0 then
  -- Create a config based on the default one
  config = redis.call("get", "rrd_default_config")
  redis.call("set", KEYS[1] .. "_config", config)
end
-- Load the config
config = cjson.decode(redis.call("get", KEYS[1] .. "_config"))

timestamp = tonumber(ARGV[2])

-- If steps are defined for the native resolution, we will round the timestamp 
if config["steps"] then
  higher_key = KEYS[1]..'_'..config["steps"]
  if (timestamp % config["steps"]) / config["steps"] <= 0.5 then
    timestamp = math.floor(timestamp - (timestamp % config["steps"]))
  else
    timestamp = math.floor(timestamp - (timestamp % config["steps"])) + config["steps"]
  end
else
  higher_key = KEYS[1]..'_free'
  timestamp = math.floor(timestamp + 0.5)
end

-- Get the amount of entries in this bucket
count = redis.call("ZCARD", higher_key)

-- We need to make sure that to old entries are not added to the bucked
if count+1 == config["rows"] then
  oldest = tonumber(redis.call("ZRANGE", higher_key, 0, 0, 'WITHSCORES')[2])
  if timestamp < oldest then
    -- We cannot add older entries 
    return false
  end
end

-- We may be updating an old entry, which we want to delete first
redis.call("ZREMRANGEBYSCORE", higher_key, timestamp, timestamp)

-- Add the new entry to the bucked
redis.call("ZADD", higher_key, timestamp, ARGV[1])

if count+1 > config["rows"] then 
  -- We have too many entries in this bucket - remove the oldest
  redis.call("ZREMRANGEBYRANK", higher_key, 0, (count-config.rows))
end

if config["rra"] then
  higher = config
  for i, rra in ipairs(config["rra"]) do
    -- Calculate the timestamp for the aggregation
    lower_start = timestamp - (timestamp % rra["steps"])
    
    -- Get all entries from the higher precision bucket
    data = redis.call("ZRANGEBYSCORE", higher_key, lower_start, lower_start+rra["steps"] )
    
    -- If steps are defined for the native resolution, only proceed if we have enough entries
    if higher["steps"] == nil or table.getn(data) > (rra["steps"]/higher["steps"]*rra["xff"]) then
      if rra["aggregation"] == "average" then
        sum = 0
        for i, value in ipairs(data) do
          --sum = sum + tonumber(string.sub(value, string.find(value, '-')+1, -1))
          sum = sum + tonumber(value)
        end
        value = sum / table.getn(data)     
      elseif rra["aggregation"] == "min" then
        min = 2^52
        for i, value in ipairs(data) do
          n = tonumber(value)
          if n < min then
            min = n 
          end
        end
        value = min
      elseif rra["aggregation"] == "max" then
        max = -2^52
        for i, value in ipairs(data) do
          n = tonumber(value)
          if n > max then
            max = n
          end
        end
        value = max
      else
        redis.log(redis.LOG_NOTICE, "Not implemented")
      end
    else
      -- We cannot insert any new data, lets bail
      return false
    end

    -- Already set the higher precision bucket to the current rra
    higher = rra
    higher_key = KEYS[1]..'_'..higher["steps"]..'_'..higher["aggregation"]

    -- We may be updating an old entry, which we want to delete first
    redis.call("ZREMRANGEBYSCORE", higher_key, lower_start, lower_start)
    
    -- Add the new entry to the bucked
    redis.call("ZADD", higher_key, lower_start, value)

    -- Get the amount of entries in this bucket
    count = redis.call("ZCARD", higher_key)

    if count > higher["rows"] then 
      -- We have too many entries in this bucket - remove the oldest
      oldest = redis.call("ZRANGE", higher_key, 0, 0)
      redis.call("ZREM", higher_key, oldest[1])
    end


  end
end

--redis.log(redis.LOG_NOTICE, cjson.encode(config["rra"]))
--redis.call("set", KEYS[1] .. "_config", cjson.encode(config))
return true