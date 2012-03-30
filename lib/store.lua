-- Check if there is a config for this metric
if redis.call("exists",KEYS[1] .. "_config") == 0 then
  -- Create a config based on the default one
  config = redis.call("get", "rrd_default_config")
  redis.call("set", KEYS[1] .. "_config", config)
end
-- Load the config
config = cjson.decode(redis.call("get", KEYS[1] .. "_config"))

higher_key = KEYS[1]..'_'..config["steps"]

if ARGV[2] == nil then
  timestamp = tonumber(redis.call("TIME"))
else
  timestamp = tonumber(ARGV[2])
end
if (timestamp % config["steps"]) / config["steps"] <= 0.5 then
  timestamp = math.floor(timestamp - (timestamp % config["steps"]))
else
  timestamp = math.floor(timestamp - (timestamp % config["steps"])) + config["steps"]
end

-- We may be updateing an old entry, which we want to delete first
redis.call("ZREMRANGEBYSCORE", higher_key, timestamp, timestamp)

-- Add the new entry to the bucked
redis.call("ZADD", higher_key, timestamp, ARGV[1])

-- Update the config with 
config["next_epoch"] = timestamp


-- Get the amount of entries in this bucket
count = redis.call("ZCARD", higher_key)

if count > config["rows"] then 
	-- We have too many entries in this bucket - remove the oldest
  pop = redis.call("ZRANGE", higher_key, 0, 0)
  redis.call("ZREM", higher_key, pop[1])
end

if config["rra"] then
  higher = config
  for i, rra in ipairs(config["rra"]) do
    --redis.log(redis.LOG_NOTICE, timestamp)
    --redis.log(redis.LOG_NOTICE, higher_key)
    --redis.log(redis.LOG_NOTICE, "rra start")

    -- Calculate the timestamp for the aggregation
    lower_start = timestamp - (timestamp % rra["steps"])
    
    -- Get all entries from the higher precision bucket
    data = redis.call("ZRANGEBYSCORE", higher_key, lower_start, lower_start+rra["steps"] )
    
    --redis.log(redis.LOG_NOTICE, lower_start)
    --redis.log(redis.LOG_NOTICE, cjson.encode(data))
    
    -- Only proceed if we have enough entries
    if table.getn(data) > (rra["steps"]/higher["steps"]*rra["xff"]) then
      if rra["aggregation"] == "average" then
        sum = 0
        for i, value in ipairs(data) do
          --sum = sum + tonumber(string.sub(value, 12, -1))
          --xxx = string.find(value, '-')
          --sum = sum + tonumber(string.sub(value, xxx+1, -1))
          sum = sum + tonumber(value)
        end
        value = sum / table.getn(data)     
        --redis.log(redis.LOG_NOTICE, cjson.encode(data))
        --redis.log(redis.LOG_NOTICE, sum, value)
      else
        redis.log(redis.LOG_NOTICE, "Not implemented")
      end
    else
      -- We cannot insert any new data, lets bail
      --redis.log(redis.LOG_NOTICE, timestamp .. "rra stop " .. lower_start)
      return false
    end

    -- Already set the higher precision bucket to the current rra
    higher = rra
    higher_key = KEYS[1]..'_'..higher["steps"]..'_'..higher["aggregation"]

    -- We may be updateing an old entry, which we want to delete first
    redis.call("ZREMRANGEBYSCORE", higher_key, lower_start, lower_start)
    
    -- Add the new entry to the bucked
    redis.call("ZADD", higher_key, lower_start, value)

    -- Get the amount of entries in this bucket
    count = redis.call("ZCARD", higher_key)

    if count > higher["rows"] then 
      -- We have too many entries in this bucket - remove the oldest
      pop = redis.call("ZRANGE", higher_key, 0, 0)
      redis.call("ZREM", higher_key, pop[1])
    end


  end
end

--redis.log(redis.LOG_NOTICE, cjson.encode(config["rra"]))
--redis.call("set", KEYS[1] .. "_config", cjson.encode(config))
return true