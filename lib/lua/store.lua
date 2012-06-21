local config, oldest
-- Check if there is a config for this metric
if redis.call("exists",KEYS[1] .. "_config") == 0 then
  -- Create a config based on the default one
  config = redis.call("get", "rrd_default_config")
  redis.call("set", KEYS[1] .. "_config", config)
end
-- Load the config
config = cjson.decode(redis.call("get", KEYS[1] .. "_config"))

local timestamp = tonumber(ARGV[2])

-- If steps are defined for the native resolution, we will round the timestamp 
local higher_key = KEYS[1]..'_'..config["steps"]
if (timestamp % config["steps"]) / config["steps"] <= 0.5 then
  timestamp = math.floor(timestamp - (timestamp % config["steps"]))
else
  timestamp = math.floor(timestamp - (timestamp % config["steps"])) + config["steps"]
end


-- Get the amount of entries in this bucket
local count = redis.call("ZCARD", higher_key)

-- We need to make sure that to old entries are not added to the bucked
if count+1 == config["rows"] then
  oldest = tonumber(redis.call("ZRANGE", higher_key, 0, 0, 'WITHSCORES')[2])
  if timestamp < oldest then
    -- We cannot add older entries 
    --redis.log(redis.LOG_NOTICE, timestamp, oldest)
    return false
  end
end

-- We may be updating an old entry, which we want to delete first
redis.call("ZREMRANGEBYSCORE", higher_key, timestamp, timestamp)

-- Add the new entry to the bucked
redis.call("ZADD", higher_key, timestamp, timestamp..'_'..ARGV[1])

if count+1 > config["rows"] then 
  -- We have too many entries in this bucket - remove the oldest
  redis.call("ZREMRANGEBYRANK", higher_key, 0, (count-config.rows))
end

--redis.log(redis.LOG_NOTICE, cjson.encode(redis.call("ZRANGE", higher_key, 0, -1 )))
--redis.log(redis.LOG_NOTICE, timestamp, ARGV[1])

local get_value = function(value) return tonumber(string.sub(value, string.find(value, '_')+1, -1)) end

redis.log(redis.LOG_NOTICE, cjson.encode(config))
if config["rra"] then
  local higher = config
  local lower_start, higher_ts, value, max, min, n, sum, rra_key, last_rra
  --redis.log(redis.LOG_NOTICE, "rra start")

  for j, rras in ipairs(config["rra"]) do
    -- Calculate the timestamp for the aggregation
    --redis.log(redis.LOG_NOTICE, cjson.encode(rras))
    local steps = rras[1].steps
    --redis.log(redis.LOG_NOTICE, cjson.encode(steps))
    local rest = (timestamp % steps) 
    if rest == 0 then
      lower_start = timestamp - steps + higher["steps"]
      higher_ts = timestamp
    else
      lower_start = timestamp - rest + higher["steps"]
      higher_ts = timestamp - rest + steps
    end

    -- Get all entries from the higher precision bucket
    --redis.log(redis.LOG_NOTICE,"ZRANGEBYSCORE", higher_key, lower_start, lower_start+steps )
    local data = redis.call(         "ZRANGEBYSCORE", higher_key, lower_start, lower_start+steps)
    --redis.log(redis.LOG_NOTICE, cjson.encode(data))
    --redis.log(redis.LOG_NOTICE, steps, cjson.encode(rras))


    for i, rra in ipairs(rras) do
    
      -- If steps are defined for the native resolution, only proceed if we have enough entries
      if table.getn(data) > (rra["steps"]/higher["steps"]*rra["xff"]) then
        if rra["aggregation"] == "average" then
          sum = 0

          for i, value in ipairs(data) do
            sum = sum + get_value(value)
            --sum = sum + tonumber(value)
          end
          value = sum / table.getn(data)
        elseif rra["aggregation"] == "sum" then
          value = 0

          for i, v in ipairs(data) do
            value = value + get_value(v)
          end
        elseif rra["aggregation"] == "min" then
          min = 2^52
          for i, value in ipairs(data) do
            n = get_value(value)
            if n < min then
              min = n 
            end
          end
          value = min
        elseif rra["aggregation"] == "max" then
          max = -2^52
          for i, value in ipairs(data) do
            n = get_value(value)
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
      --redis.log(redis.LOG_NOTICE, 'value:', rra_key, value, rra.steps, higher.steps)
      rra_key = KEYS[1]..'_'..rra["steps"]..'_'..rra["aggregation"]
      -- We may be updating an old entry, which we want to delete first
      redis.call("ZREMRANGEBYSCORE", rra_key, higher_ts, higher_ts)
      -- Get the amount of entries in this bucket
      count = redis.call("ZCARD", rra_key)

      if count > higher["rows"] then 
        -- We have too many entries in this bucket - remove the oldest
        oldest = redis.call("ZRANGE", rra_key, 0, 0)
        redis.call("ZREM", rra_key, oldest[1])
      end
      
      -- Add the new entry to the bucked
      redis.call("ZADD", rra_key, higher_ts, higher_ts..'_'..value)
      last_rra = rra
    end
    -- Set the higher precision bucket to the current rra
    higher = last_rra
    higher_key = KEYS[1]..'_'..higher["steps"]..'_'..higher["aggregation"]
  end
end

--redis.log(redis.LOG_NOTICE, cjson.encode(config["rra"]))
--redis.call("set", KEYS[1] .. "_config", cjson.encode(config))
return true