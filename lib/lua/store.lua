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

local get_value = function(value) return tonumber(string.sub(value, string.find(value, '_')+1, -1)) end

if config["rra"] then
  local higher = config
  local lower_start, higher_ts, value, rra_key, last_rra, n

  for j, rra in ipairs(config["rra"]) do
    -- Calculate the timestamp for the aggregation
    local steps = rra.steps
    local rest = (timestamp % rra.steps) 
    if rest == 0 then
      lower_start = timestamp - rra.steps + higher["steps"]
      higher_ts = timestamp
    else
      lower_start = timestamp - rest + higher["steps"]
      higher_ts = timestamp - rest + rra.steps
    end

    if config["aggregations"] then
      local a = {}
      -- Shortcut, if we are in the first rra we can calculate the aggregations faster
      if higher.steps == config.steps then 
        -- Get all entries from the higher precision bucket
        local data = redis.call(         "ZRANGEBYSCORE", higher_key, lower_start, lower_start+rra.steps)
        -- If steps are defined for the native resolution, only proceed if we have enough entries
        if not (table.getn(data) > (rra["steps"]/higher["steps"]*rra["xff"])) then
          return false
        end

        a.sum = 0
        a.min = 4503599627370496
        a.max = -4503599627370496
        for i, value in ipairs(data) do
          value = get_value(value)
          a.sum = a.sum + value
          if value < a.min then
            a.min = value
          end
          if value > a.max then
            a.max = value
          end
        end
        a.avg = a.sum / table.getn(data)
      else
        -- For every other rra we need to fetch the matching data
        for i, method in ipairs(config["aggregations"]) do
          -- Get all entries from the higher precision bucket
          local data = redis.call(         "ZRANGEBYSCORE", higher_key..'_'..method, lower_start, lower_start+rra.steps)
          -- If steps are defined for the native resolution, only proceed if we have enough entries
          if not (table.getn(data) > (rra["steps"]/higher["steps"]*rra["xff"])) then
            return false
          end

          if method == "average" then
            a.sum = 0

            for i, value in ipairs(data) do
              a.sum = a.sum + get_value(value)
            end
            a.avg = a.sum / table.getn(data)
          elseif method == "sum" then
            a.sum = 0

            for i, v in ipairs(data) do
              a.sum = a.sum + get_value(v)
            end
          elseif method == "min" then
            a.min = 4503599627370496
            for i, value in ipairs(data) do
              n = get_value(value)
              if n < a.min then
                a.min = n 
              end
            end
          elseif method == "max" then
            a.max = -4503599627370496
            for i, value in ipairs(data) do
              n = get_value(value)
              if n > a.max then
                a.max = n
              end
            end
          else
            redis.log(redis.LOG_ERROR, "Not implemented")
          end
        end
      end

      -- Update the buckets
      for i, method in ipairs(config["aggregations"]) do
        rra_key = KEYS[1]..'_'..rra["steps"]..'_'..method
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
        if method == "average" then
          redis.call("ZADD", rra_key, higher_ts, higher_ts..'_'..a.avg)
        elseif method == "sum" then
          redis.call("ZADD", rra_key, higher_ts, higher_ts..'_'..a.sum)
        elseif method == "min" then
          redis.call("ZADD", rra_key, higher_ts, higher_ts..'_'..a.min)
        elseif method == "max" then
          redis.call("ZADD", rra_key, higher_ts, higher_ts..'_'..a.max)
        end
        
        last_rra = rra
      end
        
      -- Set the higher precision bucket to the current rra
      higher = last_rra
      higher_key = KEYS[1]..'_'..higher["steps"]
    end
  end
end

return true