local function get_value(value) 
  local n = string.find(value, '_');
  if n then
    return string.sub(value, n+1, -1)
  else
    return tonumber(value)
  end
end
local function get_data(data, offset)
  local values = {}
  local timestamps = {}
  for i, d in ipairs(data) do
    --redis.log(redis.LOG_NOTICE, i, d, get_value(d, i))
    if i % 2 == 0 then
      table.insert(timestamps, get_value(d)-offset)
    else
      table.insert(values, get_value(d))
    end
  end
  --redis.log(redis.LOG_NOTICE, cjson.encode(values))
  return {timestamps, values}
end
--redis.log(redis.LOG_NOTICE, "start----------")
-- Check if there is a config for this metric
if redis.call("exists",KEYS[1] .. "_config") == 0 then
  -- Create a config based on the default one
  config = redis.call("get", "rrd_default_config")
  redis.call("set", KEYS[1] .. "_config", config)
end
-- Load the config
local config = cjson.decode(redis.call("get", KEYS[1] .. "_config"))

local start = tonumber(ARGV[1])
local stop = tonumber(ARGV[2])
local timespan = stop-start

local higher_key = KEYS[1]..'_'..config["steps"]

local oldest = redis.call("ZRANGE", higher_key, 0, 0, 'WITHSCORES')
if not oldest then
  return {}
end

local oldest = tonumber(oldest[2])
if oldest <= start then
  --redis.log(redis.LOG_NOTICE, "considering")
  --redis.log(redis.LOG_NOTICE, higher_key, start, stop, oldest, config.steps, config.rows , timespan, timespan/config.steps)
  if timespan <= config.steps*config.rows and timespan/config.steps < 500 then
    local data = redis.call("ZRANGEBYSCORE", higher_key, start, stop, 'WITHSCORES' )
    return get_data(data, 0)
  end
end

if config["rra"] then
  local higher = config
  local rra_count = table.getn(config.rra)
  local key, oldest
  for i, rra in ipairs(config["rra"]) do
    -- Get all entries from the higher precision bucket
    key = KEYS[1]..'_'..rra["steps"]..'_'..rra["aggregation"]
    oldest = redis.call("ZRANGE", key, 0, 0, 'WITHSCORES')
    if not oldest then
      return {}
    end

    oldest = tonumber(oldest[2])
    if oldest <= start or i == rra_count then
      --redis.log(redis.LOG_NOTICE, "considering")
      --redis.log(redis.LOG_NOTICE, key, start, stop, oldest, rra.steps, rra.rows, oldest+(rra.steps*(rra.rows-1))-stop)
      if (timespan <= rra.steps*rra.rows and timespan/config.steps < 500) or i == rra_count then
        local data = redis.call("ZRANGEBYSCORE", key, start, stop, 'WITHSCORES' )
        return get_data(data, rra.steps/2)
      end
    end
  end
end
