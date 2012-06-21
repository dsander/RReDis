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
    if i % 2 == 0 then
      table.insert(timestamps, get_value(d)-offset)
    else
      table.insert(values, get_value(d))
    end
  end
  return {timestamps, values}
end

-- Load the config
local config = cjson.decode(redis.call("get", KEYS[1] .. "_config"))
-- If we do not have a config we can assume that we also have no data to return
if not config then
  return {{}, {}}
end


local start = tonumber(ARGV[1])
local stop = tonumber(ARGV[2])
local timespan = stop-start

local method
if ARGV[3] == "" then
  method = 'average'
else
  method = ARGV[3]
end

local higher_key = KEYS[1]..'_'..config["steps"]

local oldest = redis.call("ZRANGE", higher_key, 0, 0, 'WITHSCORES')
if not oldest then
  return {{}, {}}
end

local oldest = tonumber(oldest[2])

if timespan <= config.steps*config.rows and timespan/config.steps < 650 then
  local data = redis.call("ZRANGEBYSCORE", higher_key, start, stop, 'WITHSCORES' )
  return get_data(data, 0)
end

if config["rra"] then
  local higher = config
  local rra_count = table.getn(config.rra)
  local key, oldest
  for i, rra in ipairs(config["rra"]) do
    -- Get all entries from the higher precision bucket
    key = KEYS[1]..'_'..rra["steps"]..'_'..method
    oldest = redis.call("ZRANGE", key, 0, 0, 'WITHSCORES')
    if oldest == {} then
      return {{}, {}}
    end

    oldest = tonumber(oldest[2])
    if (timespan <= rra.steps*rra.rows and timespan/rra.steps < 650) or i == rra_count then
      local data = redis.call("ZRANGEBYSCORE", key, start, stop, 'WITHSCORES' )
      return get_data(data, rra.steps/2)
    end
  end
end
