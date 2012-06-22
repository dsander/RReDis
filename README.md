## RReDis

https://github.com/dsander/RReDis

RReDis is a round robin database backed by redis. It uses the brand new lua scripting feature of redis 2.6.


## Getting started

Install the gem

    gem install rredis


Store some metrics
    
    require 'rredis'
    rrd = RReDis.new

    # We start two hours in the past
    start = (Time.now-(3600*2)).to_i

    # We pretend to update the data every 10 seconds for two hours
    (2*3600/10).times do |step|
      rrd.store "example", start+step*10, rand(100)
    end

Fetch data from RReDis

    require 'rredis'
    rrd = RReDis.new

    # Get the data from 5 minutes ago until now
    puts rrd.get('example', Time.now-300, Time.now).inspect
    # Get the data from one hour ago until 55 minutes ago
    puts rrd.get('example', Time.now-3600, Time.now-3300, 'min').inspect
    # Get the data from two hours ago until 90 minutes ago
    puts rrd.get('example', Time.now-7200, Time.now-5400, 'max').inspect
The get function takes three or four arguments:

*  The metric to query
*  Starting time stamp for the timespan to return
*  Ending time stamp for the timespan to return
*  Optional aggregation method 


The array returned contains two arrays, the first one with unix timestamps of the measurements, the second one the (aggregated) values.

## Configuration

Per default RReDis is configured to store one measurement every 10 seconds for one day (called native resolution from here on), and then aggregate the measurements for the following timespans:

*  1 week at 1 minute resolution
*  1 month at 15 minute resolution
*  1 year at 1 hour resolution

RReDis also stores the average, minima and maxima of aggregated measurements.

# Configuration format

RReDis default configuration:

    {:steps=>10, :rows=>17280, 
     :aggregations=>["average", "min", "max"], 
     :rra => [ {:steps=>60, :rows=>10080, :xff=>0.5},
               {:steps=>900, :rows=>2976, :xff=>0.5},
               {:steps=>3600, :rows=>8760, :xff=>0.5}]}

`:steps` interval in seconds in which to store measurements

`:rows`  amount of measurements to store, the timespan in seconds of the native resolution equals to `:steps`*`:rows`

`:aggregations` array of aggregations to use for this measurement, currently available: `average`, `min`, `max`, `sum`

`:rra`   array of archives to store historical data


For every `:rra`:

`:steps` interval in seconds in which to aggregate the measurements of the next higher resolution

`:rows`  amount of aggregations to store, the timespan in seconds of the archive equals to `:steps`*`:rows`

`:xff`   the xfiles factor - determines if aggregated measurements are stored. 1.0 would require 10 of 10 measurements from the next higher resolution (either form a rra or the native resolution), 0.1 would require 1 of 10 measurements

# Modify the configuration
You can either explicitly configure a metric via the `config` method:

    rrd = RReDis.new  
    rrd.config("metic", {:steps => 10, :rows => 3})


Or change the default configuration which will be applied to every measurement without an explicit configuration:

    rrd = RReDis.new
    rrd.default_config = {:steps => 10, :rows => 3}


## Performance

Even though redis supports a lot of data types, it was clearly not made to act as a round robin database, but still with lua scripting the performance is really nice. 

With the default configuration RReDis is able to handle around 2.5k updates per second (ubuntu vm with two cores of an i5 750 in virtual box running on windows), which is still amazingly fast considering the complexity of the store.lua script.

Check out the default.rb script in the benchmark directory. Here are the results of my machine:

    0.000395ms per op, 2529.6144076826317 op/s
    415.557405432s for for 1051200 inserts
    8639 stored measurements/aggregations
    156 bytes used per stored measurement
    6882960 measurements storable per gb of ram
    794 metrics storable per gb of ram
    5273285 redis commands performed
    5 redis commands performed per stored measurement

Using pipelining, we are able to determine the limits of redis itself. On my box redis can handle about 10k updates per second. A "advanced" key-value store able to run a lua script 10 thousand times a second doing about 50 thousand internal operations per second is just plain amazing. 
Hats off! @antirez


## LICENSE:

(The MIT License)

Copyright (c) 2012 Dominik Sander

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
