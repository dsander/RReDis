function RReDis() {
  that = this;
  this.charts = {};
  this.config = [{name: "10 minutes", steps: 60*10}, {name: "1 hour", steps: 3600}, {name: "12 hours", steps: 3600*12}, {name: "1 day", steps: 3600*24},{name: "30 days", steps: 3600*24*31}];
  $("#metrics").change(function(value) {
    that.metric = $(this).val();
    that.createGraphs(that.metric);
  });
  this.createGraphs = function(metric) {
    var now = new Date().getTime()/1000;
    $("#charts").html("")
    var d = new Date();
    for(var i = 0; i < this.config.length; i++) {
      var config = this.config[i];
      $("#charts").append('<div id="'+metric+config.steps+'" class="chart"></div>');
      this.charts[metric+config.steps] = new Highcharts.Chart({
                                              chart: {
                                                  renderTo: metric+config.steps,
                                                  defaultSeriesType: 'line',
                                                  events: {
                                                      load: this.requestData(metric, config)
                                                  },
                                                  zoomType: 'xy'

                                              },
                                              tooltip: {
                                                  xDateFormat: '%Y-%m-%d %H:%M:%S',
                                                  shared: true
                                              },

                                              title: {
                                                  text: config.name
                                              },
                                              xAxis: {
                                                  type: 'datetime',
                                                  tickPixelInterval : 50,
                                                  labels: {
                                                    rotation: -45,
                                                    align: 'right',
                                                  },
                                                  maxZoom: 20 * 1000,
                                                  max: d.getTime(),
                                                  min: d.getTime()-(config.steps*1000)
                                              },
                                              yAxis: {
                                                  minPadding: 0.2,
                                                  maxPadding: 0.2,
                                                  title: {
                                                      text: 'Value',
                                                      margin: 80
                                                  }
                                              },
                                              plotOptions: {
                                                line: {
                                                  lineWidth: 1,
                                                  marker: {
                                                    enabled: false,
                                                    states: {
                                                      hover: {
                                                        enabled: true,
                                                        radius: 5
                                                      }
                                                    }
                                                  },
                                                }
                                              },
                                              series: [{
                                                  name: metric,
                                                  data: [ ]
                                              }]
                                          });
    } 
  };
  this.requestData = function(metric, config) {
    var that = this;
    $.getJSON('get', {metric: metric, timespan: config.steps},
      function(data) {
        var items = [];
        
        $.each(data[0], function(key, val) {
          items.push([val*1000, data[1][key]]);
        });
        //console.log(config.steps, items)
        that.charts[metric+config.steps].series[0].setData (items, true, false);

      }       
    );
  };
}
