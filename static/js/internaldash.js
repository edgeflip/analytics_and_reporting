$(document).ready(init)

function init() {
    // first pageview, get summary data and draw main table
    $.post('/edgedash/', function(response) {

        window.response = response;

        var chart = d3.select('#graph').append('svg')
            .attr('width', 800)
            .attr('height', 800)

        var tdata = response.data.map( function(row){return new Date(row.hour)})
        var x = d3.scale.linear()
            .domain([d3.min(tdata), d3.max(tdata)])
            .range([0,600])

        var ydata = response.data.map( function(row){return new Date(row.count)})
        var y = d3.scale.linear()
            .domain([d3.min(ydata), d3.max(ydata)])
            .range([0,800])

        chart.selectAll("rect").data(response.data)
            .enter().append("rect")
            .attr('x', function(d) {return x(new Date(d.hour))})
            .attr('y', function(d) {return 800- (y(d.count))})
            .attr('width', 2)
            .attr('height', function(d,i) {return y(d.count)})

        })
    }


