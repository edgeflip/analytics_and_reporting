$(document).ready(init)

function init() {
    // first pageview, get summary data and draw main table
    mksummary();

    // default sorting metric
    window.metric = 'visits';
    $('#sumtable th').click(sort); 

    // though we bind the sort handler to the entire th, style the actual buttons
    $('.sorter').button( {'icons':{'secondary':'ui-icon-arrow-2-n-s'}, 'text':true})
    }


function mksummary() {
    // load summary data and build a table
    $.get("/tabledata/", function (response) {

        window.response = JSON.parse(response); // TODO: this should automatically work

        // we're manually adding column headers in the template right now, TODO, send this serverside
        var columns = ['name', 'visits', 'clicks', 'uniq_auths', 'shown', 'shares', 'audience', 'clickbacks'];

        var table = d3.select('#sumtable')


        // build rows
        var body = table.append("tbody")
        var rows = body.selectAll("tr").data(window.response)
            .enter()
            .append("tr")
            .attr("class", "child")
            .attr("id", function(d) {return d.root_id});

        // build cells per row
        rows.selectAll("td").data(
            /* so for each row, we end up wanting an array of values, in column order */
            function(row) {
                // columns.map makes a nice [] of datapoints per row
                return columns.map(function(column) {return row[column]})
            })
            .enter()
            .append("td")
            .text(function(d){return d})
            .attr("class", "datapoint")

        // and a chart-toggler at the end
        rows.append("td")
            .append("button")
            .attr("class", "charter")
            .attr("root-id", function(d){return d.root_id});

        // make them jquery buttons
        $('.charter').button( {'icons':{'primary':'ui-icon-image'}, 'text':false});
        // bind a click function
        $('button.charter').click(mkchart);

        // and a final summary row
        body.append("tr").attr("class", "totals")
            .selectAll("td")
            .data(columns)
            .enter()
            .append("td")
            .text( function(metric,i) {return i==0?'TOTALS':d3.sum(window.response.map(function(d){return d[metric]})) })
            .attr("class", "datapoint");

        // like d3.sum(response.map(function(d,i) {console.log(d);return d.visits}))


        })
    }


function mkchart () {
    window.thing = this;
    console.log( $(this).attr('root-id'));

    }


function sort() {
    console.log('sorting');
    var metric = this.id;
    if (metric == window.metric) {
        // toggling sort order if they've clicked the same metric
        window.sorter = window.sorter === d3.ascending ? d3.descending : d3.ascending;
        }
    else {
        // else a new metric, set to descending by default
        window.sorter = d3.descending;
        }

    var sortstyle = window.sorter === d3.descending ? 'descend' : 'ascend';

    // clear old styles
    $('th').removeClass('ascend descend');

    // reset button styles
    $('.sorter').button( {'icons':{'secondary':'ui-icon-arrow-2-n-s'}, 'text':true})

    // set new styles
    $('.tableFloatingHeaderOriginal #'+metric).addClass(sortstyle);
    $('.tableFloatingHeader #'+metric).addClass(sortstyle);
    $(this).addClass( sortstyle);

    // set new button icon
    var toggle = sortstyle == 'descend' ? 'ui-icon-arrowthick-1-s': 'ui-icon-arrowthick-1-n';
    $('.tableFloatingHeaderOriginal #'+metric+' .sorter').button('option', 'icons', {secondary:toggle});
    $('.tableFloatingHeader #'+metric+' .sorter').button('option', 'icons', {secondary:toggle});

    window.metric = metric;

    d3.selectAll("tr.child").sort(function(a,b) {return window.sorter(a[metric],b[metric])} );

    }


function getData() {
    // no need to post client, the server can pick that out of whatever user
    day = $.datepicker.formatDate('mm/dd/yy', $('#datepicker').datepicker('getDate'));
    campaign = $('#campaignpicker select').val();

    $.post('/chartdata/', {'campaign':campaign, 'day':day}, onData);
    }


function onData(response) {
    window.response = response;

    // reset valid dates in the jquery widget
    $('#datepicker').datepicker( "option", "minDate", response.minday);
    $('#datepicker').datepicker( "option", "maxDate", response.maxday);


    // update data tables
    window.dailydata = new google.visualization.DataTable({'cols':response.daily_cols, 'rows':response.daily});
    window.monthlydata = new google.visualization.DataTable({'cols':response.monthly_cols, 'rows':response.monthly});


    // format dates
    var shortdate = new google.visualization.DateFormat({formatType: 'short'});
    shortdate.format( monthlydata, 0); 

    var shorthour = new google.visualization.DateFormat({formatType: 'HH'});
    shorthour.format( dailydata, 0);

    draw();
    }



function draw() {
    $('label').show();
    window.chart?drawcharts():drawtables();
    }


function drawtables() {
    window.dailychart.clearChart();  // maybe unnecessary actually
    window.monthlychart.clearChart();

    // hide the datepicker widget, it's awkward in the middle
    $('#datepicker').hide();

    window.dailychart = new google.visualization.Table( $('#daily')[0] );
    window.dailychart.draw(window.dailydata, {
        title: 'Hourly Volume - '+window.response.dailyday, 
        titleTextStyle: {fontSize:18},
        });

    window.monthlychart = new google.visualization.Table( $('#monthly')[0] );
    window.monthlychart.draw(window.monthlydata, {
        title: 'Daily Volume '+response.minday+' - '+response.maxday, 
        titleTextStyle: {fontSize:18},
        });
    }


function drawcharts() {

    // turn on our datepicker if we're coming from tables
    $('#datepicker').show();

    // redraw charts
    var dailyopts = {
        title: 'Hourly Volume - '+window.response.dailyday, 
        titleTextStyle: {fontSize:18},
        enableInteractivity: 'true', 
        width: 580, 
        height:175,
        chartArea: {width:500},
        lineWidth: 1, // set this a bit thinner than monthly 

        // curveType: 'function', // nice curves but it messes up vaxis calcs
        legend: {position: 'none'}, // kill this legend, we'll hack into the bigger one below
        // "turn off" the gridlines, but keep unit labels on axes
        vAxis: {gridlines: {color:'#FFF'} },
        hAxis: {gridlines: {color:'#FFF', count:13}, format:'H', title:'Time', titleTextStyle:{fontSize:13}},
        };

    window.dailychart = new google.visualization.LineChart( $('#daily')[0] );
    window.dailychart.draw(window.dailydata, dailyopts);

    // window.dailychart = new google.visualization.LineChart( $('#daily')[0] );

    window.monthlychart = new google.visualization.LineChart( $('#monthly')[0] );
    window.monthlychart.draw(window.monthlydata, {
        title: 'Daily Volume '+response.minday+' - '+response.maxday, 
        titleTextStyle: {fontSize:18},
        enableInteractivity: 'true', 
        chartArea:{left:50,},
        width: 799,
        height:250,
        legend: {position: 'right', textStyle: {fontSize:10}, alignment: 'end'},

        // "turn off" the gridlines, but keep unit labels on axes
        vAxis: {gridlines: {color:'#FFF'}, logScale:window.logscale},
        hAxis: {gridlines: {color:'#FFF', count:7}, format:'M/dd', title:'Date', titleTextStyle:{fontSize:13}},
        });
    }

