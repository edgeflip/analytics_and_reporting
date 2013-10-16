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


// FIRST VIEW, THE LEADERBOARD

function mksummary() {
    // initial load of summary data
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
        // bind a click function for charts
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


// CAMPAIGN DETAILS SPAWNED FROM BUTTON PER ROW 

function mkchart () {
    // on click of a campaign, get more detailed data and draw some charts
    $.post('/dailydata/', {'campaign':$(this).attr('root-id')}, mkdaily );

    // stash this so other UI elements know which campaign is selected
    window.campaign_id = $(this).attr('root-id')  

    // open a blank modal so the user knows the button click registered
    $('#modal').dialog({'modal':true, 'width':1000}) // pass height if you need to
    $('#modal').on( "dialogclose", function() {
        // clear old data
        $('.chart').children().remove()
        $('#hourlytable tbody').children().remove();

        // and make sure we are showing charts, not TSV data!
        $('#hourlytable').hide();
        $('#modal .chart').show()
        $('#modal #datepicker').show();
        });
    }


function mkdatepicker (now) {

    $('#datepicker').children().remove();

    var daterange = window.daterange;

    // set current date somewhere in the middle if there is no now arg
    now = typeof now !== 'undefined' ? now : daterange[ Math.floor(daterange.length/2)];

    // BUTTONS
    var container = d3.select('#datepicker');

    // oldest date
    container.append('button')
        .text( d3.min(daterange).toUTCString().substr(0,16))
        .attr('id', 'first')
        .attr('data-index', 0);
    $('#first').button( {
            'icons':{'primary':'ui-icon-seek-first'},
            'disabled': daterange.indexOf(now) == 0? true : false,
            });

    // previous day
    var previndex = daterange.indexOf(now) == 0 ? 0 : daterange.indexOf(now)-1;
    container.append('button')
        .text( daterange[previndex].toUTCString().substr(0,11))
        .attr('id', 'prev')
        .attr('data-index', daterange.indexOf(now)-1);
    $('#prev').button( {
            'icons':{'primary':'ui-icon-seek-prev'},
            'disabled': daterange.indexOf(now) == 0? true : false,
            });

    // button for today, does nothing, but convenient for styling
    container.append('button')
        .text( now.toUTCString().substr(0,11))
        .attr('id', 'now')
        .attr('data-index', daterange.indexOf(now));
    $('#now').button({disabled:false});

    // next day
    var nextindex = daterange.indexOf(now) == daterange.length-1 ? daterange.length-1 : daterange.indexOf(now) + 1;
    container.append('button')
        .text( daterange[nextindex].toUTCString().substr(0,11))
        .attr('id', 'next')
        .attr('data-index', daterange.indexOf(now)+1);
    $('#next').button( {
            'icons':{'primary':'ui-icon-seek-next'},
            'disabled': daterange.indexOf(now) == daterange.length-1? true : false,
            });

    // newest date
    container.append('button')
        .text( d3.max(daterange).toUTCString().substr(0,16))
        .attr('id', 'last')
        .attr('data-index', daterange.length-1);
    $('#last').button( {
            'icons':{'primary':'ui-icon-seek-end'},
            'disabled': daterange.indexOf(now) == daterange.length-1? true : false,
            });

    // click handlers for all
    $('#datepicker button').click( change_now );

    }


function change_now (event, now) {
    /* Change whatever day the detailed chart is focused on */

    var reqdate = typeof now !== 'undefined' ? now : window.daterange[$(this).attr('data-index')];
    $.post('/hourlydata/', {reqdate:reqdate.toJSON(), campaign:window.campaign_id}, on_hourly);

    // update datepicking controls
    mkdatepicker(reqdate);

    }


function on_hourly (response) {
    // callback for a call to change_now basically
    window.response = response ;

    $('#hourlygraph').children().remove();
    mkgraph('#hourlygraph', response);
    }



function mkdaily (response) {
    // the first load of data, a chart of all stats over the course of
    // the campaign, grouped by day

    window.response = response;  // debuggy but convenient

    // many things use this
    window.daterange = window.response.data.map( function(row){return new Date(row.day)} ); 

    // load the first hourly chart from whatever we chose as default
    change_now(null, window.daterange[0]);

    // draw the first chart
    mkgraph('#dailygraph', response);

    // the control for revealing TSV data
    $('#tsver').button();
    $('#tsver').off('click').on('click', function() {
        $.post('/alldata/', {campaign:window.campaign_id}, tsv_data);
        })
    $('#tsver').show();
    }


function mkgraph(element, response) {

    var palette = new Rickshaw.Color.Palette();

    var graph = new Rickshaw.Graph( {
        element: document.querySelector(element),
        width: 600,
        height: 150,
        renderer: 'line',
        series: [{
                name: "Visits",
                data: response.data.map(function(row) {return {x:new Date(row.day).getTime()/1000, y:row.visits}}),
                color: palette.color(),
            },{
                name: "Clicks",
                data: response.data.map(function(row) {return {x:new Date(row.day).getTime()/1000, y:row.clicks}}),
                color: palette.color(),
            },{
                name: "Unique Auths",
                data: response.data.map(function(row) {return {x:new Date(row.day).getTime()/1000, y:row.uniq_auths}}),
                color: palette.color(),
            },{
                name: "Faces Shown",
                data: response.data.map(function(row) {return {x:new Date(row.day).getTime()/1000, y:row.shown}}),
                color: palette.color(),
            },{
                name: "Shares",
                data: response.data.map(function(row) {return {x:new Date(row.day).getTime()/1000, y:row.shares}}),
                color: palette.color(),
            },{
                name: "Audience",
                data: response.data.map(function(row) {return {x:new Date(row.day).getTime()/1000, y:row.audience}}),
                color: palette.color(),
            },{
                name: "Clickbacks",
                data: response.data.map(function(row) {return {x:new Date(row.day).getTime()/1000, y:row.clickbacks}}),
                color: palette.color(),
            },]
        });

    // new Rickshaw.Graph.Axis.Time( { graph: graph, timeUnit:time.unit('day') } );
    new Rickshaw.Graph.HoverDetail({ graph: graph, yFormatter: function (x) {return x} });

    graph.render();

    var xAxis = new Rickshaw.Graph.Axis.Time({ 
            graph: graph,
        });
    xAxis.render();

    }


function sort() {
    /* take a click on a table header and sort the summary rows */

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

function tsv_data(response) {
    console.log(response);
    window.response = typeof response.data !== 'undefined' ? response.data : window.response;

    /* Load the data and build up the table element, then toggle displays */

    // we're manually adding column headers in the template right now, TODO, send this serverside
    var columns = ['day', 'visits', 'clicks', 'uniq_auths', 'shown', 'shares', 'audience', 'clickbacks'];
    var table = d3.select('#hourlytable')

    // build rows
    var body = table.append("tbody")
    var rows = body.selectAll("tr").data(window.response)
        .enter()
        .append("tr");

    // build cells per row
    rows.selectAll("td").data(
        /* so for each row, we end up wanting an array of values, in column order */
        function(row) {
            // columns.map makes a nice [] of datapoints per row
            return columns.map(function(column) {return row[column]})
        })
        .enter()
        .append("td")
        .text(function(d){return d+'\t'})
        .attr("class", "datapoint")

    // toggle visibility .. kinda awkward
    $('#hourlytable').show();
    $('#modal .chart').hide()
    $('#modal #datepicker').hide();

    // toggle button functionality
    $('#tsver span').text('Show Graphs');
    $('#tsver').off('click').on('click', function () {
        $('#hourlytable').hide();
        $('#modal .chart').show();
        $('#modal #datepicker').show();
        $('#tsver span').text('Show Raw Data');
        $('#tsver').off('click').on('click', tsv_data);
        });
    }

