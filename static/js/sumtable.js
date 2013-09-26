window.onload = init

function init() {
    // load data
    // $.post('/chartdata/', {'campaign':'aggregate'}, onData)
    }


function onData(response) {
    window.response = response;

    // get at values like data.key, data.values
    var roots = d3.select('table').selectAll('tr.root').data(d3.entries(response.chains));

    roots.enter()
        .append('tr.root')
        .append('td')
            .attr('class', 'rootcell')
            .text( function(d,i) {return 'Root: '+d.key+' '+response.meta[d.key].name});

    var kids = roots.selectAll('tr.child').data( function(d,i) {console.log(d.value);return d.value});
    
    kids.enter()
        .append('li')
        .text( function(d,i) {return d});

    }



