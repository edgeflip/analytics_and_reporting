window.onload = init

function init() {
    // load data

    // set up detail buttons
    $('.more').button( {'icons':{'secondary':'ui-icon-minus'}, 'text':false})
    $('.more').click( function(){
        $(this).parents('tr.root').nextUntil('tr.root').slideToggle( 200);
        // $(this).text(function(_, value){return value=='-'?'+':'-'});
        var toggle = $(this).button('option', 'icons').secondary=='ui-icon-plus'?'ui-icon-minus':'ui-icon-plus';
        $(this).button('option', 'icons', {secondary:toggle});

        });

    // give it a click to hide sub-campaigns by default 
    $('.more').click();
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



