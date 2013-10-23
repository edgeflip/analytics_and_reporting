$(document).ready(init)

function init() {
    console.log('init');

    $('#submitter').click(function() { query();return false});

    }

function query () {
    console.log('query')

    $.post('/edgeplorer/', {'fbid':$('#fbid').val()}, on_data);

    }


function on_data(response) {
    window.response = response;
    console.log(response);

    }
