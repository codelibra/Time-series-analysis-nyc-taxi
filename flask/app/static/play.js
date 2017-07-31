$( function() {

    var day = 0;

    var playMap = function(){
    	++day;
    	initialize(day);
    }

	$( "#play" ).click(function() {
		console.log("starting to play the map");
		setInterval(playMap, 3000);
	});
} );