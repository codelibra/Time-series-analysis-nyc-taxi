$( function() {

    var day = 0;

	$( "#play" ).click(function() {
		console.log("starting to play the map");
		setInterval(function(){
			console.log("called" + day);
			day = day + 1;
			initialize(day);
			} , 5000);
	});
} );