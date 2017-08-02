$( function() {

	var getDayOfYear = function(text){
		var now = new Date(text);
		var start = new Date(now.getFullYear(), 0, 0);
		var diff = now - start;
		var oneDay = 1000 * 60 * 60 * 24;
		var day = Math.floor(diff / oneDay);
		return day;
	}

    $( "#datepicker" ).datepicker({
    	onSelect : function(dateText){
    		var day = getDayOfYear(dateText);
    		initialize(day);
    	}
    });
   $( "#datepicker" ).datepicker( "setDate", new Date(2015,0,01));
} );
