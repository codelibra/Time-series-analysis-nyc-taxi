$( function() {

    var day = 0;

    var playMap = function(){
    	console.log("play" + day);
    	day = day + 1;
    	var taxiData = [];
	
		$.getJSON('/realtime/' + day,
            function(data) {
              
			  for(var i=0;i<data.length;i++)
			  {
				    taxiData.push(new google.maps.LatLng(data[i]['weekly_zscore.latitude'],data[i]['weekly_zscore.longitude']));
			  }	  
			   var pointArray = new google.maps.MVCArray(taxiData);
	
				// what data for the heatmap and how to display it
				heatmap = new google.maps.visualization.HeatmapLayer({
					data: pointArray,
					radius: 12
				});

				// placing the heatmap on the map
				heatmap.setMap(map);	
		  });
    }

	$( "#play" ).click(function() {
		console.log("starting to play the map");
		setInterval(playMap, 5000);
	});
} );