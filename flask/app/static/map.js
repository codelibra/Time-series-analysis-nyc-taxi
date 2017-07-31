// Render the markers for cab locations on Google Maps
var NY = new google.maps.LatLng(40.730610, -73.935242);
var markers = [];
var map;
function initialize() {
    var mapOptions = {
	zoom: 12,
	center: NY
    };
	
    map = new google.maps.Map(document.getElementById('map-canvas'),
			      mapOptions);
	
	var taxiData = [];
	
	 $.getJSON('/realtime',
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

function drop(lat, lng) {
    point  = new google.maps.LatLng(lat,lng);
    clearMarkers();
    addMarker(point);
}
function addMarker(position) {
    markers.push(new google.maps.Marker({
	position: position,
	map: map,
    }));
}
function clearMarkers() {
    for (var i = 0; i < markers.length; i++) {
	markers[i].setMap(null);
    }
    markers = [];
}

google.maps.event.addDomListener(window, 'load', initialize);
