var mapModel = Backbone.Model.extend({
    initialize: function(){
        console.log('a new map model is initialized.');
    },
    defaults: {
    centerLon: 41.880517,
    centerLat: -87.644061,
    layerUrl: 'https://{s}.tiles.mapbox.com/v3/datamade.hn83a654/{z}/{x}/{y}.png',
    attribution: '<a href="http://www.mapbox.com/about/maps/" target="_blank">Terms &amp; Feedback</a>',
    map_options: { scrollWheelZoom: false,
            tapTolerance: 30, minZoom: 1},
    drawnItems: new L.FeatureGroup(),
    dataLayer: undefined,
    },
 });
