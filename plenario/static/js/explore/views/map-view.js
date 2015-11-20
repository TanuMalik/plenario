var MapView = Backbone.View.extend({

    el: '#map-view',

    events: {
        'click #submit-query': 'submitForm',
        'click #reset': 'resetForm'
    },

    initialize: function(map_model, query) {
        console.log("initializing a new map view");
        this.model = map_model;
        this.query = query;
        //console.log(this.query);
        //console.log(this.model);
        var start = moment().subtract('d', 90).format('MM/DD/YYYY');
        var end = moment().format('MM/DD/YYYY');
        //
        //if (this.attributes.resp && this.attributes.resp.query)
        //{
        //    start = moment(this.attributes.resp.query.obs_date__ge).format('MM/DD/YYYY');
        //    end = moment(this.attributes.resp.query.obs_date__le).format('MM/DD/YYYY');
        //}

        if (this.query.get('obs_date__ge') != '' && this.query.get('obs_date_le') != '' )
        {
            start = moment(this.query.get('obs_date__ge')).format('MM/DD/YYYY');
            end = moment(this.query.get('obs_date__le')).format('MM/DD/YYYY');
        }


        this.$el.html(template_cache('mapTemplate',{end: end, start: start}));
        //render default map based on the model information
        map = L.map('map', this.model.get('map_options'))
            .setView([this.model.get('centerLon'), this.model.get('centerLat')], 11);
        L.tileLayer(this.model.get('layerUrl'), {attribution:this.model.get('attribution')}).addTo(map);
        map.drawnItems = this.model.attributes.drawnItems;
        map.addLayer(map.drawnItems);
        this.render();
    },
    //
    //    if (this.attributes.resp && this.attributes.resp.query.agg)
    //        $('#time-agg-filter').val(this.attributes.resp.query.agg)
    //},

    render: function() {
        //console.log(this.model);
        //console.log(map.drawnItems);
        console.log("rendering map view");
        //console.log(this.model);
        var self = this;
        var drawControl = new L.Control.Draw({
        edit: {
            featureGroup: map.drawnItems
        },
        draw: {
            circle: false,
            marker: false
        }
        });
        //console.log(drawControl);
        map.addControl(drawControl);
        map.on('draw:created', this.drawCreate);
        map.on('draw:drawstart', this.drawDelete);
        map.on('draw:edited', this.drawEdit);
        map.on('draw:deleted', this.drawDelete);

        //dont know what this is doing.
        $('.date-filter').datepicker({
            dayNamesMin: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
            prevText: '',
            nextText: ''
        });

        var geojson = L.geoJson(this.model.attributes.dataLayer, {
                      color: "#f06eaa",
                      fillColor: "#f06eaa",
                      weight: 4
                    });

        if (typeof this.model.attributes.dataLayer !== 'undefined'){
            console.log("dataLayer is defined");
            map.drawnItems.addLayer(geojson);
            console.log("there's a layer!");
            map.whenReady(function () {
                window.setTimeout(function () {
                    map.fitBounds(geojson.getBounds());
                }.bind(this), 200);
            }, this);
        }

        $("#dismiss-intro").click(function(e){
            e.preventDefault();
            $('#collapse-intro').collapse('hide');

        });
    },
    resetForm: function(e){
        window.location = "/explore";
    },
    drawCreate: function(e){
        map.drawnItems.clearLayers();
        console.log("create");
        map.drawnItems.addLayer(e.layer);
        map.dataLayer = e.layer.toGeoJSON();

    },
    drawDelete: function(e){
        map.drawnItems.clearLayers();
    },
    drawEdit: function(e){
        var layers = e.layers;
        map.drawnItems.clearLayers();
        var self = this;
        layers.eachLayer(function(layer){
            self.dataLayer = layer.toGeoJSON();
            self.drawnItems.addLayer(layer);
        });
    },
    submitForm: function(e) {
        console.log('map-view submit')
        console.log(this.query);
        var message = null;
        var start = $('#start-date-filter').val();
        var end = $('#end-date-filter').val();
        start = moment(start);
        if (!start) {
            start = moment().subtract('days', 90);
        }
        end = moment(end)
        if (!end) {
            end = moment();
        }
        var valid = true;
        if (start.isValid() && end.isValid()) {
            start = start.startOf('day').format('YYYY/MM/DD');
            end = end.endOf('day').format('YYYY/MM/DD');
        } else {
            valid = false;
            message = 'Your dates are not entered correctly. Please enter them in the format month/day/year.';
        }
        this.query.set({obs_date__le: end, obs_date__ge: start});
        //    if (this.map.dataLayer){
        //        query['location_geom__within'] = JSON.stringify(this.map.dataLayer);
        //        this.map.fitBounds(this.map.drawnItems.getBounds());
        //    }
        if (map.dataLayer) {
            this.query.set('location_geom__within',JSON.stringify(map.dataLayer));
            this.model.attributes.dataLayer = map.dataLayer;
            //.model.set('dataLayer',JSON.stringify(map.dataLayer));
            map.fitBounds(map.drawnItems.getBounds());
            //console.log(map.drawnItems.getBounds());
        }

        //else if (this.attributes.resp && this.attributes.resp.query.location_geom__within) {
        //    query['location_geom__within'] = this.attributes.resp.query.location_geom__within
        //}

        else {
            valid = false;
            message = 'You must draw a shape on the map to continue your search.';
        }
        this.query.set('agg', $('#time-agg-filter').val());
        //console.log(valid);
        if (valid) {
            //if (resp) {
            //    resp.undelegateEvents();
            //}
            new ResponseView(this.query);
            var route = "aggregate/" + $.param(query.attributes);
            _gaq.push(['_trackPageview', route]);
            router.navigate(route);
        } else {
            $('#list-view').spin(false);
            var error = {
                header: 'Woops!',
                body: message,
            }
            new ErrorView({el: '#errorModal', model: error});
        }
    }
});
