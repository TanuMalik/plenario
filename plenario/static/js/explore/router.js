var AppRouter = Backbone.Router.extend({
    routes: {
        "": "defaultRoute",
        "aggregate/:query": "aggregate",
        "detail/:query": "detail",
        //"polygon/:polygonName": "polygon",
    },
     defaultRoute: function(){
         // TRY initialize the models within the views so when the user goes back it saves the previous models.
         map_model = new mapModel({});
         query = new queryModel({});
         new AboutView({model:query});
         new MapView(map_model, query);
    },
    //defaultRoute: function(){
    //    new AboutView({el: '#list-view'});
    //    map = new MapView({el: '#map-view', attributes: {}})
    //},
    aggregate: function(query){
        var q = parseParams(query);
        new ResponseView(query);
        new MapView(map_model, query);
    },
    //aggregate: function(query){
    //    var q = parseParams(query);
    //    resp = new ResponseView({el: '#list-view', attributes: {query: q}});
    //    var attrs = {
    //        resp: resp
    //    }
    //    if (typeof q['location_geom__within'] !== 'undefined'){
    //        attrs['dataLayer'] = $.parseJSON(q['location_geom__within']);
    //    }
    //    map = new MapView({el: '#map-view', attributes: attrs});
    //},
    detail: function(query){
        var q = parseParams(query);
        var dataset = q['dataset_name']
        $.when($.getJSON('/v1/api/datasets/sample.json', {dataset_name: dataset})).then(
            function(resp){
                new DetailView(q,resp['objects'][0]);
            }
        )
    },

    //polygon: function(polygonName){
    //    new PolygonView({el: '#map-view', attributes:{'polygonName': polygonName}})
    //}
});

