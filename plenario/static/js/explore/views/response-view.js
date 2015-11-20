var ResponseView = Backbone.View.extend({
    el:'#list-view',

    events: {
        'click .detail': 'detailView'
    },

    initialize: function(query){
        //console.log("Initializing Response View");
        this.query = query;
        this.render();
    },

    render: function(){
        //console.log("Rendering Response View");
        $('#list-view').show();
        $('#detail-view').hide();
        var self = this;
        if (typeof this.explore !== 'undefined'){
            this.explore.remove();
        }
        this.$el.empty();
        this.charts = {};
        this.$el.spin('large');
        this.getResults();
    },
    detailView: function(e){
        console.log('Response-View DetailView')
        var dataset_name = $(e.target).data('dataset_name');
        this.query.set('dataset_name',dataset_name);
        this.undelegateEvents();
        $('#map-view').empty();
        var meta = this.meta[dataset_name];
        new DetailView(this.query,meta);
        var route = 'detail/' + $.param(this.query)
        _gaq.push(['_trackPageview', route]);
        router.navigate(route)
    },
    getResults: function(){
        var self = this;
        $.when(this.resultsFetcher(), this.metaFetcher()).then(
            function(resp, meta_resp){
                self.$el.spin(false);
                var results = resp[0].objects;
                var results_meta = resp[0]['meta']
                var m = meta_resp[0]['objects'] //all datasets
                var objects = []
                self.meta = {}
                //console.log(m);
                $.each(m, function(i, obj){
                    self.meta[obj.dataset_name] = obj
                })
                $.each(results, function(i, obj){
                    obj['values'] = []
                    obj['count'] = 0;
                    $.each(obj.items, function(i, o){
                        obj['values'].push([moment(o.datetime + "+0000").valueOf(),o.count]);
                        obj['count'] += o.count;
                    });
                    //console.log(obj['values'])
                    obj['meta'] = self.meta[obj['dataset_name']]
                    objects.push(obj)
                });

                self.$el.html(template_cache('datasetTable', {
                    objects: objects,
                    query: self.query.attributes
                }));

                $.each(objects, function(i, obj){
                    ChartHelper.sparkline((obj['dataset_name'] + '-sparkline'), results_meta['query']['agg'], obj['values']);
                });

                $('#response-datasets').DataTable( {
                    "aaSorting": [ [2,'desc'] ],
                    "aoColumns": [
                        null,
                        null,
                        null,
                        { "bSortable": false },
                        { "bSortable": false }
                    ],
                    "paging": false,
                    "searching": false,
                    "info": false
                } );
            }
        ).fail(function(resp){
            var error = {
                header: 'Woops!',
                body: "Error fetching data.",
            }
            new ErrorView({el: '#errorModal', model: error});
        });
    },

    //the api only takes in location, ob dates, and aggregation
    resultsFetcher: function(){
        var self = this;
        var q = self.query.attributes;
        delete q['resolution'];
        delete q['dataset_name'];
        return $.ajax({
            url: '/v1/api/timeseries/',
            dataType: 'json',
            data: self.query.attributes
        });
    },
    metaFetcher: function(){
        return $.ajax({
            url: '/v1/api/datasets/',
            dataType: 'json'
        })
    }
});
