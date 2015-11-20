var AboutView = Backbone.View.extend({
    el: '#list-view',

    events: {
        'click .about-detail': 'detailView'
    },

    initialize: function(){
        console.log("Initializing About View");
        this.render();
    },

    render: function(){
        console.log("Rendering About View");
        $('#list-view').show();
        $('#detail-view').hide();
        this.$el.empty();
        this.$el.spin('large');
        var self = this;
        $.when(this.get_datasets()).then(
            function(resp){
                resp = resp['objects']
                self.$el.spin(false);
                self.$el.html(template_cache('aboutTemplate', {datasets:resp}));
                var dataObjs = {}
                //console.log(resp);
                $.each(resp, function(i, obj){
                    dataObjs[obj['dataset_name']] = obj;
                })
                self.datasetsObj = dataObjs;

                $('#available-datasets').DataTable( {
                    "aaSorting": [ [0,'asc'] ],
                    "aoColumns": [
                        null,
                        null,
                        { "bSortable": false }
                    ],
                    "paging": false,
                    "searching": false,
                    "info": false
                } );
            }
        )
    },
    get_datasets: function(){
        return $.ajax({
            url: '/v1/api/datasets/',
            dataType: 'json'
        })
    },
    detailView: function(e){
        //store the information in the query model instead
        console.log('about-view detailView')
        var start = $('#start-date-filter').val();
        var end = $('#end-date-filter').val();
        start = moment(start);
        if (!start){ start = moment().subtract('days', 90); }
        end = moment(end)
        if(!end){ end = moment(); }
        start = start.startOf('day').format('YYYY/MM/DD');
        end = end.endOf('day').format('YYYY/MM/DD');
        this.model.set({obs_date__le:end, obs_date__ge:start,agg:$('#time-agg-filter').val()});
        var dataset_name = $(e.target).data('dataset_name');
        this.model.set('dataset_name',dataset_name);
        this.model.set('resolution','500');
        console.log(this.model);
        this.undelegateEvents();
        $('#map-view').empty();
        console.log("initializaing About-view Detail-view");
        var meta = this.datasetsObj[dataset_name];
        new DetailView(this.model,meta);
        var route = 'detail/' + $.param(this.model.attributes);
        _gaq.push(['_trackPageview', route]);
        router.navigate(route)
    }
});