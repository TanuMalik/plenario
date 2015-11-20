var queryModel = Backbone.Model.extend({
    initialize: function(){
        console.log('a new query model is initialized.');
    },

    defaults: {
        obs_date__le:'',
        obs_date__ge:'',
        agg:'weeks',
        dataset_name:'',
        resolution:'500',
        location_geom__within:'',
    },
    //setStart: function() {
    //    var start = $('#start-date-filter').val();
    //    start =  moment(start);
    //    if (!start){ start = moment().subtract('days', 90); }
    //    start = start.startOf('day').format('YYYY/MM/DD');
    //    this.set('obs_date__le',start);
    //},
    //setEnd: function() {
    //    var end = $('#end-date-filter').val();
    //    end = moment(end);
    //    if (!end) { end = moment(); }
    //    end = end.endOf('day').format('YYYY/MM/DD');
    //    this.set('obs_date__ge',end);
    //}
 });
