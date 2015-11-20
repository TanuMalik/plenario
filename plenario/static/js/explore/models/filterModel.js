var filterModel = Backbone.Model.extend({
    initialize: function(){
        console.log('a new filter model is initialized.');
    },
    defaults: {
       "id":0,
       "field":'',
       "value":'',
       "operator":'',
       "removable":false
    }
 });
