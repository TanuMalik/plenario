var FilterView = Backbone.View.extend({
    el: '#filter_builder',

    events: {
        'click .remove-filter': 'clear'
    },

    initialize: function(filter, options){
        console.log("Initializing Filter View");
        this.filter = filter;
        this.field_options = options;
        console.log(this.filter);
        console.log(this.field_options);
        this.render();
    },
    render: function(){
        console.log("Rendering Filter View");
        this.$el.append(_.template(get_template('filterTemplate'))(this.filter.attributes));

        var filter_dict_id = this.filter.get('id');
        $.each(this.field_options['objects'], function(k, v){
            $('#field_' + filter_dict_id).append("<option value='" + v['field_name'] + "'>" + humanize(v['field_name']) + "</option>");
        });

        // select dropdowns
        $("#field_" + this.filter.get('id')).val(this.filter.get('field'));
        $("#operator_" + this.filter.get('id')).val(this.filter.get('operator'));
    },
    clear: function(e){
        $("#row_" + $(e.currentTarget).attr("data-id")).remove();
    }
});