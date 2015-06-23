window.render_network = function(nodes, edges, container_id, click_event) {
    // NOTE: original code is http://visjs.org/examples/network/nodeStyles/customGroups.html
    var color = 'gray';
    var len = undefined;

    // create a network
    var container = $(container_id)[0];
    var data = {
        nodes: nodes,
        edges: edges
    };
    var options = {
        nodes: {
            shape: 'dot',
            size: 20,
            font: {
                size: 15,
                color: '#000000'
            },
            borderWidth: 2
        },
        edges: {
            width: 2
        }
    };

    network = new vis.Network(container, data, options);
    network.on("click", click_event);

};


window.render_visualSearch = function(container_id, default_params, vs_accepted_params) {
  var env_config_visualSearch = {
    "facet_values": (function() {
        var task_namespaces = _.map(["task_cls", "luiti_package"], function(param) { return {"label": param, "category": "Namespaces"}; });
        var task_params= _.map(_.keys(default_params), function(param) { return {"label": param, "category": "Params"}; });
      return task_params.concat(task_namespaces);
    })(),
  };

  var vs_config = {
    container: $(container_id),
    query: '',
    autosearch: false,
    callbacks: {
      search: function(query, searchCollection) {
        var result = {};
        var self = visualSearch;  // link to self

        // build a url query
        _.map(self.searchQuery.facets(), function(facet) {
          var kv = _.pairs(facet)[0];
          if (_.has(result, kv[0])) {
            result[kv[0]].push(kv[1]);
          } else {
            result[kv[0]] = [kv[1]];
          };
        });

        console.log("[vs search]", query, searchCollection, result);

        if (_.has(result, "date_value")) {  // and is valid.
          var url = URI(window.location);
          url._parts.query = "";
          url.setQuery(result);
          window.location = url.build();
        }

        return false;
      },
      facetMatches: function(callback) {
        callback(env_config_visualSearch["facet_values"]);
      },
      valueMatches: function(facet, searchTerm, callback) {
        return callback(vs_accepted_params[facet]);
      }
    }
  };

  // Example format is: visualSearch.searchBox.value("Country: US State: \"New York\" Key: Value")
  var load_params = function() {
    // support same key with multiple values.
    var query_opts = _.extend({}, default_params, URI.parseQuery(URI(window.location)._parts.query));
    var vs_values = [];
    _.each(query_opts, function(opt_values, opt_key) {
      if (!_.isArray(opt_values)) { opt_values = [opt_values] };
      _.each(opt_values, function(opt_value) {
        vs_values = vs_values.concat(JSON.stringify(opt_key) + ": " + JSON.stringify(opt_value));
      });
    });
    return vs_values.join(" ");
  };

  // Run it!
  var visualSearch = VS.init(vs_config);
  visualSearch.searchBox.value(load_params());

  return visualSearch;
};


window.render_header_title = function(title) {
  $("head title").html(env.title);
  $("body #header .title").html(env.title);
};

window.render_all = function(env) {
  // 1. render network
  render_network(env.nodes,
                 env.edges,
                 "#network",
                 function (params) {
                   console.log("[click a node on #network]", params);
                   var task_id = params["nodes"][0]; // only one task can be clicked.
                   // Delegate to show TaskDetailView
                   $("#nodes_groups").find('.nodes_group li[data-task-id="' + task_id + '"]').click();
                 });

  // 2. render visualSearch
  render_visualSearch(".visual_search", env.current_params, env.config.accepted_params);

  // Other views.
  render_header_title(env.title);

}

var init_data_url = "/luiti/dag_visualiser/init_data.json" + location.search;

$.getJSON(init_data_url, function(data) {
  window.env = data;
  console.log("load data", env);

  render_all(env);
});
