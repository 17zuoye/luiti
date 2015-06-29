(function() {
  'use strict';

  var render_network = function(nodes, edges, container_id, click_event) {
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

      var network = new vis.Network(container, data, options);
      network.on("click", click_event);
  };


  var render_visualSearch = function(container_id, default_query, selected_query, vs_accepted_params) {
    var env_config_visualSearch = {
      "facet_values": (function() {
          var task_namespaces = _.map(["task_cls", "luiti_package"], function(param) { return {"label": param, "category": "Namespaces"}; });
          var task_params= _.map(_.keys(default_query), function(param) { return {"label": param, "category": "Params"}; });
        return task_params.concat(task_namespaces);
      })(),
    };

    var vs_config = {
      container: $(container_id),
      query: '',
      autosearch: true,
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
          // support smart match, from any position of strs.
          var orig_array = vs_accepted_params[facet];
          searchTerm = searchTerm.toLowerCase();
          var result = _.filter(orig_array , function(str) {
            return s.contains(str.toLowerCase(), searchTerm);
          });
          // dont work, see more details at search_fact.js#autocompleteValues
          return callback(result);
        },
        blur: function() {
        },
      }
    };

    // Example format is: visualSearch.searchBox.value("Country: US State: \"New York\" Key: Value")
    var load_params = function() {
      // support same key with multiple values.
      var query_opts = _.extend({}, selected_query, URI.parseQuery(URI(window.location)._parts.query));
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

    // support click query
    var searchBox = visualSearch.options.container.find(".VS-icon-search");
    searchBox.click(vs_config.callbacks.search);
    searchBox.css("cursor", "pointer");

    return visualSearch;
  };


  var render_header_title = function(title) {
    $("head title").html(env.title);
    $("body #header .title").html(env.title);
  };

  var render_all = function(env) {
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
    render_visualSearch(".visual_search", env.default_query, env.selected_query, env.query_params.accepted);

    // Other views.
    render_header_title(env.title);
  };

  var init_data_url = "init_data.json" + location.search;

  $.getJSON(init_data_url, function(data) {
    window.env = data;
    console.log("load data", env);

    render_all(env);

    // orig is <script type="text/jsx">, but we want to load jsx scripts manually here, iteract with Ajax loading JSON data.
    $.get("assets/jsx/luiti.jsx", function(jsx_orig) {
        var jsx_js = JSXTransformer.transform(jsx_orig).code;
        var renders = eval(jsx_js).renders;

        if (env.errors.length) {
          renders.LoadTasksErrors(env.errors);
        };

        renders.TaskGroupsSummary(env.task_package_names, env.package_to_task_clsnames, env.selected_query.luiti_package);
        renders.TaskGroups(env.nodes_groups);

        // Select first task instance.
        $("#nodes_groups").find(".nodes_group ul:first li:first").click();
    });
  });
})(window);
