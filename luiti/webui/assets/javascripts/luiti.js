(function() {
  'use strict';

  // mark color, when select a task, separate in and out.
  var colors = {
      "requires": "lime",
      "self": "#7BE141",
      "upons": "green",
  };

  var render_network = function(nodes, edges, container_id, click_event) {
      nodes = _.map(nodes, function(node) {
          if (_.contains(queryparams.selected_query.task_cls, node.label)) {
            node.color = colors.self;
          } else {
            node.color = colors.requires;
          };
          return node;
      });

      // NOTE: original code is http://visjs.org/examples/network/nodeStyles/customGroups.html
      var container = $(container_id)[0];  // create a network
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
          var task_namespaces = _.map(["task_cls", "luiti_package"], function(param) {
            return {"label": param, "category": "Namespaces"};
          });
          var task_params= _.map(_.keys(default_query), function(param) {
            return {"label": param, "category": "Params"};
          });
        return task_params.concat(task_namespaces);
      })(),
    };

    var get_current_query = function(visualSearch) {
      var result = {};

      _.map(visualSearch.searchQuery.facets(), function(facet) {
        var kv = _.pairs(facet)[0];
        if (_.has(result, kv[0])) {
          result[kv[0]].push(kv[1]);
        } else {
          result[kv[0]] = [kv[1]];
        };
      });

      return result;
    }

    var vs_config = {
      container: $(container_id),
      query: '',
      autosearch: true,
      callbacks: {
        search: function(query, searchCollection) {
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
          var result = get_current_query(visualSearch);

          // Update a React view.
          group_summary.setState({"selected_luiti_packages": result["luiti_package"]})
        },
      }
    };

    // Example format is: visualSearch.searchBox.value("Country: US State: \"New York\" Key: Value")
    var load_params = function(query_opts) {
      // support same key with multiple values.
      var vs_values = [];
      _.each(query_opts, function(opt_values, opt_key) {
        _.each(opt_values, function(opt_value) {
          vs_values = vs_values.concat(JSON.stringify(opt_key) + ": " + JSON.stringify(opt_value));
        });
      });
      return vs_values.join(" ");
    };

    // Run it!
    var visualSearch = VS.init(vs_config);

    visualSearch.current_query = (function() {
      var result = _.extend({}, selected_query, URI.parseQuery(URI(window.location)._parts.query));
      // wrap value in a Array.
      _.each(_.keys(result), function(key) {
        if (!_.isArray(result[key])) {
          result[key] = [result[key]];
        };
      });
      return result;
    })();

    visualSearch.setValue = function(opts) {
      return visualSearch.searchBox.value(load_params(opts));
    };
    visualSearch.setValue(visualSearch.current_query);

    // support click query
    var searchBox = visualSearch.options.container.find(".VS-icon-search");
    searchBox.click(function(event) {
      var result = get_current_query(visualSearch);

      // build a url query
      var url = URI(window.location);
      url._parts.query = "";
      url.setQuery(result);
      window.location = url.build();

      return false;
    });
    searchBox.css("cursor", "pointer");

    return visualSearch;
  };


  var render_header_title = function(title) {
    $("head title").html(title);
    $("body #header .title").html(title);
  };

  var render_all = function(env) {
    // 1. render network
    render_network(nodeedge.nodes,
                   nodeedge.edges,
                   "#network",
                   function (params) {
                     console.log("[click a node on #network]", params);
                     var task_id = params["nodes"][0]; // only one task can be clicked.
                     // Delegate to show TaskDetailView
                     $("#nodes_groups").find('.nodes_group li[data-task-id="' + task_id + '"]').click();
                   });

    // 2. render visualSearch
    env.visualSearch = render_visualSearch(".visual_search", queryparams.default_query, queryparams.selected_query, queryparams.accepted);

    // Other views.
    render_header_title(title);
  };

  var init_data_url = "init_data.json" + location.search;

  $.getJSON(init_data_url, function(data) {
    // bind env's first level key to global `window` object.
    _.each(data, function(value, key) {
      window[key] = value;
    });
    window.env = data;
    console.log("load data", env);

    // transform data
    nodeedge.nodeid_to_node_dict = _.reduce(nodeedge.nodes, function(dict, node) {
      dict[node.id] = node;
      return dict;
    }, {});

    render_all(env);

    // orig is <script type="text/jsx">, but we want to load jsx scripts manually here, iteract with Ajax loading JSON data.
    $.get("assets/jsx/luiti.jsx", function(jsx_orig) {
        var jsx_js = JSXTransformer.transform(jsx_orig).code;
        window.renders = eval(jsx_js).renders;

        if (errors.load_tasks.length) {
          renders.LoadTasksErrors(errors);
        };

        renders.TaskGroupsSummary(ptm.task_package_names, ptm.package_to_task_clsnames, queryparams.selected_query.luiti_package);
        renders.TaskGroups(nodeedge.nodes_groups);

        // Select first task instance.
        var lis = $("#nodes_groups").find(".nodes_group ul li");
        var selector_attrs_task_cls = _.map((queryparams.selected_query.task_cls || []), function(task_cls) {
            return "[data-task_cls=" + task_cls + "]";
          });
        // e.g. "[data-task_cls*=Profile], [data-task_cls*=Dump]"
        var selected_lis = lis.filter(selector_attrs_task_cls.join(", "));
        if (selected_lis.length == 0) {
          selected_lis = lis;
        };
        selected_lis.first().click();
    });
  });
})(window);
