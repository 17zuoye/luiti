'use strict';

// Tech NOTE: React expects a single element to be returned from a render method.

var LoadTasksErrorsView = React.createClass({
  render: function() {
    return (
      <div className="panel panel-default">
        <div className="panel-heading">
          <h3 className="panel-title">Load Tasks Errors</h3>
        </div>
        <div className="panel-body">
          <table className="table">
            <thead>
              <tr>
                <td>Task Class</td>
                <td>Errors Backtraces</td>
              </tr>
            </thead>
            <tbody>
              { this.props.errors.load_tasks.map(function(err_info, err_idx) {
                return (
                  <tr key={err_idx} >
                    <td>
                      { err_info.task_clsname }
                    </td>
                    <td>
                      <pre>
                        { err_info.err }
                      </pre>
                    </td>
                  </tr>
                );
              }) }
            </tbody>
          </table>
        </div>
      </div>
    );
  }
});

var LoadTasksErrorsView_render = function(errors) {
  React.render(
    <LoadTasksErrorsView errors={errors} />,
    $("#load_tasks_errors")[0]
  );
};

var TaskGroupsSummaryView = React.createClass({
  mixins: [React.addons.LinkedStateMixin],
  getInitialState: function() {
    return {
      "package_to_task_clsnames" : this.props.package_to_task_clsnames,
      "selected_luiti_packages"  : this.props.selected_luiti_packages,
      "task_package_names"       : this.props.task_package_names,
    };
  },
  handleClick: function(event) {
    // Take control of <li/>, use `setState` to render current react View.
    var ele_click = $(event.target);
    var li = ele_click.closest("li.luiti_package");
    var checkBox = li.find("input[type=checkbox]");
    var current_package = li.attr("data-package_name");
    var current_query = env.visualSearch.current_query;

    var current_check_status = ! _.contains(current_query.luiti_package, current_package);

    if (current_check_status) {
      current_query.luiti_package = current_query.luiti_package.concat(current_package);
    } else {
      current_query.luiti_package = _.without(current_query.luiti_package, current_package);
    };
    env.visualSearch.setValue(current_query);

    this.setState({"selected_luiti_packages": current_query.luiti_package});
  },
  render: function() {
    window.group_summary = this;  // TODO improve with a real event system.
    var package_to_task_clsnames = this.linkState("package_to_task_clsnames").value;
    var selected_luiti_packages = this.linkState("selected_luiti_packages").value;

    return (
      <div>
        <h4>Total tasks count: {ptm.task_class_names.length}</h4>
        <div>
          <h4>All packages</h4>
          <ul>
          { _.map(this.linkState("task_package_names").value, function(package_name) {
            var is_checked = _.contains(selected_luiti_packages, package_name);

            return (
              <li onClick={group_summary.handleClick} key={package_name} className="input-group luiti_package" data-package_name={package_name} data-checked={is_checked} >
                <input type="checkbox" defaultChecked={false} checked={is_checked} ></input>
                <span className="pull-right">{package_name}[{package_to_task_clsnames[package_name].length}]</span>
              </li>
            );
          }) }
          </ul>
        </div>
      </div>
    );
  }
});

var TaskGroupsSummaryView_render = function(task_package_names, package_to_task_clsnames, selected_luiti_packages) {
  React.render(
    <TaskGroupsSummaryView task_package_names={task_package_names} package_to_task_clsnames={package_to_task_clsnames} selected_luiti_packages={selected_luiti_packages} key={task_package_names}/>,
    $("#task_groups_summary")[0]
  );
}

var TaskGroupsView = React.createClass({
  getInitialState: function() {
    return {
      "nodes_groups": this.props.nodes_groups,
      "selected_task_id": null,
    };
  },
  render: function() {
    return (
      <div>
        {this.state.nodes_groups.map(function(groups, group_idx) {
            return <TaskGroupView groups={groups} key={group_idx} group_idx={group_idx} />;
        })}
      </div>
    );
  }
});

var TaskGroupView = React.createClass({
  render: function() {
    var group_idx = this.props.group_idx + 1;
    return (
      <div className="nodes_group" key={group_idx}>
        <h5>
          { group_idx }#Group[{this.props.groups.length}]
        </h5>
        <ul>
          { this.props.groups.map(function(node_label, node_idx) {
              return <TaskInfoView group_idx={group_idx} key={node_idx} node_idx={node_idx} node_label={node_label} />;
          }) }
        </ul>
      </div>
    );
  }
});

var TaskInfoView = React.createClass({
  handleClick: function(event) {
    // TODO change to use react.js style.
    // ref: http://stackoverflow.com/questions/30034265/trigger-child-re-rendering-in-react-js
    var current_target = $(event.target);
    current_target.parents("#nodes_groups").find("li").removeClass("highlighted");
    current_target.addClass("highlighted");
    return TaskDetailView_render(this.props.node_label, nodeedge.graph_infos);
  },
  task_attrs: {},
  render: function() {
      var node_label = this.props.node_label;
      var task_cls = node_label.slice(0, node_label.indexOf('('));
      return (
        <li onClick={this.handleClick} key={this.props.group_idx + ' ' + this.props.node_idx} data-task-id={node_label} data-task_cls={task_cls} >
          { task_cls + "." + nodeedge.nodeid_to_node_dict[node_label].package_name }
        </li>
      );
  }
});

var TaskGroupsView_render = function(nodes_groups) {
  React.render(
    <TaskGroupsView nodes_groups={ nodes_groups } />,
    $("#nodes_groups")[0]
  );
};

var TaskDetailView = React.createClass({
  // show task code
  render: function() {
    var ref = this.props.params;
    var graph_infos = this.props.graph_infos;

    var require_task_names_direct = graph_infos.requires.direct[ref.task_name];
    var require_task_names_direct_total = _.difference(graph_infos.requires.total[ref.task_name], require_task_names_direct);

    var upons_task_names_direct = graph_infos.upons.direct[ref.task_name];
    var upons_task_names_direct_total = _.difference(graph_infos.upons.total[ref.task_name], upons_task_names_direct);

    return (
      <table className="table">
        <tbody>
          <tr>
            <td>Task name</td>
            <td>{ref.task_name}</td>
          </tr>
          <tr>
            <td>Output HDFS path</td>
            <td><a target="_blank" href={ref.hdfs_path_in_hue}>{ref.hdfs_path}</a></td>
          </tr>
          <tr>
            <td>task file path</td>
            <td><a target="_blank" href={ref.task_file_url}>{ref.task_file}</a></td>
          </tr>
          <tr>
            <td>task doc</td>
            <td><pre className="well">{ref.task_doc}</pre></td>
          </tr>
          <tr>
            <td>tasks requires direct</td>
            <td>
              <TaskLinksView task_names={ require_task_names_direct } />
            </td>
          </tr>
          <tr>
            <td>tasks requires total (without direct)</td>
            <td>
              <TaskLinksView task_names={ require_task_names_direct_total } />
            </td>
          </tr>
          <tr>
            <td>tasks upons direct</td>
            <td>
              <TaskLinksView task_names={ upons_task_names_direct } />
            </td>
          </tr>
          <tr>
            <td>tasks upons total (without direct)</td>
            <td>
              <TaskLinksView task_names={ upons_task_names_direct_total } />
            </td>
          </tr>
        </tbody>
      </table>
    );
  }
});

var TaskLinkView = React.createClass({
  render: function() {
    var task_name = this.props.task_name;
    var task_info = ptm.task_instance_repr_to_info[task_name] || {};

    var url = URI(window.location);
    var query = _.extend({}, task_info.param_kwargs, {"task_cls": task_info.task_cls})
    url.addQuery(query);
    var link = url.build();

    return (
      <a href={ link }>
        <h4>
          <span className="label label-default task-link">
            { task_name }
          </span>
        </h4>
      </a>
    );
  }
});

var TaskLinksView = React.createClass({
  render: function() {
    var task_names = this.props.task_names;
    return (
      <pre className="well">
        { (task_names || []).map(function(dep1) {
          return <TaskLinkView key={dep1} task_name={dep1}/>
        }) }
     </pre>
    );
  }
});

var TaskDetailView_render = function(task_id, graph_infos) {
  var ref = nodeedge.nodeid_to_node_dict[task_id];

  var task_file = ref["task_file"];
  var task_package = task_file.split("/")[0];

  var params = {
    task_name: ref["id"],
    hdfs_path: ref["data_file"],
    task_doc: ref["task_doc"],
    hdfs_path_in_hue: queryparams.luiti_visualiser_env.file_web_url_prefix + ref["data_file"],
    task_file: task_file,
    task_file_url: "/luiti/code/" + task_package + "/" + ref["label"],
    graph_infos: graph_infos,
  }
  React.render(
    <TaskDetailView params={params} graph_infos={graph_infos}/>,
    $("#task_detail")[0]
  );
};

var exports = {
  "views": {
    "LoadTasksErrors": LoadTasksErrorsView,
    "TaskGroupsSummary": TaskGroupsSummaryView,
    "TaskGroups": TaskGroupsView,
    "TaskGroup": TaskGroupView,
    "TaskInfo": TaskInfoView,
    "TaskDetail": TaskDetailView,
  },
  "renders": {
    "LoadTasksErrors": LoadTasksErrorsView_render,
    "TaskGroupsSummary": TaskGroupsSummaryView_render,
    "TaskGroups": TaskGroupsView_render,
    "TaskDetail": TaskDetailView_render,
  },
}
exports;  // return eval value.
