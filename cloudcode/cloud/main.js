
var _ = require('underscore');

// The SimpleTask class should be secured via the data browser to remove
//   public access.  All operations on this will use the master key.
var SimpleTask = Parse.Object.extend('SimpleTask');

// This function creates a SimpleTask record for processing by a background job.
// it accepts scalar parameters and also an array of Parse objects
function taskCreator(taskAction, params, objects) {
  var task = new SimpleTask();
  task.setACL({});
  var targetParams = (params && params.length) ? params : [];
  var targetObjects = (objects && objects.length) ? objects : [];
  return task.save({
    'taskAction' : taskAction,
    'taskParameters' : targetParams,
    'taskObjects' : targetObjects,
    'taskClaimed' : 0,
    'taskStatus' : 'new',
    'taskMessage' : ''
  }, { useMasterKey : true });
}

// Available actions are defined here and link to their function.
var WorkActions = {
  'simpleTask1' : simpleTask1
};

// Creates a SumObject record with the sum of the parameters, and a pointer to
//   the first parameter object.
function simpleTask1(task, params, objects) {
  var sumObject = new Parse.Object('SumObject');
  var sum = _.reduce(params, function(memo, num) {
    return memo + num;
  }, 0);
  return sumObject.save({
    'sumValue' : sum,
    'sumObject' : objects[0]
  });
}

// An afterSave hook on the TestObject class will create a task to perform
//   processing later.
Parse.Cloud.afterSave('TestObject', function(request) {
  taskCreator('simpleTask1', [1, 2, 3], [request.object]);
});


// This background job is run ad-hoc/scheduled, and processes outstanding tasks.
Parse.Cloud.job('simpleWorkQueue', function(request, status) {
  Parse.Cloud.useMasterKey();
  // Query for simple tasks which haven't been processed
  var query = new Parse.Query(SimpleTask);
  query.equalTo('taskClaimed', 0);
  // Include any target objects
  query.include('taskObjects');
  var processed = 0;
  query.each(function(task) {
    // This block will return a promise which is manually resolved to prevent
    //   errors from bubbling up.
    var promise = new Parse.Promise();
    processed++;
    var params = task.get('taskParameters');
    var objects = task.get('taskObjects');
    // The taskClaimed field is atomically incremented to ensure that it is
    //   processed only once.
    task.increment('taskClaimed');
    task.save().then(function(task) {
      var action = task.get('taskAction');
      // invalid actions not defined by WorkActions are discarded and will not
      //   be processed again.
      if (task.get('taskClaimed') == 1 && WorkActions[action]) {
        WorkActions[action](task, params, objects).then(function() {
          promise.resolve();
        }, function() {
          promise.resolve();
        });
      } else {
        promise.resolve();
      }
    });
    return promise;
  }).then(function() {
    status.success('Processing completed. (' + processed + ' tasks)')
  }, function(err) {
    console.log(err);
    status.error('Something failed!  Check the cloud log.');
  });
});