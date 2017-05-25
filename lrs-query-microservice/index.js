var AWS = require('aws-sdk');

AWS.config.region = "us-west-2";
AWS.config.apiVersions = {
  rds: '2013-09-09',
};

var pg = require('pg');
var config = require('config');
var pgp = require('pg-promise');
var auth = require('basic-auth');
var joi = require('joi');

exports.handler = function(event, context, callback) {

  var connected = false;
  var conString = "postgres://"+config.get('db.username')+":"+config.get('db.password')+"@"+config.get('db.host')+":"+config.get('db.port')+"/"+config.get('db.database');
  var connection = new pg.Client(conString);
  connection.connect();
  connected = true;
  var tenant = null;

  if(connected) {
    // Extract the basic authentication credentials
    var userId = event.params.userId;
    console.log("userID : " + userId);
    var credentials = {
      'username' : event.headers.username,
      'password' : event.headers.password
    };

    // Validate that credentials are present
    var validationSchema = joi.object().keys({
      'username': joi.string().required(),
      'password': joi.string().required()
    });

    console.log("Event :" + JSON.stringify(event));
    var validationResult = joi.validate(credentials, validationSchema);

    // Return error message if validation fails
    if (validationResult.error) {
      return callback({'code': 400, 'msg': validationResult.error.details[0].message});
    }
    var counter = 0;
    var query = connection.query("SELECT tenant_id FROM read_credentials WHERE key= '"+ String(credentials.username) + "' and secret = '" + String(credentials.password) + "'");
    query.on("row", function(row, result) {
      result.addRow(row);
      console.log('Tenant row :' + typeof(row.tenant_id));
      tenant = row.tenant_id;
    });
    query.on("end", function() {
      // When the tenant is found then the authentication is verified
      // Proceed with retrieving the statements
      if(tenant) {
        query = connection.query("SELECT * FROM statements WHERE user_id = " + userId +" and tenant_id="+ tenant);
        query.on("row", function (row, result) {

          result.addRow(row);

        });
        query.on("end", function (result) {
          var jsonString = JSON.stringify(result.rows);
          var jsonObj = JSON.parse(jsonString);
          console.log(jsonString);
          connection.end();
          context.succeed(jsonObj);
        });
      } else {
        context.succeed("Authentication Failed. Check your credentials !!");
        connection.end();
      }
    });

  } else {
    context.succeed("Database connection failed !");
  }

};
