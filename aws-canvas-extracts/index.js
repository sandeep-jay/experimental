var _ = require('lodash');
var async = require('async');
var config = require('config');
var csv = require('fast-csv');
var fs = require('fs');
var glob = require('glob');
var shortid = require('shortid');
var util = require('util');

var AWS = require('aws-sdk')

var log = require('./logger');
var redshiftUtil = require('./util');
// var schema = require('../lib/schema');

var argv = require('yargs')
  .usage('Usage: $0 --max-old-space-size=8192')
  .describe('u', 'Download the latest Canvas Redshift data files')
  .help('h')
  .alias('h', 'help')
  .argv;

var s3 = new AWS.S3();
/**
 * Download the complete snapshot of Canvas Redshift data files. This api call
 * retrieves full and partial file dumps up until the last full dump.
 * The API returns filenames that are globally unique but stable, which allows
 * for the client to download files that are not available locally and ignore the
 * files downloaded previously.
 * Note - Canvas API has a 50 GB cap on downloads per request.
 *
 * @param  {Function}             callback              Standard callback function
 * @api private
 */

var putObjectToS3 = function putObjectToS3(data, key){
  var s3 = new AWS.S3();

// TODO: Remove this. Use the AWS credentials for local deployments only
// Not required if you are running on lambda using IAM roles
/*
  AWS.config.update({
    accessKeyId: config.get('aws.credentials.accessKeyId'),
    secretAccessKey: config.get('aws.credentials.secretAccessKey'),
    region: config.get('aws.credentials.region')
  });
*/
  var d = new Date();
  var randomId = shortid.generate();
  var filename = util.format('extracts/%d/%d/%d/%d/%d/%s/dumpList.json',
      d.getFullYear(),
      d.getMonth() + 1,
      d.getDate(),
      d.getHours(),
      d.getMinutes(),
      randomId
    );
  var params = {
     'Bucket' : config.get('aws.s3.bucket') ,
     'Body' : JSON.stringify(data),
     'Key': filename,
     'ServerSideEncryption': 'AES256'
  }
  s3.putObject(params, function(err, data) {
    if (err){
      console.error({'err': err}, 'Unable to store file in S3 bucket');
      //return callback(err);
    } // an error occurred
    else
      console.info("Upload successful to S3 bucket");           // successful response
  });
  //return callback();
}


var downloadFiles = function(callback) {
  // Get the list of files, tables and signed URLS that contains a complete Canvas data snapshots
  redshiftUtil.canvasDataApiRequest('/file/sync', function(fileDump) {
    var files = [];
    for (var i = fileDump.files.length - 1; i >= 0; i--) {
      files = files.concat(fileDump.files[i]);
    }
    console.info("Complete Canvas dump files:\n");

    //fs.writeFile('./data.json', JSON.stringify(files), 'utf-8');
    putObjectToS3(files, key);

    console.info("Upload successful to S3 bucket.Closing application");

    //Download the files to disk
    redshiftUtil.downloadFiles('raw', files, function(filenames) {
      console.info('Finished downloading files');
    });
  });
};

exports.handler = function (event, context) {
downloadFiles(function(){ });

}
