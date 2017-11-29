/**
 * Created by kimtaehyun on 2017. 7. 14..
 */

const HazelcastClient = require('hazelcast-client').Client
  , Config = require('hazelcast-client').Config
  , uuidv4 = require('uuid/v4')
  , debug = require('debug')('ciot-databus-client')
;


const RESERVED_KEY = {
  "CIOT_APP_MANAGER_REQ": "CIOT_APP_MANAGER_REQ",
  "CIOT_OPENDATA_LIST": "CIOT_OPENDATA_LIST"
};


var GLOBAL_NODE_ADDRESS = "dev.synctechno.com";
var MAP_NAME = "CUSTOMERS2";

var ketiCiotGlobalObjectKeys = {
  'CIOT_PROCESS': 'CIOT_PROCESS',
  'CIOT_TEST': 'CIOT_TEST',
  'CUSTOMERS2': 'CUSTfffOMERS2',
  'CIOT_DEMO_SWITCH': 'CIOT_DEMO_SWITCH'
};


/**
 *
 *
 * @param options
 *   {
 *     "application": {
 *       "instanceId": 'uuid',
 *       "appName": 'application name from package.json',
 *       "openData": {
 *         "local": {
 *           "name": "open data name",
 *           "description": "data description",
 *           "template": {object template with default value}
 *         },
 *         "global: {
 *           "name": "open data name",
 *           "description": "data description",
 *           "template": {object template with default value}
 *         }
 *       }
 *     }
 *
 *     "databus": {
 *       "global": {
 *         "host": "ip.address.of.cloud",
 *         "port": "port",
 *       },
 *       "local": {
 *         "host": "ip.address.of.appmanager",
 *         "port": "port"
 *       }
 *     }
 *   }
 *
 */
function CiotDatabusClient(options) {
  /**
   * Options
   */
  options = options || {};

  try {
    _assertProperty(options.application.appName);
    _assertProperty(options.databus.global.host);
    _assertProperty(options.databus.global.port);
    _assertProperty(options.databus.local.host);
    _assertProperty(options.databus.local.port);
  } catch (ex) {
    debug( ex );
    throw Error('Failed on creating new CiotDatabusClient object: missing required options');
  }


  /**
   * Member Variables
   *
   */
  this.databus = options.databus;
  this.application = options.application;
  this.application.instanceId = options.application.instanceId || uuidv4();

  this.subscriptionList = {
    "global": {},
    "local": {}
  };

  this.clientInitialized = false;
}


/**
 * Member Functions
 *
 */
CiotDatabusClient.prototype.init = _initClient;


CiotDatabusClient.prototype.startApplication = _startApplication;
CiotDatabusClient.prototype.stopApplication = _stopApplication;


// read and write my application data
CiotDatabusClient.prototype.setGlobalAppData = _setGlobalAppData;
CiotDatabusClient.prototype.getGlobalAppData = _getGlobalAppData;

CiotDatabusClient.prototype.setLocalAppData = _setLocalAppData;
CiotDatabusClient.prototype.getLocalAppData = _getLocalAppData;


//  read open data which is owned by other application
CiotDatabusClient.prototype.getGlobalOpendata = _getGlobalOpendata;

CiotDatabusClient.prototype.getLocalOpendata = _getLocalOpendata;

//  subscribe/unsubscribe to open data which is owned by other application
CiotDatabusClient.prototype.subscribeToLocalOpendata = _subscribeToLocalOpendata;
CiotDatabusClient.prototype.unsubscribeToLocalOpendata = _unsubscribeToLocalOpendata;

CiotDatabusClient.prototype.subscribeToGlobalOpendata = _subscribeToGlobalOpendata;
CiotDatabusClient.prototype.unsubscribeToGlobalOpendata = _unsubscribeToGlobalOpendata;


/**
 * Implementations of Private functions for internal use.
 */
function _assert(condition, message) {
  if (!condition) {
    throw message || "Assertion failed";
  }
}


function _assertProperty(condition, message) {
  if (typeof condition === 'undefined') {
    throw message || "Assertion failed";
  }
}


function _createHazelcastClient(host, port) {
  return new Promise(function (resolve, reject) {
    try {
      var config = new Config.ClientConfig();
      config.networkConfig.addresses = [{
        host: host,
        port: port
      }];

      HazelcastClient.newHazelcastClient(config)
        .then(function(hazelcastClient){
          resolve(hazelcastClient)
        })
        .catch(function(err){
          reject(err);
        });
    }
    catch (ex) {
      debug( ex );
      reject(ex);
    }
  });
}




/**
 * Implementations of Member Functions
 *
 */

function _initClient() {
  var thisClient = this;
  return new Promise(function (resolve, reject) {
    try {
      debug( 'initClient 11', thisClient.clientInitialized )
      if (thisClient.clientInitialized)
        resolve(thisClient);
      else {
        debug( 'initClient 22', thisClient.databus.global.host, thisClient.databus.global.port )

        _createHazelcastClient(thisClient.databus.global.host, thisClient.databus.global.port)
          .then(function(globalDatabusClient){
            debug( 'initClient 33' )

            thisClient.globalDatabusClient = globalDatabusClient;
            debug( 'initClient 44' )

            return _createHazelcastClient(thisClient.databus.local.host, thisClient.databus.local.port)
          })
          .then(function(localDatabusClient){
            thisClient.localDatabusClient = localDatabusClient;
            debug( 'initClient 55' )

            thisClient.clientInitialized = true;
            resolve(thisClient);
          })
          .catch(function(err){
            debug( 'initClient error' )

            debug( err );
            reject(err);
          });
      }
    }
    catch (ex) {
      debug( ex );
      reject(ex);
    }
  });
}


function _startApplication() {
  var thisClient = this;
  return new Promise(function(resolve, reject){
    try {
      thisClient.init()
        .then(function(thisClient){
          var appManagerRequestQueue = thisClient.localDatabusClient.getQueue(RESERVED_KEY.CIOT_APP_MANAGER_REQ);

          appManagerRequestQueue.put({
            "state": "start",
            "instance": thisClient.application
          })
            .then(function(result){
              resolve( result );
            })
            .catch(function(err){
              reject(err);
            });
        })
    }
    catch(ex) {
      debug(ex);
      reject(ex);
    }
  });
}


function _stopApplication() {
  var thisClient = this;
  return new Promise(function(resolve, reject){
    try {
      thisClient.init()
        .then(function(thisClient){
          var appManagerRequestQueue = thisClient.localDatabusClient.getQueue(RESERVED_KEY.CIOT_APP_MANAGER_REQ);
          appManagerRequestQueue.put({
            "state": "stop",
            "instanceId": thisClient.application.instanceId
          })
            .then(function(result){
              resolve(result);
            })
            .catch(function(err){
              reject(err);
            });
        });
    }
    catch(ex) {
      debug(ex);
      reject(ex);
    }
  });
}



function _setGlobalAppData(data) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      _assertProperty(thisClient.application.openData.global.name);

      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.globalDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.put(thisClient.application.openData.global.name, data);
        })

        .then(function(result){
          resolve(result);
        })

        .catch(function (err) {
          debug(err);
          reject(err);
        });
    }
    catch(ex) {
      debug(ex);
      reject(ex);
    }
  });
}



function _setLocalAppData(data) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      _assertProperty(thisClient.application.openData.local.name);

      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.localDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.put(thisClient.application.openData.local.name, data);
        })

        .then(function(result) {
          resolve(result);
        })

        .catch(function (err) {
          debug(err);
          reject(err);
        });
    }
    catch(ex) {
      debug(ex);
      reject(ex);
    }
  });
}



function _getGlobalAppData() {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      _assertProperty(thisClient.application.openData.global.name);

      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.globalDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.get(thisClient.application.openData.global.name);
        })

        .then(function(result) {
          resolve(result);
        })

        .catch(function (err) {
          debug(err);
          reject(err);
        });
    }
    catch(ex) {
      debug(ex);
      reject(ex);
    }
  });
}



function _getLocalAppData() {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      _assertProperty(thisClient.application.openData.local.name);

      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.localDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.get(thisClient.application.openData.local.name);
        })

        .then(function(result) {
          resolve(result);
        })

        .catch(function (err) {
          debug(err);
          reject(err);
        });
    }
    catch(ex) {
      debug(ex);
      reject(ex);
    }

  });
}




function _getGlobalOpendata(name) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.globalDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.get(name);
        })

        .then(function(result) {
          resolve(result);
        })

        .catch(function (err) {
          debug(err);
          reject(err);
        });
    }
    catch(ex) {
      debug(ex);
      reject(ex);
    }
  });
}




function _getLocalOpendata(name) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.localDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.get(name);
        })

        .then(function(result) {
          resolve(result);
        })

        .catch(function (err) {
          debug(err);
          reject(err);
        });
    }
    catch(ex) {
      debug(ex);
      reject(ex);
    }
  });
}




function _subscribeToGlobalOpendata(key, listener) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      thisClient.init()
        .then(function (thisClient) {
          var openDataList = thisClient.globalDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          if(openDataList) {
            var listenerList = thisClient.subscriptionList.local[key];
            if(typeof listenerList === 'undefined') {
              listenerList = [];
              thisClient.subscriptionList.local[key] = listenerList;
            }

            if( listenerList.indexOf(listener) == -1 ) {
              openDataList.addEntryListener(listener, key)

                .then(function(result) {
                  resolve(result);
                })

                .catch(function(err) {
                  debug(err);
                  reject(err);
                } );


            }
            else {
              resolve('');
            }
          }
          else {
            debug('key is not exists');
            reject('key is not exists');
          }
        })
        .catch(function (err) {
          debug( err );
          reject(err);
        });
    }
    catch (ex) {
      debug( ex );
      reject(ex);
    }

  });
}



function _subscribeToLocalOpendata(key, listener) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      thisClient.init()
        .then(function (thisClient) {
          var openDataList = thisClient.localDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          if(openDataList) {
            var listenerList = thisClient.subscriptionList.local[key];
            if(typeof listenerList === 'undefined') {
              listenerList = [];
              thisClient.subscriptionList.local[key] = listenerList;
            }

            if( listenerList.indexOf(listener) == -1 ) {
              openDataList.addEntryListener(listener, key)
                .then(function(result){
                  resolve( result );
                })
                .catch(function(err){
                  debug(err);
                  reject(err);
                });
            }
            else {
              resolve('');
            }
          }
          else {
            debug('key is not exists');
            reject('key is not exists');
          }
        })
        .catch(function (err) {
          debug( err );
          reject(err);
        });
    }
    catch (ex) {
      debug( ex );
      reject(ex);
    }

  });
}


function _unsubscribeToGlobalOpendata(key, listener) {
  throw new Error('not implemented yet');
}


function _unsubscribeToLocalOpendata(key, listener) {
  throw new Error('not implemented yet');
}


/**
 * Expose 'CiotDatabusClient'
 */
module.exports = CiotDatabusClient;

