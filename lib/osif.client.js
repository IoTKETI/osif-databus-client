/**
 * Created by TaeHyun KIM @ SyncTechno.com on 2017. 7. 14.
 *
 * This project is supported by KETI Cognitive IoT Project.
 */

var HazelcastClient = require('hazelcast-client').Client
  , Config = require('hazelcast-client').Config
  , uuidv4 = require('uuid/v4')
  , debug = require('debug')('ciot-databus-client')
;


var RESERVED_KEY = {
  "CIOT_APP_MANAGER_REQ": "CIOT_APP_MANAGER_REQ",
  "CIOT_OPENDATA_LIST": "CIOT_OPENDATA_LIST"
};


var GLOBAL_NODE_ADDRESS = "dev.synctechno.com";
var MAP_NAME = "CUSTOMERS2";

var ketiCiotGlobalObjectKeys = {
  'CIOT_PROCESS': 'CIOT_PROCESS',
  'CIOT_TEST': 'CIOT_TEST',
  'CUSTOMERS2': 'CUSTOMERS2',
  'CIOT_DEMO_SWITCH': 'CIOT_DEMO_SWITCH'
};


/**
 *
 *
 * @param options
 *   {
 *     "service": {
 *       "serviceId": 'uuid',
 *       "serviceName": 'service name',
 *       "serviceDesc": 'description text for service',
 *       "versionCode": {
 *         major: 1, minor: 1, revision:1
 *       },
 *       "openData": {
 *         "local": [{
 *           "name": "open data name",
 *           "description": "data description",
 *           "template": "descriptive text for format of open data contents"
 *         }],
 *         "global: [{
 *           "name": "open data name",
 *           "description": "data description",
 *           "template": "descriptive text for format of open data contents"
 *         }]
 *       }
 *     }
 *
 *     "databus": {
 *       "globalHost":  "ip.address.of.cloud",
 *       "globalPort": "port",
 *       "localPort": "port"
 *     }
 *   }
 *
 */
function OsifClient(options) {
  /**
   * Options
   */
  options = options || {};

  try {
    _assertProperty(options.service.serviceName, 'options.service.serviceName');
    _assertProperty(options.service.versionCode, 'options.service.versionCode');
    _assertProperty(options.databus.globalHost, 'options.databus.globalHost');
    _assertProperty(options.databus.globalPort, 'options.databus.globalPort');
    _assertProperty(options.databus.localPort, 'options.databus.localPort');

    options.databus['localHost'] = '127.0.0.1'; //  default
  } catch (ex) {
    debug( ex );
    throw Error('Failed on creating new CiotDatabusClient object: missing required options');
  }


  /**
   * Member Variables
   *
   */
  this.databus = options.databus;
  this.service = options.service;
  this.service.instanceId = options.service.serviceId || uuidv4();

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
OsifClient.prototype.init = _initClient;


OsifClient.prototype.startService = _startService;
OsifClient.prototype.stopService = _stopService;


// read and write my service data
OsifClient.prototype.setGlobalAppData = _setGlobalAppData;
OsifClient.prototype.getGlobalAppData = _getGlobalAppData;

OsifClient.prototype.setLocalAppData = _setLocalAppData;
OsifClient.prototype.getLocalAppData = _getLocalAppData;


//  read open data which is owned by other service
OsifClient.prototype.getGlobalOpendata = _getGlobalOpendata;

OsifClient.prototype.getLocalOpendata = _getLocalOpendata;

//  subscribe/unsubscribe to open data which is owned by other service
OsifClient.prototype.subscribeToLocalOpendata = _subscribeToLocalOpendata;
OsifClient.prototype.unsubscribeToLocalOpendata = _unsubscribeToLocalOpendata;

OsifClient.prototype.subscribeToGlobalOpendata = _subscribeToGlobalOpendata;
OsifClient.prototype.unsubscribeToGlobalOpendata = _unsubscribeToGlobalOpendata;


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
        debug( 'initClient 22', thisClient.databus.globalHost, thisClient.databus.globalPort )

        _createHazelcastClient(thisClient.databus.globalHost, thisClient.databus.globalPort)
          .then(function(globalDatabusClient){
            debug( 'initClient 33' )

            thisClient.globalDatabusClient = globalDatabusClient;
            debug( 'initClient 44' )

            return _createHazelcastClient(thisClient.databus.localHost, thisClient.databus.localPort)
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


function _startService() {
  var thisClient = this;
  return new Promise(function(resolve, reject){
    try {
      thisClient.init()
        .then(function(thisClient){
          var appManagerRequestQueue = thisClient.localDatabusClient.getQueue(RESERVED_KEY.CIOT_APP_MANAGER_REQ);

          appManagerRequestQueue.put({
            "state": "start",
            "instance": thisClient.service
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


function _stopService() {
  var thisClient = this;
  return new Promise(function(resolve, reject){
    try {
      thisClient.init()
        .then(function(thisClient){
          var appManagerRequestQueue = thisClient.localDatabusClient.getQueue(RESERVED_KEY.CIOT_APP_MANAGER_REQ);
          appManagerRequestQueue.put({
            "state": "stop",
            "serviceId": thisClient.service.serviceId
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
      _assertProperty(thisClient.service.openData.global[0].name);

      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.globalDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.put(thisClient.service.openData.global[0].name, data);
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
      _assertProperty(thisClient.service.openData.local[0].name);

      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.localDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.put(thisClient.service.openData.local[0].name, data);
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
      _assertProperty(thisClient.service.openData.global[0].name);

      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.globalDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.get(thisClient.service.openData.global[0].name);
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
      _assertProperty(thisClient.service.openData.local[0].name);

      thisClient.init()
        .then(function(thisClient){
          var openDataList = thisClient.localDatabusClient.getMap(RESERVED_KEY.CIOT_OPENDATA_LIST);
          return openDataList.get(thisClient.service.openData.local[0].name);
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


/**CIOT_PROCESS
 * Expose 'CiotDatabusClient'
 */
module.exports = OsifClient;

