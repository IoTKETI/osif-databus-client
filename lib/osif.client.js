/**
 * Created by TaeHyun KIM @ SyncTechno.com on 2017. 7. 14.
 *
 * This project is supported by KETI Cognitive IoT Project.
 */

var HazelcastClient = require('hazelcast-client').Client
  , Config = require('hazelcast-client').Config
  , uuidv4 = require('uuid/v4')
  , debug = require('debug')('osif')
;


var RESERVED_KEY = {
  "OSIF_DEVICE_INFO": "OSIF_DEVICE_INFO",
  "OSIF_LOCAL_OPENDATA": "OSIF_LOCAL_OPENDATA",

  "OSIF_RUNNING_DEVICE": "OSIF_RUNNING_DEVICE",
  "OSIF_RUNNING_SERVICE": "OSIF_RUNNING_SERVICE",
  "OSIF_GLOBAL_OPENDATA": "OSIF_GLOBAL_OPENDATA",


  "OSIF_DEVICE_ID": "OSIF_DEVICE_ID",


  "CIOT_APP_MANAGER_REQ": "CIOT_APP_MANAGER_REQ",
  "CIOT_OPENDATA_LIST": "CIOT_OPENDATA_LIST"
};


OsifClient.RESERVED_KEY = RESERVED_KEY;

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
    _assertProperty(options.service.serviceId, 'options.service.serviceId');
    _assertProperty(options.service.versionCode, 'options.service.versionCode');
    _assertProperty(options.databus.globalHost, 'options.databus.globalHost');
    _assertProperty(options.databus.globalPort, 'options.databus.globalPort');
    _assertProperty(options.databus.localPort, 'options.databus.localPort');

    options.databus['localHost'] = '127.0.0.1'; //  default

    options.databus['group'] = {
      'name': 'osif',
      'password': 'osif-pass'
    };
  } catch (ex) {
    debug( ex );
    throw Error('Failed on creating new OsifClient object: missing required options');
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


OsifClient.prototype.readDeviceId = _readDeviceId;
OsifClient.prototype.registerServiceInstanceInfo = _registerServiceInstanceInfo;
OsifClient.prototype.unregisterServiceInstanceInfo = _unregisterServiceInstanceInfo;


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

function _createHazelcastClient(host, port, group) {
  console.log( host, port, group);

  return new Promise(function (resolve, reject) {
    try {
      var config = new Config.ClientConfig();
      config.networkConfig.addresses = [{
        host: host,
        port: port
      }];

      if(group)
        config.groupConfig = group;

      HazelcastClient.newHazelcastClient(config)
        .then(function(hazelcastClient){
          resolve(hazelcastClient)
        })
        .catch(function(err){
          debug('error:', host, port);
          reject(err);
        });
    }
    catch (ex) {
      debug( ex );
      reject(ex);
    }
  });
}

function _readDeviceId() {
  var thisObj = this;

  return new Promise((resolve, reject)=>{
    try {
      var deviceInfoMap = thisObj.localDatabusClient.getMap(RESERVED_KEY.OSIF_DEVICE_INFO);

      deviceInfoMap.get(RESERVED_KEY.OSIF_DEVICE_ID)
        .then((deviceId)=>{
          console.log( "DEVICE ID", deviceId);
          resolve(deviceId);
        })

        .catch((err)=>{
          reject(err);
        });
    }
    catch(ex) {
      debug('EXCEPTION:', ex);
      reject(ex);
    }
  });
}

function _registerServiceInstanceInfo() {
  var thisObj = this;

  return new Promise((resolve, reject) => {
    try {

      //  register running service info. to global data grid
      console.log( "set running service map:", thisObj.runningServiceId);
      thisObj.runningServiceMap.set(thisObj.runningServiceId, thisObj.service)

        .then(()=>{

          var openDataList = [];
          if(thisObj.service.openData && thisObj.service.openData.local) {
            thisObj.service.openData.local.map((item)=>{
              if(item.name)
                openDataList.push(item.name);
            });
          }

          if(openDataList.length > 0) {
            return Promise.all(
              openDataList.map((item)=>{
                var openDataName = thisObj.runningServiceId + '#' + item;
                return thisObj.localOpenDataMap.set(openDataName, '');
              })
            );
          }
          else {
            return [];
          }

        })

        .then((result)=>{

          var openDataList = [];
          if(thisObj.service.openData && thisObj.service.openData.global) {
            thisObj.service.openData.global.map((item)=>{
              if(item.name)
                openDataList.push(item.name);
            });
          }

          if(openDataList.length > 0) {
            return Promise.all(
              openDataList.map((item)=>{
                var openDataName = thisObj.runningServiceId + '#' + item;
                return thisObj.globalOpenDataMap.set(openDataName, '');
              })
            );
          }
          else {
            return [];
          }

        })

        .then((result)=>{
          resolve(thisObj);
        })


        .catch((err)=>{
          debug('ERROR:', err);
          reject(err);
        })
    }
    catch (ex) {
      debug('EXCEPTION:', ex);
      reject(ex);
    }
  });
}

function _unregisterServiceInstanceInfo() {
  var thisObj = this;

  return new Promise((resolve, reject) => {
    try {

      //  register running service info. to global data grid
      thisObj.runningServiceMap.delete(thisObj.runningServiceId)
        .then(()=>{

          var openDataList = [];
          if(thisObj.service.openData && thisObj.service.openData.local) {
            thisObj.service.openData.local.map((item)=>{
              if(item.name)
                openDataList.push(item.name);
            });
          }

          if(openDataList.length > 0) {
            return Promise.all(
              openDataList.map((item)=>{
                var openDataName = thisObj.runningServiceId + '#' + item;
                return thisObj.localOpenDataMap.delete(openDataName);
              })
            );
          }
          else {
            return [];
          }

        })

        .then((result)=>{

          var openDataList = [];
          if(thisObj.service.openData && thisObj.service.openData.global) {
            thisObj.service.openData.global.map((item)=>{
              if(item.name)
                openDataList.push(item.name);
            });
          }

          if(openDataList.length > 0) {
            return Promise.all(
              openDataList.map((item)=>{
                var openDataName = thisObj.runningServiceId + '#' + item;
                return thisObj.globalOpenDataMap.delete(openDataName);
              })
            );
          }
          else {
            return [];
          }

        })

        .then((result)=>{
          resolve(thisObj);
        })


        .catch((err)=>{
          debug('ERROR:', err);
          reject(err);
        })
    }
    catch (ex) {
      debug('EXCEPTION:', ex);
      reject(ex);
    }
  });
}


/**
 * Implementations of Member Functions
 *
 */
function _initClient() {
  var thisObj = this;
  return new Promise(function (resolve, reject) {
    try {
      if (thisObj.clientInitialized)
        resolve(thisObj);
      else {
        _createHazelcastClient(thisObj.databus.globalHost, thisObj.databus.globalPort, thisObj.databus.group)
          .then(function(globalDatabusClient){
            thisObj.globalDatabusClient = globalDatabusClient;

            return _createHazelcastClient(thisObj.databus.localHost, thisObj.databus.localPort)
          })
          .then(function(localDatabusClient) {
            thisObj.localDatabusClient = localDatabusClient;

            return thisObj.readDeviceId();
          })

          .then((deviceId)=> {
            thisObj.deviceId = deviceId;
            //thisObj.runningServiceId = [deviceId, thisObj.service.serviceName].join('#');
            thisObj.runningServiceId = thisObj.service.serviceId;

            thisObj.runningServiceMap = thisObj.globalDatabusClient.getMap(RESERVED_KEY.OSIF_RUNNING_SERVICE);
            thisObj.globalOpenDataMap = thisObj.globalDatabusClient.getMap(RESERVED_KEY.OSIF_GLOBAL_OPENDATA);
            thisObj.localOpenDataMap  = thisObj.localDatabusClient.getMap(RESERVED_KEY.OSIF_LOCAL_OPENDATA);

            thisObj.clientInitialized = true;

            resolve(thisObj);
          })

          .catch(function(err){
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
        .then((client)=> {

          // register service info to Global.OSIF_RUNNING_DEVICE
          return thisClient.registerServiceInstanceInfo();
        })

        .then((client)=>{
          resolve(thisClient);
        })

        .catch(function(err){
          reject(err);
        });
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
        .then((client)=> {
          return thisClient.unregisterServiceInstanceInfo();

        })

        .then((client)=>{
          resolve(client);
        })

        .catch((err)=>{
          reject(err);
        });
    }
    catch(ex) {
      debug(ex);
      reject(ex);
    }
  });
}

function _setGlobalAppData(key, value) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {

      var dataInfo = thisClient.service.openData.global.find((item)=>{
        if(item.name == key)
          return true;
        else
          return false;
      });

      if(!dataInfo)
        throw new Error('Illegal argument');


      thisClient.init()
        .then(function(thisClient){
          var key = thisClient.runningServiceId + "#" + dataInfo.name;
console.log('SET GLOBAL APP DATA:', key, value );
          return thisClient.globalOpenDataMap.set(key, value);
        })

        .then(function(result){
          resolve(value);
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

function _setLocalAppData(key, value) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      var dataInfo = thisClient.service.openData.local.find((item)=>{
        if(item.name == key)
          return true;
        else
          return false;
      });

      if(!dataInfo)
        throw new Error('Illegal argument');


      thisClient.init()
        .then(function(thisClient){
          var key = thisClient.runningServiceId + "#" + dataInfo.name;

          return thisClient.localOpenDataMap.set(key, value);
        })

        .then(function(result){
          resolve(value);
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

function _getGlobalAppData(key) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      var dataInfo = thisClient.service.openData.global.find((item)=>{
        if(item.name == key)
          return true;
        else
          return false;
      });

      if(!dataInfo)
        throw new Error('Illegal argument');

      thisClient.init()
        .then(function(thisClient){
          var key = thisClient.runningServiceId + "#" + dataInfo.name;

          return thisClient.globalOpenDataMap.get(key);
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

function _getLocalAppData(key) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {
      var dataInfo = thisClient.service.openData.local.find((item)=>{
        if(item.name == key)
          return true;
        else
          return false;
      });

      if(!dataInfo)
        throw new Error('Illegal argument');

      thisClient.init()
        .then(function(thisClient){
          var key = thisClient.runningServiceId + "#" + dataInfo.name;

          return thisClient.localOpenDataMap.get(key);
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

function _getGlobalOpendata(serviceId, name) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {

      thisClient.init()
        .then(function(thisClient){
          var key = serviceId + "#" + name;

          return thisClient.globalOpenDataMap.get(key);
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

function _getLocalOpendata(serviceId, key) {
  var thisClient = this;

  return new Promise(function (resolve, reject) {
    try {

      thisClient.init()
        .then(function(thisClient){
          var key = serviceId + "#" + name;

          return thisClient.localOpenDataMap.get(key);
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

function _subscribeToGlobalOpendata(serviceId, key, listener) {
  var thisClient = this;
  var openDataKey = serviceId + "#" + key;

  return new Promise(function (resolve, reject) {
    try {

      thisClient.init()
        .then(function (thisClient) {
          if(thisClient.globalOpenDataMap) {
            var listenerList = thisClient.subscriptionList.global[openDataKey];
            if(typeof listenerList === 'undefined') {
              listenerList = [];
              thisClient.subscriptionList.global[openDataKey] = listenerList;
            }

            if( listenerList.indexOf(listener) == -1 ) {
              listenerList.push(listener);

              var listenerObj = {
                'updated': listener,
                'added': listener
              };
console.log( "LISTEN TO: ", openDataKey );
              thisClient.globalOpenDataMap.addEntryListener(listenerObj, openDataKey, true)

                .then(function(result) {
                  resolve(result);
                })

                .catch(function(err) {
                  debug(err);
                  reject(err);
                } );
            }
            else {
              resolve(thisClient);
            }
          }
          else {
            throw new Error('Cannot access global open data');
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
  var openDataKey = serviceId + "#" + key;

  return new Promise(function (resolve, reject) {
    try {

      thisClient.init()
        .then(function (thisClient) {
          if(thisClient.localOpenDataMap) {
            var listenerList = thisClient.subscriptionList.local[openDataKey];
            if(typeof listenerList === 'undefined') {
              listenerList = [];
              thisClient.subscriptionList.local[openDataKey] = listenerList;
            }

            if( listenerList.indexOf(listener) == -1 ) {
              listenerList.push(listener);

              var listenerObj = {
                'updated': listener,
                'added': listener
              };

              thisClient.localOpenDataMap.addEntryListener(listenerObj, openDataKey, true)

                .then(function(result) {
                  resolve(result);
                })

                .catch(function(err) {
                  debug(err);
                  reject(err);
                } );
            }
            else {
              resolve(thisClient);
            }
          }
          else {
            throw new Error('Cannot access local open data');
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
 * Expose 'OsifClient'
 */
module.exports = OsifClient;

