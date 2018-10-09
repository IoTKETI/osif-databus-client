var HazelcastClient = require('hazelcast-client').Client
  , Config = require('hazelcast-client').Config
  , uuidv4 = require('uuid/v4')
  , debug = require('debug')('osif-client-test');

var OsifClient = require('../index.js').Client;



// pre process
//  osif-node-platform write device info to local data grid

function _pre_simulateDevicePlatform() {

  return new Promise((resolve, reject)=>{
    try {
      var localConfig = new Config.ClientConfig();
      localConfig.networkConfig.addresses = [{
        host: 'localhost',
        port: 5701
      }];

      var globalConfig = new Config.ClientConfig();
      globalConfig.networkConfig.addresses = [{
        host: 'localhost',
        port: 5701
      }];

      var _hazelcastClient = null;

      HazelcastClient.newHazelcastClient(localConfig)
        .then((hazelcastClient)=>{
          _hazelcastClient = hazelcastClient;

          //  Write device info to local data grid
          var loalDeviceInfoMap = _hazelcastClient.getMap(OsifClient.RESERVED_KEY.OSIF_DEVICE_INFO);
          return loalDeviceInfoMap.set(OsifClient.RESERVED_KEY.OSIF_DEVICE_ID, __deviceId);
        })

        .then(()=> {

          return HazelcastClient.newHazelcastClient(localConfig);
        })

        .then((hazelcastClient)=>{

          //  Write running device info to global data grid
          var runningDeviceMap = hazelcastClient.getMap(OsifClient.RESERVED_KEY.OSIF_RUNNING_DEVICE);
          return runningDeviceMap.set(__deviceId, {deviceId: __deviceId, name: 'Test device'});
        })

        .then(()=>{

          resolve();
        })

        .catch(function(err){
          reject(err);
        });
    }
    catch(ex) {
      debug('EXCEPTION:', ex);
      reject(ex);
    }
  });
}

var aaa = function (value) {
  console.log(key, ' is updated ', value, arguments);
}

function _pre_listenOpenData() {


  return new Promise((resolve, reject)=>{
    try {
      var options = require('./osif-service2.json');
      var client2 = new OsifClient(options);
      __client2 = client2;

      client2.init()
        .then((client)=> {
          return client.startService();
        })

        .then(function (client) {



          client.subscribeToGlobalOpendata(
            key,
            aaa);

          resolve(client);
        })

        .catch((err)=>{
          reject(err);
        })

    }
    catch(ex) {
      debug('EXCEPTION:', ex);
      reject(ex);
    }
  });
}


function __template() {
  return new Promise((resolve, reject)=>{
    try {

    }
    catch(ex) {
      debug("EXCEPTION:", ex);
      reject(ex);
    }
  });
}



function _test_startService() {

  return new Promise((resolve, reject)=>{
    try {

      var options = require('./osif-service.json');
      var client1 = new OsifClient(options);
      __client1 = client1;

      client1.init()
        .then((client)=> {
          return client.startService();

        })

        .then(function (client) {

          resolve(client);
        })

        .catch((err)=>{
          reject(err);
        })
    }
    catch(ex) {
      debug("EXCEPTION:", ex);
      reject(ex);
    }
  });
}


function _test_setGlobalData() {
  return new Promise((resolve, reject)=>{
    try {

      debug('_test_setGlobalData');
      __client1.init()
        .then((client)=> {

          return __client1.setGlobalAppData(keyName, 'new value');
        })

        .then(function (result) {

          resolve(result);
        })

        .catch((err)=>{
          reject(err);
        })
    }
    catch(ex) {
      debug("EXCEPTION:", ex);
      reject(ex);
    }
  });
}

function _error(err){
  debug("ERROR:", err);
}


_pre_simulateDevicePlatform()
  .then(_test_startService)
  .then(_pre_listenOpenData)
  .then(_test_setGlobalData)
  .catch(_error);



//
//
//
//
//
//
// client1.init()
//   .then(function(client){
//     client.startService()
//       .then(function(value){
//         console.log( 'startService', value );
//
//         return client.stopService();
//       })
//
//       .then(function(value){
//         console.log( 'stopService', value );
//
//         var appData = {
//           "goName-1":"global-od-value1",
//           "goName-2":"global-od-value2",
//           "goName-3":"global-od-value3",
//           "goName-4":"global-od-value4"
//         }
//
//         return client.setGlobalAppData("wether", appData);
//       })
//
//       .then(function(value){
//         console.log( 'setGlobalAppData',  value );
//
//         var appData = {
//           "loName-1":"global-od-value1",
//           "loName-2":"global-od-value2",
//           "loName-3":"global-od-value3",
//           "loName-4":"global-od-value4"
//         }
//
//         return client.setLocalAppData(appData);
//       })
//
//
//       .then(function(value){
//         console.log( 'setLocalAppData', value );
//
//
//         return client.getGlobalAppData();
//       })
//
//
//       .then(function(value){
//         console.log( 'getGlobalAppData', value );
//
//         return client.getLocalAppData();
//       })
//
//
//       .then(function(value){
//         console.log( 'getLocalAppData', value );
//
//
//         return client.getGlobalOpendata('global-open-data');
//       })
//
//
//       .then(function(value){
//         console.log( 'getGlobalOpendata', value );
//
//         return client.getLocalOpendata('local-open-data');
//       })
//
//
//
//       .then(function(value){
//         console.log( 'getLocalOpenata', value );
//
//
//         var listener = {
//           'updated':     function listener(value) {
//             console.log( 'local listener: ', value );
//           }
//         };
//
//
//         return client.subscribeToGlobalOpendata('global-open-data-222', listener);
//       })
//
//
//       .then(function(value){
//         console.log( 'subscribeToGlobalOpendata', value );
//
//         var listener = {
//           'updated':     function listener(value) {
//             console.log( 'local listener: ', value );
//           }
//         };
//
//
//         return client.subscribeToLocalOpendata('local-open-data-222', listener);
//       })
//
//       .then(function(value){
//         console.log( 'subscribeToLocalOpendata', value );
//       })
//
//       .catch(function(err) {
//         console.log( err );
//       })
//     ;
//   });