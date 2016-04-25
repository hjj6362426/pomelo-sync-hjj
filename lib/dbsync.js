/**
 * Module dependencies.
 */
var commands = require('./commands');
var utils = require('./utils/utils');
var Queue = require('./utils/queue');
var fs = require('fs');
var crypto = require('crypto');
var moment = require('moment');
var async = require('async');
var Rewriter = require('../lib/rewriter/rewriter');
var SyncTimer = require('../lib/timer/synctimer');
var clone = utils.clone;

var chip_size = 16 * 1024;

/**
 * 
 * DataSync Components.
 *
 * Initialize a new `DataSync` with the given `options`.
 *
 * DataSync's prototype is based on `commands` under the same directory;
 * 
 * @class DataSync
 * @constructor
 * @param {Object} options init params include aof,log,interval,mapping and mappingPath etc. 
 * 
 */
var DataSync = module.exports = function(options) {
	options = options || {};
	this.dbs = [];
	this.selectDB(0);
	this.client = options.client;
	this.aof = options.aof || false;
	this.debug = options.debug || false;
	this.log = options.log || console;
	this.interval = options.interval || 1000 * 60;
	this.flushQueue =  new Queue();
    this.recoverOn = false;
	this.mergerMap = {};
	if (!!this.aof) {
        console.log('aof open');
        this.recoverOn = options.recoverOn || false;
		if (!!options.filename) {
			this.filename = options.filename;
		} else {
            var path = process.cwd() + '/logs';
            if (!fs.existsSync(path)) {
              fs.mkdirSync(path);
            }
            this.filename = path+'/dbsync.log';
            //var path = process.cwd() + '/dblogs';
            //fs.mkdirSync(path);
            //var name = moment.format('YYYYMMDDHHmmss');
            //this.filename = path+'/'+name+'.log';
            //this.splitIndex = 0;
		}
		this.stream = fs.createWriteStream(this.filename, { flags: 'a' });
        //this.split = {beat:0, moment:''};
	}
	if (!!options.mapping){
		this.mapping = options.mapping;
	} else if (!!options.mappingPath) {
		this.mapping = this.loadMapping(options.mappingPath); 
	}
	this.rewriter = options.rewriter || new Rewriter(this);
	this.timer = options.timer || new SyncTimer();
    //console.log(this.recoverOn);
    if (!this.recoverOn) this.timer.start(this);

    //默认配置
    var path = process.cwd() + '/config';
    if (!fs.existsSync(path)) {
      fs.mkdirSync(path);
    }
    this.iniFile = path + '/sync.json';
    //this.savebeat = 0;
    this.config ={};
    var data = fs.readFileSync(this.iniFile, 'utf8');
    if (data) {
      this.config = JSON.parse(data);
    }

    this.chip_size = options.chip_size || 16 * 1024;
    //console.log('config =' + JSON.stringify(this.config));
};

/**
 * Expose commands to store.
 */
DataSync.prototype = commands;

/**
 * Select database at the given `index`.
 * @api private
 * @param {Number} index
 */

DataSync.prototype.selectDB = function(index){
  var db = this.dbs[index];
  if (!db) {
    db = {};
    db.data = {};
    this.dbs[index] = db;
  }
  this.db = db;
};

/**
 *return the first used db
 *
 * @api private
 */
DataSync.prototype.use = function() {
  this.selectDB(0);
  var db = this.dbs[0];
  var keys = Object.keys(db);
  var dbkey = keys[0];
  return db[dbkey];
};

/**
 * Lookup `key`, when volatile compare timestamps to
 * expire the key.
 *
 * @api private
 * @param {String} key
 * @return {Object}
 */

DataSync.prototype.lookup = function(key){
  var obj = this.db.data[key];
  if (obj && 'number' == typeof obj.expires && Date.now() > obj.expires) {
    delete this.db.data[key];
    return;
  }
  return obj;
};

DataSync.prototype.getConfig = function (key) {
  if (!this.config) {
    var data = fs.readFileSync(this.iniFile, 'utf8');
    if (data) {
      this.config = JSON.parse();
    }
  }
  return this.config[key] || null;
};

DataSync.prototype.setConfig = function (key, value) {
  if (!this.config) this.config = {};
  this.config[key] = value;
  //fs.writeFileSync(this.iniFile, JSON.stringify(this.config));
  fs.writeFile(this.iniFile, JSON.stringify(this.config), {flag:''}, function (err) {
    if(err) throw err;
  });
};

DataSync.prototype.recoverDefault = function () {
  //console.log(this);
  var self = this;
  console.log(self.filename);
  console.log(self.config.PrevChipSplit);
  console.log(self.config.CurChipSplit);
  self.recover(self.filename, self.config.PrevChipSplit, self.config.CurChipSplit);
  //self.recover(self.filename, self.config.PrevChipSplit);
};

DataSync.prototype.recover = function (fileName, start, end) {
  var self = this;
  if (!self.recoverOn) return;
  //console.log('fileName = ' + fileName);
  //console.log('start = ' + start);
  //console.log('end = ' + end);
  if (typeof fileName != 'string' ||
      (!!start && typeof start != 'string')  ||
      (!!end && typeof end != 'string') ) {
    throw new Error('recover failed, param type err');
    return;
  }
  var strStartSplit = start || '';  //起始分隔符
  var strEndSplit = end || '';      //结束分隔符
  //var rStream = fs.createReadStream(fileName, {encoding: 'utf8'});
  var strBuff = '';
  var isFindStart = false;
  var isFindEnd = false;
  var fd = fs.openSync(fileName, 'r');
  if (!fd) throw new Error('cant open file:' + fileName);

  if (strStartSplit === '') isFindStart = true;   // 如果没有设定起始位置，则从文件最开始读入
  if (strEndSplit === '') isFindEnd = true;
  var chunk = new Buffer(chip_size);
  chunk.fill(0);
  var offset = 0;
  var data_size = 0;
  while ( (size = fs.readSync(fd, chunk, 0, chip_size, offset)) > 0) {
    strBuff += chunk.toString('UTF-8', 0, data_size);
    //console.log(size);
    offset += chip_size;
    //chunk.fill(0);
    //console.log(strBuff);
    // 还未找到
    if (!isFindStart) {
      var i = strBuff.search(strStartSplit);
      if (i !== -1) {
        //console.log(strBuff);
        //console.log('isFindStart = true i =' +i);
        isFindStart = true;
        strBuff = strBuff.substr(i + strStartSplit.length + 2);
        //console.log(strBuff);
        //console.log('isFindStart = true end');
        //console.log('find it at idx =' + i);
        //console.log(chunk);
        //return;
      }
      else {
        //删除无用块的缓存
        if (strBuff.length > chip_size*2) {
          //console.log('release strBuff');
          strBuff = strBuff.substr(chip_size);
          //console.log(strBuff);
          //console.log('release strBuff end');
        }
        continue;
      }
    }
    // 未找到结束 执行查找
    if (!isFindEnd) {
      var j = strBuff.search(strEndSplit);
      if (j !== -1) {
        //console.log('isFindEnd = true i =' +j);
        isFindEnd = true;
        strBuff = strBuff.substr(0, j);
        //console.log(strBuff);
        break;
      }
    }
  }

  fs.close(fd);
  //console.log('start split = ' + strStartSplit);
  //console.log('end split = ' + strEndSplit);
  //console.log('this is strBuff start:');
  //console.log(strBuff);
  //console.log('this is strBuff end;');
  if (strBuff.length === 0) {
    console.log('no recover data');
    self.timer.start(self);
    return;
  }

  var mergerMap = {};

  var log_array = strBuff.split('*');
  //console.log('log_array='+ JSON.stringify(log_array));
  var size = 0;
  for (var i in log_array) {
    var req_data_arr = log_array[i].split('\r\n');
    if (req_data_arr.length < 4) continue;
    //console.log('req_data_arr='+ JSON.stringify(req_data_arr));
    //var param_num = req_data_arr[0];
    var param_cmd = req_data_arr[1];                // 执行参数
    var param_id = parseInt(req_data_arr[2]);       // id
    var param_msg = JSON.parse(req_data_arr[3]);    // 更新内容

    var mergerKey = [param_cmd, param_id].join('');
    mergerMap[mergerKey] = {key: param_cmd, val: clone(param_msg)};
    ++size;
  }
  var sql = 0;
  async.forEachOf(mergerMap, function (value, key, callback) {
    var entry = value;
    self.execSync(entry.key, entry.val, callback);
    ++sql;
    //console.log('recover execSync:' + JSON.stringify(entry));
  }, function (err) {
    if (err) {
      throw new Error(err);
    }
    console.log('recover data count = ' + size + '('+sql+')');
    // TODO 恢复操作完成，后续操作
    self.timer.start(self);
  });
};

//stream流读写问题
DataSync.prototype.recover2 = function (fileName, start, end) {
  var self = this;
  if (!self.recoverOn) return;
  console.log('fileName = ' + fileName);
  console.log('start = ' + start);
  console.log('end = ' + end);
  if (typeof fileName != 'string' ||
      (!!start && typeof start != 'string')  ||
      (!!end && typeof end != 'string') ) {
    throw new Error('recover failed, param type err');
    return;
  }
  //var strArray = strSplit.split('_');
  ////__X_X_X__
  //if (strArray.length !== 7) {
  //  console.log('recover failed, param form err');
  //  return;
  //}
  //
  //var idx = parseInt(strArray[4]);
  //var time = strArray[5];
  var strStartSplit = start || '';  //起始分隔符
  var strEndSplit = end || '';      //结束分隔符
  var rStream = fs.createReadStream(fileName, {encoding: 'utf8'});
  var strBuff = '';
  var isFindStart = false;
  var isFindEnd = false;
  rStream.on('readable', function () {
    var chunk;
    if (strStartSplit === '') isFindStart = true;   // 如果没有设定起始位置，则从文件最开始读入
    if (strEndSplit === '') isFindEnd = true;
    var loops = 0;
    while (null !== (chunk = rStream.read(chip_size))) {
      console.log('loops ='+ ++loops);
      strBuff += chunk.toString();
      console.log(strBuff);
      // 还未找到
      if (!isFindStart) {
        var i = strBuff.search(strStartSplit);
        if (i !== -1) {
          console.log(strBuff);
          console.log('isFindStart = true i =' +i);
          isFindStart = true;
          strBuff = strBuff.substr(i + strStartSplit.length + 2);
          console.log(strBuff);
          console.log('isFindStart = true end');
          //console.log('find it at idx =' + i);
          //console.log(chunk);
          //return;
        }
        else {
          //删除无用块的缓存
          if (strBuff.length > chip_size*2) {
            console.log('release strBuff');
            strBuff = strBuff.substr(chip_size);
            console.log(strBuff);
            console.log('release strBuff end');
          }
          continue;
        }
      }
      // 未找到结束 执行查找
      if (!isFindEnd) {
        var j = strBuff.search(strEndSplit);
        if (i !== -1) {
          console.log('isFindEnd = true');
          isFindEnd = true;
          strBuff = strBuff.toString().substr(0, i);
          //console.log('find it at idx =' + i);
          //console.log(chunk);
          break;
        }
      }
      console.log('loop ' + loops + ' over');
    }
    console.log(chunk);


    console.log('split = ' + strStartSplit);
    console.log('this is strBuff start:');
    //console.log(strBuff);
    console.log('this is strBuff end;');
    if (strBuff.length === 0) {
      console.log('no recover data');
      self.timer.start(self);
      return;
    }

    var mergerMap = {};

    var log_array = strBuff.split('*');
    //console.log('log_array='+ JSON.stringify(log_array));
    for (var i in log_array) {
      var req_data_arr = log_array[i].split('\r\n');
      if (req_data_arr.length < 4) continue;
      //console.log('req_data_arr='+ JSON.stringify(req_data_arr));
      //var param_num = req_data_arr[0];
      var param_cmd = req_data_arr[1];                // 执行参数
      var param_id = parseInt(req_data_arr[2]);       // id
      var param_msg = JSON.parse(req_data_arr[3]);    // 更新内容

      var mergerKey = [param_cmd, param_id].join('');
      mergerMap[mergerKey] = {key: param_cmd, val: clone(param_msg)};
    }

    async.forEachOf(mergerMap, function (value, key, callback) {
      var entry = value;
      self.execSync(entry.key, entry.val, callback);
      //console.log('recover execSync:' + JSON.stringify(entry));
    }, function (err) {
      if (err) {
        throw new Error(err);
      }
      // TODO 恢复操作完成，后续操作
      self.timer.start(self);
    });


    //for (var mergerKey in mergerMap) {
    //  var entry = mergerMap[mergerKey];
    //  this.execSync(entry.key, entry.val, entry.cb);
    //}

  });
};

/**
 * Write the given `cmd`, and `args` to the AOF.
 *
 * @api private
 * @param {String} cmd
 * @param {Array} args
 */

DataSync.prototype.writeToAOF = function(cmd, args){
  var self = this;
  if (!self.aof) {return;}

  var argc = args.length;
  var op = '*' + (argc + 1) + '\r\n' + cmd + '\r\n';

  // Write head length
  this.stream.write(op);
  var i = 0;
  // Write Args
  for (i = 0; i < argc; ++i) {
    var key = utils.string(args[i]);
    this.stream.write(key);
    this.stream.write('\r\n');
  }
};

DataSync.prototype.writeSplitToAOF = function(split){
  var self = this;
  if (!self.aof) {return;}
  var split_line = split + '\r\n';
  this.stream.write(split_line);
};

