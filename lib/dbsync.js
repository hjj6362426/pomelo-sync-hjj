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

var clearStreamFile = function (DateSync) {
    DateSync.stream.close();

};

var defaultLogger = function() {
    return {
        debug: console.log,
        info: console.log,
        warn: console.warn,
        error: console.error,
    }
};

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
    var self = this;
	options = options || {};
	this.dbs = [];
	this.selectDB(0);
    this.server_id = options.server_id;
	this.client = options.client;
	this.aof = options.aof || true;
	this.debug = options.debug || false;
	this.log = options.log || console;
	this.interval = options.interval || 1000 * 60;
	this.flushQueue =  new Queue();
    this.useSafeMode = options.useSafeMode || true;
    this.safeCallback = options.safeCallback || null;
    this.aliveIndex = 0;
    this.mapArray = [{},{}];
	this.mergerMap = this.mapArray[this.aliveIndex];
    this.saveMap = this.mapArray[1 - this.aliveIndex];
    this.config = {};
    this.basePath = process.cwd() + '/recover';
    this.perStepSaveCount = options.perCount || 20;
    this.backup_counts = parseInt(options.backup_counts) || 7;
    this.writeSplit = null;     // 需要写入的split
    this.logger = options.logger || defaultLogger();
    this.use_strict = options.use_strict;
	if (!!this.useSafeMode) {
        this.logger.info(this.server_id, ' safeMode open');

        if (!fs.existsSync(this.basePath)) {
            fs.mkdirSync(this.basePath);
        }
        var config_path = this.basePath + '/ini';
        if (!fs.existsSync(config_path)) {
            fs.mkdirSync(config_path);
        }
        var error_path = this.basePath + '/error';
        if (!fs.existsSync(error_path)) {
            fs.mkdirSync(error_path);
        }
        this.iniFilePath = config_path + '/ini-'+ this.server_id +'.json'; // 配置文件
        this.errorFilePath = error_path+'/error-'+ this.server_id +'.log';
        this.fileName = 'dbsync-'+ this.server_id + '.log';
        this.fileNamePath = this.basePath + '/' + this.fileName;
        // 读取配置
        if (fs.existsSync(this.iniFilePath)) {
            var data = fs.readFileSync(this.iniFilePath, 'utf8');
            if (data) {
                this.config = utils.parse(data);
            }
        }
        //if (!this.config.recoverFile) {
        //    var time = moment().format('YYYYMMDDHHmmss');
        //    if (fs.existsSync(this.fileNamePath)) {
        //
        //        var new_name = this.fileName + '.' + time;
        //        var new_path = this.basePath + '/' + new_name;
        //        try {
        //            fs.renameSync(this.fileNamePath, new_path);
        //            self.setConfig('recoverFile', new_name);
        //        }
        //        catch(ex){
        //            throw ex;
        //        }
        //    }
        //    this.stream = fs.createWriteStream(this.fileNamePath, { flags: 'a' });
        //    var StartSplit = '__SPLIT_' + time + '__';
        //    self.setConfig('TempSplit', StartSplit);
        //    self.writeSplitToAOF(StartSplit);             // 初始化 写入文件
        //}
        //else {
        //    this.stream = fs.createWriteStream(this.fileNamePath, { flags: 'a' });
        //}

        this.error_stream = fs.createWriteStream(this.errorFilePath, { flags: 'a' });

        this.chip_size = options.chip_size || 16 * 1024;
	}
    if (!!options.mapping){
        this.mapping = options.mapping;
    } else if (!!options.mappingPath) {
        this.mapping = this.loadMapping(options.mappingPath);
    }
    this.rewriter = options.rewriter || new Rewriter(this);
    this.timer = /*options.timer || */new SyncTimer();
    //if (!this.useSafeMode){
    //    self.dbSyncStart();
    //}
    //this.count = 0;
    //this.isRecoverWork = false;

    //console.log('config =' + JSON.stringify(this.config));
};

/**
 * Expose commands to store.
 */
DataSync.prototype = commands;

/**
 * swap mergerMap and saveMap.
 */
DataSync.prototype.swapMap = function () {
    var self = this;
    self.aliveIndex = (++self.aliveIndex) % 2;
    self.mergerMap = self.mapArray[self.aliveIndex];
    self.saveMap = self.mapArray[1 - self.aliveIndex];
    //console.log('swapMap: merge idx = ' + self.aliveIndex);
};

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

// 取配置
DataSync.prototype.getConfig = function (key) {
    var self = this;
    if (!self.config) {
        //var data = fs.readFileSync(this.inifilename, 'utf8');
        //if (data) {
        //  this.config = JSON.parse(data);
        //}
        self.config = {};
    }
    return self.config[key] || null;
};

// 写配置
DataSync.prototype.setConfig = function (key, value) {
    var self = this;
    if (!self.config) self.config = {};
    self.config[key] = value;
    if (self.useSafeMode){
        fs.writeFileSync(self.iniFilePath, JSON.stringify(self.config),{ flags: 'w' });
    }
    //var ini_stream = fs.createWriteStream(this.inifilename, { flags: 'w' });
    //ini_stream.write(utils.string(this.config));
    //ini_stream.end();
    //ini_stream.close();
    //fs.writeFile(this.inifilename, utils.string(this.config), {flag:'w'}, function (err) {
    //  if(err) throw err;
    //});
};

// 启动
DataSync.prototype.start = function (cb) {
    var self = this;
    // 先恢复
    async.waterfall([
        function (fn) {
            // 执行恢复
            self.recoverDefault(fn);
        }
    ], function (err) {
        if (err) {
            cb(err);
            return;
        }
        self.dbSyncStart();
        cb(null);
    });
};

DataSync.prototype.startTimer = function () {
    var self = this;
    self.rewriter.isStoped = false;
    self.timer.start(self);

};

// 一旦启动 默认日志数据已全更新到db， 清除标志
DataSync.prototype.dbSyncStart = function () {
    var self = this;
    // 重命名文件
    //var time = self.getConfig('file_time');
    //if (time) {
    //    //// 文件重命名
    //    //if (fs.existsSync(self.fileNamePath)) {
    //    // 将当前文件重命名
    //    var new_name = self.fileName + '.' + time;
    //    var new_path = self.basePath + '/' + new_name;
    //    try {
    //        // 备份文件
    //        fs.renameSync(self.fileNamePath, new_path);
    //    }
    //    catch(ex){
    //        throw ex;
    //    }
    //}
    if (fs.existsSync(self.fileNamePath)) {
        var statInfo = fs.statSync(self.fileNamePath);
        if (statInfo.size > 0) {
            // 将备份文件依次推后覆盖
            for (var i = self.backup_counts - 1; i > 0; i--) {
                var from_name = self.fileName + '.' + i;
                var from_path = self.basePath + '/' + from_name;
                if (fs.existsSync(from_path)) {
                    var to_name = self.fileName + '.' + (i+1);
                    var to_path = self.basePath + '/' + to_name;
                    fs.renameSync(from_path, to_path);
                }
            }
            // 备份当前文件、当前文件更名为.1
            var new_name = self.fileName + '.' + 1;
            var new_path = self.basePath + '/' + new_name;
            // 备份文件
            fs.renameSync(self.fileNamePath, new_path);
        }
    }
    // 初始化 队列数据持久化文件写入流
    self.stream = fs.createWriteStream(self.fileNamePath, { flags: 'a' });

    var current_time = moment().format('YYYY-MM-DD-HH-mm-ss');   // 当前时间戳
    var current_split = '__SPLIT_' + current_time + '__';        // 当前分隔符

    self.setConfig('file_time', current_time);      // 用于重命名文件
    self.setConfig('recover_split', current_split);     // 恢复用split
    self.setConfig('write_file_split', current_split);  // 当前写文件的split
    self.writeSplit = current_split;                // 缓存分割符
    self.timer.start(self);
};

// 默认恢复策略
DataSync.prototype.recoverDefault = function (cb) {
    var self = this;
    var start = self.getConfig('recover_split');
    if (!start) {
        cb(null);
        return;
    }
    // 从文件恢复
    self.recover(self.fileNamePath, start, null, cb);
};

// 执行恢复
// filename : 恢复数据的来源文件
// start: 起始位置 （null or ‘’ ＝ 文件起始位置）
// end: 结束位置（null or ‘’ ＝ 文件结束位置）
DataSync.prototype.recover = function (fileName, start, end, cb) {
    var self = this;
    if (!self.useSafeMode) {
        cb(null);
        return;
    }
    //console.log('fileName = ' + fileName);
    //console.log('start = ' + start);
    //console.log('end = ' + end);
    ////没有配置项则不执行恢复
    //if (!start && !end) {
    //  console.log('no data recover');
    //  self.timer.start(self);
    //  return;
    //}

    if (!!fileName && typeof fileName != 'string' ||
        (!!start && typeof start != 'string')  ||
        (!!end && typeof end != 'string') ) {
        cb('recover failed, param type err');
        return;
    }

    var strStartSplit = start || '';  //起始分隔符
    var strEndSplit = end || '';      //结束分隔符
    var strBuff = '';
    var isFindStart = false;
    var isFindEnd = false;
    var fd = fs.openSync(fileName, 'r');
    if (!fd) {
        cb('cant open file:' + fileName);
        return;
    }

    // 如果设定起始位置为''，则从文件最开始读入
    if (strStartSplit === '') {
        //console.log(self.server_id + ' recover will start at file start position');
        isFindStart = true;
    }
    // 如果设定结束位置为''，则读到文件结束
    if (strEndSplit === '') {
        //console.log(self.server_id + ' recover will end at file end position');
        isFindEnd = true;
    }
    var chunk = new Buffer(chip_size);
    chunk.fill(0);
    var offset = 0;
    var data_size = 0;
    while ( (data_size = fs.readSync(fd, chunk, 0, chip_size, offset)) > 0) {
        strBuff += chunk.toString('UTF-8', 0, data_size);
        //console.log(data_size);
        offset += chip_size;
        //chunk.fill(0);
        //console.log(strBuff);
        // 未确定起始位置
        if (!isFindStart) {
            var i = strBuff.search(strStartSplit);
            if (i !== -1) {
                //console.log(strBuff);
                self.logger.info(self.server_id + ' find start split, idx =' + (offset+i));
                isFindStart = true;
                strBuff = strBuff.substr(i + strStartSplit.length + 2); // 过滤分隔符后紧随的\r\n
                //console.log(strBuff);
                //console.log('isFindStart = true end');
                //console.log('find it at idx =' + i);
                //console.log(chunk);
                //return;
            }
            else {
                //删除无用块的缓存 拼接可能被分割的分隔符（必须满足 chip_size > split_size）
                if (strBuff.length > chip_size*2) {
                    //console.log('release strBuff');
                    strBuff = strBuff.substr(chip_size);
                    //console.log(strBuff);
                    //console.log('release strBuff end');
                }
                continue;
            }
        }
        // 未确定结束位置
        if (!isFindEnd) {
            var j = strBuff.search(strEndSplit);
            if (j !== -1) {
                self.logger.info(self.server_id + ' find end split, idx =' + (offset+j));
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
    if (!isFindStart || strBuff.length === 0) {
        //console.log(self.server_id + ' no data to recover, ', ' from ', strStartSplit || 'null', ' to ', strEndSplit || 'null' );
        self.logger.info(self.server_id + ' no data to recover');
        cb(null);
        return;
    }
    else {
        self.logger.info(self.server_id + ' recover data length = ' + strBuff.length);
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
        var param_cmd = req_data_arr[1];                    // 执行参数
        var param_id = req_data_arr[2];                     // id
        var param_msg = utils.parse(req_data_arr[3]);       // 更新内容

        if (param_cmd.indexOf('Sync.add') >= 0) {
            param_cmd = param_cmd.replace('Sync.add', 'Sync.replace');
        }
        if (param_cmd.indexOf('Sync.update') >= 0) {
            param_cmd = param_cmd.replace('Sync.update', 'Sync.replace');
        }
        var mergerKey = [param_cmd, param_id].join('');
        mergerMap[mergerKey] = {key: param_cmd, val: clone(param_msg)};
        //var isDone = false;
        //if (mergerKey.indexOf('Sync.add') >= 0) {
        //    var searchKey = mergerKey.replace('Sync.add', 'Sync.replace');
        //    mergerMap[searchKey] = {key: param_cmd, val: clone(param_msg)};
        //    isDone = true;
        //    console.log('switch add to replace');
        //}
        //else if (mergerKey.indexOf('Sync.update') >= 0) {
        //    var searchKey = mergerKey.replace('Sync.update', 'Sync.replace');
        //    if (!!mergerMap[searchKey]) {
        //        mergerMap[searchKey].val = clone(param_msg);
        //        isDone = true;
        //        console.log('switch update to replace');
        //    }
        //}
        //if (!isDone) {
        //
        //}

        ++size;
    }

    var watchtimer = setInterval(function(){
        self.logger.info(self.server_id + ' recover lest count:' + self.rewriter.count + ' isDone: ' + self.isDone());
    },2500);

    var sql_count = 0;
    async.forEachOf(mergerMap, function (value, key, callback) {
        var entry = value;
        self.execSync(entry.key, entry.val, callback);
        ++sql_count;
        //console.log('recover execSync:' + JSON.stringify(entry));
    }, function (err, res) {
        if (err) {
            self.logger.info(err);
            self.safeNotify({code:3, message:'recover error', error:err});
            clearInterval(watchtimer);
            cb ('recover error : ' + JSON.stringify(err));
            return;
            //throw new Error(err);
        }
        clearInterval(watchtimer);
        self.safeNotify({code:0, message:'recover sucess, count = ' + size + '('+sql_count+')'});
        self.logger.info(self.server_id + ' recover sucess, count = ' + size + '('+sql_count+')');
        self.rewriter.error = [];   //清除recover阶段产生的但是被忽略的错误信息
        //// TODO 恢复操作完成，后续操作
        //self.dbSyncStart();
        cb (null);
    });
};

DataSync.prototype.isStoped = function () {
    var self = this;
    return self.rewriter.isStoped;
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
    if (!self.useSafeMode) {return;}
    // 存在未写入的分割符 先写入分隔符
    if (self.writeSplit) {
        self.writeSplitToAOF(self.writeSplit);
        // 写入一次后清空，待下一次分隔符生成时再写入
        self.writeSplit = null;
    }

    var argc = args.length;
    var op = '*' + (argc + 1) + '\r\n' + cmd + '\r\n';

    // Write head length
    self.stream.write(op);
    var i = 0;
    // Write Args
    for (i = 0; i < argc; ++i) {
        //console.log(args[i]);
        var key = utils.string(args[i]);
        //console.log('key = ' + key);
        self.stream.write(key);
        self.stream.write('\r\n');
    }
};

DataSync.prototype.writeSplitToAOF = function(split){
    var self = this;
    if (!self.useSafeMode) {return;}
    var split_line = split + '\r\n';
    self.stream.write(split_line);
};

DataSync.prototype.writeError = function(err){
    var self = this;
    if (!self.useSafeMode) {return;}

    var err_data = utils.string(err);
    var recover_split = self.getConfig('recover_split');
    var data = '*' + recover_split + '\r\n' + err_data + '\r\n';
    self.error_stream.write(data);
};

DataSync.prototype.safeNotify = function(msg){
  var self = this;

  if (self.safeCallback) {
      self.safeCallback(msg);
  }
};
