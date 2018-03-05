/**
 * Module dependencies.
 */
 var moment = require('moment');
 var utils = require('../utils/utils');
 var invoke = utils.invoke;

/**
 * Initialize a new AOF Rewriter with the given `db`.
 * 
 * @param {options}
 * 
 */
 var Rewriter = module.exports = function Rewriter(server) {
 	this.server = server;
 	this.count = 0;
	this.error = [];
    this.doSaveFlag = false;
    this.isStoped = false;
 };

/**
 * Initiate sync.
 * 数据同步db
 */
Rewriter.prototype.sync = function(){
    var self = this,server = self.server;
    //console.log('start sync 111111111 at ', server.server_id );
    // 判断同步机制是否已暂停
    if (self.isStoped) {
        server.logger.info('sync break by stoped flag');
        return;
    }
    // 判断当前正在执行db同步
    if (!self.doSaveFlag) {
        // 判断最后一次db同步是否成功
        if(self.lastSaveSuccessful()) {
            self.doSaveFlag = true;
            // 交换存储和写入Map
            server.swapMap();
            // 执行存储（同步）
            self.startSave();
        }
    }
    else {
        server.logger.info('wait for prev chip save end');
    }
};

/**
 * 判断最后一次db同步是否成功
 */
Rewriter.prototype.lastSaveSuccessful = function () {
    var self = this,server = self.server;
    // 判断是否有错误发生
    if (self.error.length > 0) {
        server.logger.error('sync-safe error count = ' + self.error.length);
        // 记录错误到日志
        if (server.useSafeMode) {
            for (var i in self.error) {
                var err = self.error[i];
                server.writeError(err);
            }
        }
        // 抛出错误
        //server.safeNotify('error', {type:'error', error:self.error});
        server.safeNotify({code:4, message: 'sync save error', errors:self.error});
        self.error = [];
        server.logger.error('sync-safe stoped !!!!');
        // 关闭同步触发机制（删除定时器）
        server.timer.stop();   //定时器停止 todo
        // 设置db同步 停止标志
        self.isStoped = true;
        return false;
    }
    // 判断当前处理是否已经结束
    if (!self.isDone()) {
        //server.safeNotify('warn', {type:'delay', count:self.count});
        server.safeNotify({code:1, message: server.server_id + ': current chip was delayed, map size = ' + utils.getMapLength(self.server.saveMap) + ', wait cb count = '  + self.count + ')'});
        server.logger.warn(server.server_id ,': current chip was delayed, , map size = ', utils.getMapLength(self.server.saveMap), ', wait cb count = ', self.count, ')');
        return false;
    }

    return true;
};

/**
 * 开始同步db
 */
Rewriter.prototype.startSave = function () {
    var self = this,server = self.server;
    var size = 0;
    var needSave = false;
    var mergerMap = server.saveMap;
    // 遍历存储map
    for (var mergerKey in mergerMap) {
        needSave = true;
        ++size;
    }
    // 需要执行同步（数据存在）
    if (needSave) {
        // 生成新的写入split
        var current_time = moment().format('YYYY-MM-DD-HH-mm-ss');   // 当前时间戳
        var current_split = '__SPLIT_' + current_time + '__';        // 当前分隔符
        server.setConfig('write_file_split', current_split);  // 当前写文件的split
        // 缓存写入split(用于触发写入时写入)
        server.writeSplit = current_split;  // 待写入split
        server.logger.info(server.server_id ,': current chip sql count = ' + size);
        // 执行分段同步
        self.stepSave();
    }
    else {
        self.doSaveFlag = false;
    }
};

/**
 * 分段同步
 */
Rewriter.prototype.stepSave = function () {
    var self = this,server = self.server;
    var mergerMap = server.saveMap;
    var size = 0;
    // 遍历执行同步
    for (var mergerKey in mergerMap) {
        var entry = mergerMap[mergerKey];
        // 保护 跳过不处理null
        if (!entry) {
            continue;
        }
        self.tick(entry.key, entry.val, function (err, res) {
            // db同步操作的回调
            if (self.isDone()) {
                if (self.error.length === 0) {
                    // 如果本批次同步操作已全部执行成功
                    var current_split = server.getConfig('write_file_split');
                    // 更新恢复点的记录
                    server.setConfig('recover_split', current_split);     // 恢复用split
                    // 强制清除 存储map
                    server.saveMap = {};
                    // 标志同步结束
                    self.doSaveFlag = false;

                    server.logger.info(server.server_id ,': current chip save successful');
                }
                else {
                    // 强制清除 存储map
                    server.saveMap = {};
                    // 标志同步结束
                    self.doSaveFlag = false;
                    server.logger.info(server.server_id ,': current chip save failed');
                }

            }
            if (!!entry.cb && typeof entry.cb == "function") {
                entry.cb(err, res);
            }
        });
        // 保护可能删除不及时引起异常
        mergerMap[mergerKey] = null;
        delete mergerMap[mergerKey];

        // 限制每次同步数量
        ++size;
        if (size >= server.perStepSaveCount) {
            break;
        }
    }
    // 未完成
    if (size >= server.perStepSaveCount) {
        //console.log('stepSave Next, done = ' + size);
        // 下一循环继续同步
        setImmediate(function () {
            self.stepSave();
        });
    }
    // 已完成
    else {
        // 提示同步请求已全部发出（不代表成功）
        server.logger.info(server.server_id ,': current chip save finish (not mean successful)');
    }

};

 //Rewriter.prototype.sync = function(/*isRecover*/){
 //    //isRecover = isRecover || false;
 //    var self = this,server = self.server;
 //    var begin = Date.now();
 //    //console.log('sync begin time = ' + begin);
 //    //server.count = 0;
 //    if (self.error.length > 0) {
 //        console.error('sync-safe error count = ' + self.error.length);
 //        // 记录错误到日志
 //        if (server.useSafeMode) {
 //            for (var i in self.error) {
 //                var err = self.error[i];
 //                server.writeError(err);
 //            }
 //        }
 //        // 抛出错误
 //        //server.safeNotify('error', {type:'error', error:self.error});
 //        server.safeNotify({code:2, status:'normal', message:self.error});
 //        self.error = [];
 //        //server.timer.stop();   //定时器停止
 //        return false;
 //    }
 //    if (self.count !== 0) {
 //        //server.safeNotify('warn', {type:'delay', count:self.count});
 //        server.safeNotify({code:1, status:'normal', message:self.count});
 //        console.warn('sync-safe current chip was delayed, lest count = ' + this.count);
 //        return false;
 //    }
 //    else {
 //        var prevSplit = server.getConfig('PrevChipSplit');
 //        var curSplit = server.getConfig('CurChipSplit');
 //        if (prevSplit !== curSplit) {
 //            server.setConfig('PrevChipSplit', curSplit);
 //        }
 //    }
 //
 //    server.flushQueue.shiftEach(function(element){
 //        self.tick(element.key,element.val);
 //    });
 //    var mergerMap = server.mergerMap;
 //    var size = 0;
 //    var needSplit = false;
 //    for (var mergerKey in mergerMap) {
 //        var entry = mergerMap[mergerKey];
 //        self.tick(entry.key, entry.val, entry.cb);
 //        delete mergerMap[mergerKey];
 //
 //        needSplit = true;
 //        ++size;
 //    }
 //
 //    if (server.useSafeMode && needSplit /* && !isRecover*/) {
 //        //var prevSplit = server.getConfig('CurChipSplit');
 //        //server.setConfig('PrevChipSplit', prevSplit);
 //        //++server.savebeat;
 //        var time = moment().format('YYYYMMDDHHmmss');
 //        var curSplit = '__SPLIT_' + time + '__';
 //        server.setConfig('CurChipSplit', curSplit);
 //        server.writeSplitToAOF(curSplit);
 //        console.log('this chip sql count = ' + size);
 //    }
 //
 //    var end = Date.now();
 //    //console.log('sync end time = ' + end);
 //    //if (end - begin > 100) {
 //    //    console.log('sync doing time = ' + (end - begin)/1000);
 //    //}
 //    return true;
 //};

/*
 *
 * flush db
 *
 */
 Rewriter.prototype.flush = function(key, val, cb){
 	this.tick(key, val, cb);
 };
/*
 *
 * judge task is done
 *
 */
 Rewriter.prototype.tick = function(key,val,cb){
     var self = this,server = self.server;
     if (!server.client){
         server.logger.error('db sync client is null');
         return ;
     }
     var syncb = server.mapping[key];
     if (!syncb) {
         server.logger.error(key + ' callback function not exist ');
         return;
     }

     self.count+=1;
     invoke(syncb,server.client,val,function(err,res){
         self.count-=1;
         // 正常运行时不忽略插入主键冲突错误（防止代码BUG导致的主键冲突问题被忽略而造成数据丢失）
         if (err) {
             self.error.push(err);
         }
         if (!!cb && typeof cb == "function") {
             cb(err, res);
         }
     });

     //if (!cb) {
     //	self.count+=1;
     //	return invoke(syncb,server.client,val,function(err,res){
     //	self.count-=1;
     //	if (err) {
     //		self.error.push(err);
     //	}
     //});
     //} else {
     //self.count+=1;
     //	invoke(syncb,server.client,val, function (err, res) {
     //	self.count-=1;
     //	if (err) {
     //		self.error.push(err);
     //	}
     //	cb(err, res);
     //});
     //}
 };

/*
 *
 * judge task is done
 *
 */
 Rewriter.prototype.isDone = function() {
 	return (this.count===0 && utils.getMapLength(this.server.saveMap) === 0);
 };
