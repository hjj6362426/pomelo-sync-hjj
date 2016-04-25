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
 };

var error_cb = function (err,res) {
	if (err) {

	}
}

/**
 * Initiate sync.
 */

 Rewriter.prototype.sync = function(/*isRecover*/){
	 //isRecover = isRecover || false;
 	var self = this,server = self.server;
 	server.flushQueue.shiftEach(function(element){
 		self.tick(element.key,element.val);	
 	});
 	var mergerMap = server.mergerMap;
	 var size = 0;
	 var needSplit = false;
 	for (var mergerKey in mergerMap) {
        var entry = mergerMap[mergerKey];
        self.tick(entry.key, entry.val, entry.cb);
        delete mergerMap[mergerKey];
        needSplit = true;
		++size;
    }

	 if (needSplit/* && !isRecover*/) {
		 var prevSplit = server.getConfig('CurChipSplit');
		 server.setConfig('PrevChipSplit', prevSplit);
		 //++server.savebeat;
		 var time = moment().format('YYYYMMDDHHmmss');
		 var curSplit = '__SPLIT_' + time + '__';
		 server.setConfig('CurChipSplit', curSplit);
		 server.writeSplitToAOF(curSplit);
		 console.log('this chip sql count = ' + size);
	 }
 	return true;
 };

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
 		server.log.error('db sync client is null');
 		return ;
 	}
 	var syncb = server.mapping[key];
 	if (!syncb) {
 		server.log.error(key + ' callback function not exist ');
 		return;
 	}
 	if (!cb) {
 		self.count+=1;
 		return invoke(syncb,server.client,val,function(err,res){
			self.count-=1;
			if (err) {
				self.error.push[self];
			}
		});
 	} else {
 		invoke(syncb,server.client,val,cb);
 	}
 };
/*
 *
 * judge task is done
 *
 */
 Rewriter.prototype.isDone = function() {
 	return this.count===0;
 };
