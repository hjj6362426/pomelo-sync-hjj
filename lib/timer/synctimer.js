/**
 * Module dependencies.
 */

var SyncTimer = module.exports = function SyncTimer() {
    this.timer = null;
    this.server_id = null;
};

/**
 * start sync timer .
 */

SyncTimer.prototype.start = function(db){
	console.log(db.server_id, ' SyncTimer start!!');
    var self = this;
    self.server_id = db.server_id;
    if (self.timer) {
        return;
    }
    self.timer = setInterval(function(){
		//console.log('Background append only file rewriting started');
		db.sync();
	},db.interval);
};

SyncTimer.prototype.stop = function(){
    var self = this;
    console.log(self.server_id, 'SyncTimer stop!!');
    if (self.timer) {
        clearInterval(self.timer);   //关掉定时器
        console.log(self.server_id, 'SyncTimer stop done!!');
        self.timer = null;
    }
};

