var DataSync = require('../');

var dbclient = require('./lib/mysql').client;

var opt = {};
var mappingPath = __dirname+ '/mapping';
opt.client = dbclient;
opt.interval = 1000 * 10;
opt.aof = true;
opt.recoverOn = true;
var sync = new DataSync(opt);
console.log('before loading ')
sync.mapping = sync.loadMapping(mappingPath);

console.log(sync.mapping);

//var key = 'user_key';
//var User = function User(id, name){
//	this.name = name;
//	this.x = parseInt(Math.random()*1000);
//	this.y = parseInt(Math.random()*1000);
//	this.sceneId = 1;
//};

//for (var i = 1; i < 20000; ++i) {
//	var user1 = new User(i, 'hello'+i);
//	sync.exec('player.createUser', i, user1);
//}
//
//return;

sync.recoverDefault();
//var iid = 0;
//setInterval(function(){
//	var item = {};
//	item.id = ++iid;
//	item.cfg_id = parseInt(Math.random()*1000);
//	item.role_id = parseInt(Math.random()*20000);
//	item.num = parseInt(Math.random()*100);
//	item.type = item.cfg_id%10;
//	sync.exec('bag.createItem',item.id,item);
//},1);
//
//setInterval(function(){
//	var item = {};
//	item.id = ++iid;
//	item.cfg_id = parseInt(Math.random()*1000);
//	item.role_id = parseInt(Math.random()*20000);
//	item.num = parseInt(Math.random()*100);
//	item.type = item.cfg_id%10;
//	sync.exec('bag.createItem',item.id,item);
//},1);

setInterval(function(){
	var User = function User(id, name){
		this.name = name;
		this.x = 2;
		this.y = 2;
		this.sceneId = 1;
		this.uid = id;
	};
	var idx = parseInt(Math.random()*8000);
	var user1 = new User(idx, 'hello'+idx);
	sync.exec('player.updateUser',idx,user1);
},1);

setInterval(function(){
	var User = function User(id, name){
		this.name = name;
		this.x = 2;
		this.y = 2;
		this.sceneId = 1;
		this.uid = id;
	};
	var idx = parseInt(Math.random()*8000);
	var user1 = new User(idx, 'hello'+idx);
	sync.exec('player.updateUser',idx,user1);
},1);

setInterval(function(){
	var User = function User(id, name){
		this.name = name;
		this.x = 3;
		this.y = 3;
		this.sceneId = 1;
		this.uid = id;
	};
	var idx = parseInt(Math.random()*8000);
	var user1 = new User(idx, 'hello'+idx);
	sync.exec('player.updateUser',idx,user1);
},1);

setInterval(function(){
	var User = function User(id, name){
		this.name = name;
		this.x = 2;
		this.y = 2;
		this.sceneId = 1;
		this.uid = id;
	};
	var idx = parseInt(Math.random()*8000);
	var user1 = new User(idx, 'hello'+idx);
	sync.exec('player.updateUser',idx,user1);
},1);

setInterval(function(){
	var User = function User(id, name){
		this.name = name;
		this.x = 4;
		this.y = 4;
		this.sceneId = 1;
		this.uid = id;
	};
	var idx = parseInt(Math.random()*8000);
	var user1 = new User(idx, 'hello'+idx);
	sync.exec('player.updateUser',idx,user1);
},1);

setInterval(function(){
	console.log(' count:' + sync.rewriter.count + ' isDone: ' + sync.isDone());
},2500);

return;

var user1 = new User('hello');
user1.x = user1.y = 999;
user1.uid = 10003;
user1.sceneId = 1;
//var resp = sync.set(key,user1);
//
//console.log('resp %j' , sync.get(key));

//sync.execSync('bag.selectUser',10004,function(err,data){
//	console.log(err + '  select data ' + data);
//});

user1.x = 888;
user1.y = 777;

console.log(' count ' + sync.rewriter.count);

sync.exec('player.updateUser',10003,user1);

user1.x = 999;

sync.flush('player.updateUser',10003,user1);
 
setInterval(function(){
 console.log(' count:' + sync.rewriter.count + ' isDone: ' + sync.isDone());
},1000);
