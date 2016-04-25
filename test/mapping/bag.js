module.exports = item = {};

item.createItem=function(client,val,cb){
	var sql = 'INSERT INTO Item (cfg_id, role_id, num,type) VALUES(?,?,?,?);';
	var args = [val.cfg_id, val.role_id, val.num, val.type];
	client.query(sql, args, function(err, res){
		if(err !== null){
			console.error('write mysql failed!　' + sql + ' ' + JSON.stringify(val));
		} else {
			//console.info('write mysql success! flash dbok ' + sql + ' ' + JSON.stringify(val));
			cb();
		}
	});
};

item.updateItem=function(client,val,cb){
	//console.error(' updatewrite ' + JSON.stringify(val)+ '  ' + val.x+ '  ' +val.y + '  ' +val.uid);
	var sql = 'update Item set num = ? ,type = ? where id = ?';
	var args = [val.num, val.type, val.id];
	client.query(sql, args, function(err, res){
		if(err !== null){
			console.error('write mysql failed!　' + sql + ' ' + JSON.stringify(val));
		} else {
			//console.info('write mysql success! flash dbok ' + sql + ' ' + JSON.stringify(val));
			cb();
		}
	});
};

