var mysql = require('mysql');
var client = mysql.createConnection({
    host: '10.241.93.201',
    user: 'guoxiangyu',
    password: 'guoxiangyu',
    database: 'xkfyz_v01_1001',
    supportBigNumbers: true			//增加长整型支持,否则bigint过长会出错
});
 
exports.client = client;
