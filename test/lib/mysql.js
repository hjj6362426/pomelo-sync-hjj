var mysql = require('mysql');
var client = mysql.createConnection({
    "host" : "10.241.95.140",
    "port" : "3306",
    "database" : "xkfyz_hjj",
    "user" : "xk_user",
    "password" : "04ec9d350d49e19ad1bad9b70abd11a0e",
    supportBigNumbers: true			//增加长整型支持,否则bigint过长会出错
});
 
exports.client = client;
