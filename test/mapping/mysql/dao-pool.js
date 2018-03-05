var mysql = require('mysql');
var crypto = require('crypto');

/*
 * Create mysql connection pool.
 */

var createMysqlPool = function(app) {
	//var mysqlConfig = app.get('mysql');

    var decode = function(encodeData) {
        var typeBit = encodeData[0];
        encodeData = encodeData.substr(1);
        var decipher = crypto.createDecipher('aes-256-cbc', 'aes-256-cfb');
        var dec = decipher.update(encodeData, 'hex', 'utf8');
        dec += decipher.final('utf8');

        switch (typeBit) {
            case '0':
            return dec;

            case '1':
            return Number(dec);

            case '2':
            return Boolean(dec);

            default:
            throw new Error('Decode data error.');
        }
    };

	return mysql.createPool({
        "host" : "10.241.95.140",
        "database" : "xkfyz_hjj",
        "user" : "xk_user",
        "password" : "04ec9d350d49e19ad1bad9b70abd11a0e",
        connectionLimit : 10,
        supportBigNumbers: true
    });
};

exports.createMysqlPool = createMysqlPool;
