var fs = require('fs');

var path = process.cwd() + '/logs';
//fs.mkdirSync(path);
var filename = path+'/test.log';
var filename2 = path+'/test2.log';
//var fs

//this.recoverFile = path + '/recover';
//var ini = {
//    a: 1,
//    b: 2,
//    c: 3
//};
//var isOk = fs.writeFileSync(this.recoverFile, JSON.stringify(ini));
//var iniData = fs.readFileSync(this.recoverFile, 'utf8');
//var ini2 = JSON.parse(iniData);
//console.log(ini2.a);
//
//return;

//var iniData = fs.readFileSync(filename, 'utf8');
//console.log(iniData);

var fd = fs.openSync(filename, 'r');
if (!fd) throw new Error('cant open file:' + fileName);

var temp = '';
//var buf;
var idx = 0;
var buf = new Buffer(20);
//var i = fs.readSync(fd, buf, 0, 20, idx);
//console.log(i);
//console.log(buf.toString());
//return;
while(fs.readSync(fd, buf, 0, 20, idx) > 0) {
    idx += 20;
    temp += buf.toString();
    console.log(buf.toString());
    buf.fill(0);
    console.log('----------');
}
console.log(temp);
return;
//var wStream = fs.createWriteStream(filename2,{ flags: 'w' });
var rStream = fs.createReadStream(filename,{encoding: 'utf8'});
//var buf = new Buffer(4096);
//rStream.pipe(wStream);
var index = 0;
//rStream.on('readable', function () {
    var chunk;
    var idx = 0;

    var data;
    var isFind = false;
    while (null !== (chunk = rStream.read(20))) {
        if (!isFind) {
            var i = chunk.search('998');
            if (i !== -1) {
                isFind = true;
                console.log('find it at idx ='+ i);
                //console.log(chunk);
                return;
            }
        }
        console.log(chunk.toString());
        console.log('while-------'+ ++idx + '-------');
        //console.log(chunk);
    }
    console.log('on-------'+ ++index + '-------');
//});


//console.log(buf.toString('uft8'));


//for (var i = 0; i< 100; ++i) {
//    wStream.write('this is write this is write this is write ==' + i + '\r\n', {});
//}


//rStream.on('data', function (chunk) {
//    console.log('---------------------------');
//    console.log(chunk);
//});
