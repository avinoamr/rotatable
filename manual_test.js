var fs = require("fs");
// var assert = require("assert");
var {
    createRotatable
} = require("./index");
var zlib = require('zlib');


var data = [{
        i: 1
    }, {
        i: 2
    }, {
        i: 3
    },
    {
        i: 4
    }, {
        i: 5
    }, {
        i: 6
    },
];

var d = data.pop();
d.w = "w1";
body = JSON.stringify(d) + "\n";


var w1 = createRotatable("test.log", {
    size: 100,
    suffix: ".1"
})

w1.write(body);

// var inFile = fs.createReadStream('test.log');

// var s = zlib.gzipSync(inFile);