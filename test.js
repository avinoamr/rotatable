var fs = require( "fs" );
var assert = require( "assert" );
var rotatable = require( "./index" );

describe( "functional test", function () {
    var results = { written: 0, rotates: [] }

    var data = [
        { i: 1 }, { i: 2 }, { i: 3 }, 
        { i: 4 }, { i: 5 }, { i: 6 }, 
    ];

    it( "rotates at ~100B", function( ){
        results.files.forEach( function ( f ) {
            assert( f.stats.size < 105, "file size is smaller than 105 bytes" );

            if ( f.path != "test.log" ) {
                assert( f.stats.size > 100, "rotated file size is greater than 100 bytes" );
            }
        })
    });

    it( "contains all of the data", function () {
        var out = results.files.map( function ( f ) {
            return f.body
        }).join( "\n" );

        var objs = out.split( "\n" )
            .filter( function ( line ) {
                return !!line;
            })
            .map( JSON.parse );

        data.forEach( function ( d ) {
            var w1found = objs.filter( function ( obj ) {
                return obj.w == "w1" && obj.i == d.i;
            })

            var w2found = objs.filter( function ( obj ) {
                return obj.w == "w1" && obj.i == d.i;
            });

            console.log( d.i, w1found );
            assert.equal( w1found.length, 1 );
            assert.equal( w2found.length, 1 );
        })
    });

    it( "emits rotate events", function () {
        assert.equal( results.rotates.length, 1 );

        var file = results.rotates[ 0 ];
        assert.equal( file.indexOf( "test.log" ), 0 )
    })


    before( function ( done ) {
        cleanup();

        // create two parallel writers
        var w1 = rotatable( "test.log", { size: 100, suffix: ".1" } )
            .on( "finish", finish )
            .on( "rotate", function ( file ) {
                results.rotates.push( file )
            });
        var w2 = rotatable( "test.log", { size: 100, suffix: ".2" } )
            .on( "finish", finish )
            .on( "rotate", function ( file ) {
                results.rotates.push( file )
            });

        next();
        function next( err ) {
            var d = data.pop();
            if ( !d ) {
                w1.end();
                w2.end();
                return;
            }

            var saved = 0;
            var body;
            d.w = "w1";
            body = JSON.stringify( d ) + "\n";
            results.written += body.length;
            w1.write( body, function () {
                if ( ++saved == 2 ) {
                    next();
                }
            });

            d.w = "w2";
            body = JSON.stringify( d ) + "\n";
            results.written += body.length;
            w2.write( body, function () {
                if ( ++saved == 2 ) {
                    next();
                }
            });
        }

        var finished = 0;
        function finish() {
            if ( ++finished == 2 ) {
                results.files = readFiles();
                done();
            }
        }

    })

    after( cleanup );
});

function cleanup() {
    fs.readdirSync( "." )
        .filter( function ( path ) {
            return path.indexOf( "test.log" ) == 0;
        })
        .forEach( function ( path ) {
            fs.unlinkSync( path );
        })
}

function readFiles() {
    return fs.readdirSync( "." )
        .filter( function ( path ) {
            return path.indexOf( "test.log" ) == 0;
        })
        .reduce( function ( files, path ) {
            files.push({
                path: path,
                body: fs.readFileSync( path ).toString(),
                stats: fs.statSync( path )
            })
            return files;
        }, [] )
}


// var f = rotatable( "test.log", { flags: "a" } )
//     .on( "error", function ( err ) {
//         console.error( err )
//     })
//     .on( "open", function () {
//         console.log( "REOPEN" );
//     })
//     .on( "rotate", function ( path ) {
//         console.log( "ROTATED", path )
//     })
// w();

// function w() {
//     process.stdout.write( "." );
//     f.write( "Hello World\n", function ( err ) {
//         if ( err ) {
//             console.error( err );
//         } else {
//             setTimeout( w, 30 );
//         }
//     })
// }