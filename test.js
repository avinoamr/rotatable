var assert = require( "assert" );
var stream = require( "stream" );
var rotatable = require( "./index" );

describe( "Rotatable", function () {

    it( "adds the .rotate() method to streams", function () {
        var rotated = rotatable( new stream.Readable() );
        assert.equal( typeof rotated.rotate, "function" );
    });

    it( "supports the rotate function", function ( done ) {
        var results = {};
        rotatable( from( [ 1, 2, 3, 4, 5 ] ) )
            .rotate( function ( data ) {
                return data == 3;
            })
            .pipe( function ( count, size ) {
                results[ "s" + count ] = [];
                return new stream.PassThrough({ objectMode: true })
                    .on( "data", function ( d ) {
                        results[ "s" + count ].push( d )
                    })
            })
            .on( "end", function () {
                assert.deepEqual( results.s0, [ 1, 2 ] )
                assert.deepEqual( results.s1, [ 3, 4, 5 ] )
                done();
            })
    });

    it( "supports the rotate options", function ( done ) {
        var results = {};
        rotatable( from( [ 1, 2, 3, 4, 5 ] ) )
            .rotate({ size: 2 })
            .pipe( function ( count ) {
                results[ "s" + count ] = [];
                return new stream.PassThrough({ objectMode: true })
                    .on( "data", function ( d ) {
                        results[ "s" + count ].push( d )
                    })
            })
            .on( "end", function () {
                assert.deepEqual( results.s0, [ 1, 2 ] )
                assert.deepEqual( results.s1, [ 3, 4 ] )
                assert.deepEqual( results.s2, [ 5 ] )
                done();
            })
    });

    it( "defaults to a PassThrough stream", function ( done ) {
        createReadStream( "hello" )
            .pipe( rotatable() )
            .rotate( function () {
                return false
            })
            .pipe( function ( count ) {
                return createWriteStream()
                    .on( "finish", function () {
                        assert.equal( count, 0 );
                        assert.equal( this.val(), "hello" );
                        done();
                    })
            })
    });

    it( "passes options object into the PassThrough stream", function ( done ) {
        var results = [];
        from( [ 1, 2, 3, 4, 5 ] )
            .pipe( rotatable( { objectMode: true } ) )
            .rotate( function () {
                return false
            })
            .pipe( function ( count ) {
                return new stream.PassThrough({ objectMode: true })
                    .on( "data", function ( d ) {
                        results.push( d )
                    })
                    .on( "finish", function () {
                        assert.equal( count, 0 );
                        assert.deepEqual( results, [ 1, 2, 3, 4, 5 ] );
                        done();
                    })
            })
    });

    it( "throws an error when non-readable stream is forked", function () {
        assert.throws( function () {
            rotatable( new stream.Writable () );
        }, /non-readable/i )
    });

    it( "emits an error when no pipe function is defined", function ( done ) {
        rotatable( from( [ 1, 2, 3, 4, 5 ] ) )
            .rotate( function () {
                return true
            })
            .on( "error", function ( err ) {
                assert.equal( err.message, "No pipe function was defined" );
                done();
            })
            .on( "data", function () {});
    });

    it( "throws an error when the pipe isn't a writable stream", function () {
        var rotated = rotatable( from( [ 1, 2, 3, 4, 5 ] ) )
            .rotate( function () {
                return true
            })

        assert.throws( function () {
            rotated.pipe( {} );
        }, /must be a function/ )
    });

    it( "implements the README.md usage example", function ( done ) {
        var input = [
            "One",
            "Two",
            "Three",
            "Four",
            "Five"
        ].join( "\n" );
        var results = {};
        rotatable( createReadStream( input, { highWaterMark: 5 } ) )
            .rotate({ size: "10B" })
            .pipe( function ( count ) {
                return createWriteStream( "s" + count )
                    .on( "finish", function () {
                        results[ "s" + count ] = this.val();
                        if ( results.s0 && results.s1 && results.s2 ) {
                            complete();
                        }
                    })
            });

        function complete() {
            assert.equal( results.s0, "One\nTwo\nTh" );
            assert.equal( results.s1, "ree\nFour\nF" );
            assert.equal( results.s2, "ive" );
            done();
        }
    });


});

function from( data ) {
    var _stream = new stream.Readable({ objectMode: true });
    _stream._read = function () {
        this.push( data.shift() || null );
    }
    return _stream;
}

function createReadStream( text, options ) {
    var reader = new stream.Readable( options );
    reader._read = function ( size ) {
        this.push( text.substr( 0, size ) || null );
        text = text.substr( size );
    }
    return reader;
}

function createWriteStream() {
    var text = "";
    var writer = new stream.Writable();
    writer._write = function ( chunk, enc, done ) {
        text += chunk;
        done();
    }
    writer.val = function () { return text };
    return writer;
}
