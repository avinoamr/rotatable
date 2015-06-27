var util = require( "util" );
var bytes = require( "bytes" );
var stream = require( "stream" );

module.exports = rotatable;
module.exports.RotateStream = RotateStream;

function rotatable( _stream ) {
    // default pass-through
    if ( arguments.length == 0 ) {
        _stream = {};
    }

    // pass the options argument into the PassThrough constructor, as-is
    if ( _stream.constructor == Object ) {
        
        // default high water mark of 1, for minimal memory overhead
        _stream.highWaterMark || ( _stream.highWaterMark = 1 );
        _stream = new stream.PassThrough( _stream );
    }

    if ( !_stream.pipe || !_stream.read ) {
        throw new Error( "Non-readable streams are not forkable" );
    }

    _stream.rotate = function ( rotatefn, options ) {
        options || ( options = {} );
        options.objectMode = this._readableState.objectMode;
        return this.pipe( new RotateStream( rotatefn, options ) );
    }

    return _stream;
}


util.inherits( RotateStream, stream.PassThrough );
function RotateStream( rotatefn, options ) {
    options || ( options = {} );
    options.highWaterMark = 1;
    stream.PassThrough.call( this, options );

    if ( typeof rotatefn == "object" ) {
        var options = rotatefn;
        rotatefn = function ( data, size ) {
            var maxsize = isNaN( options.size )
                ? bytes( options.size ) : options.size;
            return size >= maxsize;
        }
    }

    this._rotatableState = { 
        rotatefn: rotatefn,
        pipefn: function () {
            throw new Error( "No pipe function was defined" )
        },
        destination: null,
        count: 0,
        size: 0
    };
}

RotateStream.prototype.pipe = function ( pipefn ) {
    if ( typeof pipefn != "function" ) {
        throw new Error( "Rotatable pipes must be a function" );
    }

    this._rotatableState.pipefn = pipefn;
    return this;
}

RotateStream.prototype._transform = function ( data, enc, done ) {
    var state = this._rotatableState;
    var rotate = state.rotatefn.call( this, data, state.size );
    if ( rotate || !state.destination ) {
        if ( state.destination ) {
            this.unpipe( state.destination );
            state.destination.end();
        }

        try {
            var pipeto = state.pipefn.call( this, state.count ++ );
            if ( !pipeto || !pipeto.write ) {
                throw new Error( "Pipe function returned a non-writable stream" );
            }
            state.destination = pipeto;
            stream.PassThrough.prototype.pipe.call( this, pipeto );
        } catch ( err ) {
            this.emit( "error", err );
        }

        state.size = 0; // reset the size
    }

    state.size += this._writableState.objectMode ? 1 : data.length;
    return stream.PassThrough.prototype._transform.apply( this, arguments );
}



