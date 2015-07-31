var fs = require( "fs" );
var util = require( "util" );
var bytes = require( "bytes" );
var stream = require( "stream" );

module.exports = rotatable;
module.exports.RotateStream = RotateStream;

function rotatable( path, options ) {
    return new RotateStream( path, options );
}

util.inherits( RotateStream, fs.WriteStream );
function RotateStream( path, options ) {
    options || ( options = {} );
    fs.WriteStream.call( this, path, options );

    this.size = 100;

    this.on( "open", function () {

    })
}

RotateStream.prototype._write = function ( data, encoding, cb ) {
    var that = this;

    var _cb = function ( err ) {
        if ( err ) return cb( err );

        fs.fstat( that.fd, function ( err, stats ) {
            if ( err ) {
                return cb( err );
            }
            console.log( that.path, stats.dev, stats.ino, stats.nlink, stats.rdev );

            if ( stats.size >= that.size ) {
                // return that._rotate(function ( err ) {
                //     if ( err ) return cb( err );
                    // cb();
                // })
            }

            cb();
        });
    }

    return fs.WriteStream.prototype._write.call( this, data, encoding, _cb );
}

RotateStream.prototype._rotate = function ( cb ) {
    var that = this;
    var path = this.path + "." + new Date().toISOString();

    this.bytesWritten = 0;
    fs.rename( this.path, path, function ( err ) {
        if ( err ) {
            return cb( err );
        }

        that.emit( "rotate", path );
        that.open();
        cb();
    })
}

