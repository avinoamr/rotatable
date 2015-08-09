var fs = require( "fs" );
var util = require( "util" );
var bytes = require( "bytes" );
var stream = require( "stream" );
var lockfile = require( "lockfile" );

module.exports = rotatable;
module.exports.RotateStream = RotateStream;

function rotatable( path, options ) {
    return new RotateStream( path, options );
}

util.inherits( RotateStream, fs.WriteStream );
var _write = fs.WriteStream.prototype._write
function RotateStream( path, options ) {
    options || ( options = {} );

    if ( !options.flags ) {
        options.flags = "a";
    }

    if ( !options.size ) {
        options.size = "5gb";
    }

    if ( options.flags.indexOf( "a" ) == -1 ) {
        throw new Error( "RotateStream must be opened in append mode" );
    }

    fs.WriteStream.call( this, path, options );

    this.size = isNaN( options.size )
        ? bytes( options.size ) : options.size;

    this.suffix = options.suffix || "";

    var that = this;
    this.on( "open", function () {
        fs.fstat( this.fd, function ( err, stats ) {
            if ( err ) {
                that.emit( "error", err );
            } else {
                that.ino = stats.ino;
            }
        })
    })
}

RotateStream.prototype._reopen = function ( cb ) {
    var that = this;

    this.bytesWritten = 0;
    this.pos = 0;
    that.close( function () {
        that.closed = that.destroyed = false;
        that.once( "open", cb )
            .open();
    })
}

RotateStream.prototype._write = function ( data, encoding, cb ) {
    var that = this;

    // we first need to check if the file has already been rotated by a 
    // different process. while calling stat(2) on every batch could be 
    // expansive - the cost is still relatively low compared to the whole write
    // operation, and is mitigated by fs cache. We should still re-consider this
    // approach
    fs.stat( this.path, function ( err, stats ) {
        if ( err && err.code != "ENOENT" ) {
            // if no entry, file has been moved or removed - keep going
            return cb( err );
        }

        // file has been changed (possibly rotated by a different process),
        if ( !stats || ( that.ino && stats.ino != that.ino ) ) {
            return that._reopen( function () {
                that._write.call( that, data, encoding, cb );
            })
        }

        // file exceeds the maximum rotation size
        if ( stats.size >= that.size ) {
            var suffix = stats.birthtime.toISOString() + that.suffix;
            return that._rotate( suffix, function ( err ) {
                if ( err ) {
                    return cb( err );
                }

                that._reopen( function () {
                    that._write.call( that, data, encoding, cb );
                })
            })
        }

        // all is well, write the data
        _write.call( that, data, encoding, function ( err ) {
            cb( err )
        });
    })
}

RotateStream.prototype._rotate = function ( suffix, cb ) {
    var that = this;
    var path = this.path + "." + suffix;
    var lock = this.path + ".lock";

    // prevent multiple processes from rotating the same file
    lockfile.lock( lock, { stale: 10000, wait: 5000 }, function ( err ) {
        if ( err ) {
            return _cb( err );
        }

        fs.stat( that.path, function ( err, stats ) {

            // if no entry, file has been moved or removed - keep going
            if ( err && err.code != "ENOENT" ) {
                return _cb( err );
            }

            // was the file externally renamed?
            if ( !stats || that.ino != stats.ino ) {
                return _cb();
            }

            fs.rename( that.path, path, function ( err ) {

                // if no entry, file has been moved or removed - keep going
                if ( err && err.code != "ENOENT" ) {
                    return _cb( err );
                }

                _cb();
                that.emit( "rotate", path );
            })
        })
    });

    function _cb ( err ) {
        lockfile.unlock( lock, function ( unlockerr ) {
            cb( unlockerr || err );
        })
    }
}



