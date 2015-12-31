var fs = require( 'fs' );
var zlib = require( 'zlib' );
var util = require( 'util' );
var bytes = require( 'bytes' );
var stream = require( 'stream' );
var lockfile = require( 'lockfile' );

var aws;
try {
    aws = require( 'aws-sdk' );
} catch ( err ) {}

module.exports = rotatable;
module.exports.RotateStream = RotateStream;

function rotatable( path, options ) {
    return new RotateStream( path, options );
}

util.inherits( RotateStream, fs.WriteStream );
function RotateStream( path, options ) {
    options || ( options = {} );

    if ( !options.flags ) {
        options.flags = 'a';
    }

    if ( !options.size ) {
        options.size = '5gb';
    }

    if ( options.flags.indexOf( 'a' ) == -1 ) {
        throw new Error( 'RotateStream must be opened in append mode' );
    }

    fs.WriteStream.call( this, path, options );

    this.size = isNaN( options.size )
        ? bytes( options.size ) : options.size;

    this.suffix = options.suffix || '';

    var that = this;
    this.on( 'open', function () {
        fs.fstat( this.fd, function ( err, stats ) {
            if ( err ) {
                that.emit( 'error', err );
            } else {
                that.ino = stats.ino;
            }
        })
    })

    if ( options.gzip ) {
        this.on( 'rotated', function ( _, path ) {
            this._compress( path );
        });
    }

    if ( options.upload ) {
        if ( !aws ) {
            throw new Error( 'The aws-sdk module isn\'t installed' )
        }

        var comps = options.upload.match( /s3\:\/\/((.*)\@)?([^\/]+)\/(.*)/ );

        if ( !comps ) {
            throw new Error( 'Malformed S3 upload path' );
        }

        var credentials = ( comps[ 2 ] || '' ).split( ':' );
        var bucket = comps[ 3 ];
        var prefix = comps[ 4 ] || '';

        if ( !bucket ) {
            throw new Error( 'S3 Upload bucket is not defined' );
        }

        var s3 = {
            signatureVersion: 'v4'
        }

        if ( credentials[ 0 ] ) {
            s3.accessKeyId = credentials[ 0 ];
        }

        if ( credentials[ 1 ] ) {
            s3.secretAccessKey = credentials[ 1 ];
        }

        s3 = new aws.S3( s3 );
        s3.bucket = bucket;
        s3.prefix = prefix;

        this.on( options.gzip ? 'compressed' : 'rotated', function ( _, path ) {
            this._upload( path, s3 );
        });
    }
}

RotateStream.prototype._reopen = function ( cb ) {
    var that = this;

    this.bytesWritten = 0;
    this.pos = 0;

    fs.close( this.fd, function ( err ) {
        if ( err ) {
            this.emit( 'error', err );
        } else {
            that.closed = that.destroyed = false;
            that.once( 'open', cb )
                .open();
        }
    });
    this.fd = null;
}

RotateStream.prototype._write = function ( data, encoding, cb ) {
    var that = this;

    // we first need to check if the file has already been rotated by a 
    // different process. while calling stat(2) on every batch could be 
    // expansive - the cost is still relatively low compared to the whole write
    // operation, and is mitigated by fs cache. We should still re-consider this
    // approach
    fs.stat( this.path, function ( err, stats ) {
        if ( err && err.code != 'ENOENT' ) {
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
        fs.WriteStream.prototype._write
            .call( that, data, encoding, function ( err ) {
                cb( err )
            });
    })
}

RotateStream.prototype._compress = function ( path ) {
    var that = this;
    var pathGzip = path + '.gz';
    this.emit( 'compress', path, pathGzip );
    fs.createReadStream( path )
        .pipe( zlib.createGzip() )
        .pipe( fs.createWriteStream( pathGzip ) )
        .once( 'error', this.emit.bind( this, 'error' ) )
        .once( 'finish', function () {
            // remove the uncompressed file
            that._unlink( path, function () {
                that.emit( 'compressed', path, pathGzip );
            })
        });
}

RotateStream.prototype._upload = function ( path, s3 ) {
    var that = this;
    var key = s3.prefix + '/' + require( 'path' ).basename( path );

    this.emit( 'upload', path, s3.bucket + '/' + key );
    s3.putObject({
        Bucket: s3.bucket,
        Key: key,
        Body: fs.createReadStream( path )
    })
    .on( 'error', this.emit.bind( this, 'error' ) )
    .on( 'httpUploadProgress', function ( progress ) {
        that.emit( 'uploading', path, s3.bucket + '/' + key, progress );
    })
    .on( 'success', function () {
        // remove the uploaded file
        that._unlink( path, function () {
            that.emit( 'uploaded', path, s3.bucket + '/' + key );
        })
    })
    .send()
}

RotateStream.prototype._unlink = function ( path, cb ) {
    var that = this;
    this.emit( 'delete', path );
    fs.unlink( path, function ( err ) {
        if ( err ) {
            that.emit( 'error', err );
        } else {
            that.emit( 'deleted', path );
            cb();
        }
    })
}

RotateStream.prototype._rotate = function ( suffix, cb ) {
    var that = this;
    var path = this.path + '.' + suffix;
    var lock = this.path + '.lock';

    // prevent multiple processes from rotating the same file
    lockfile.lock( lock, { stale: 10000, wait: 5000 }, function ( err ) {
        if ( err ) {
            return _cb( err );
        }

        fs.stat( that.path, function ( err, stats ) {

            // if no entry, file has been moved or removed - keep going
            if ( err && err.code != 'ENOENT' ) {
                return _cb( err );
            }

            // was the file externally renamed?
            if ( !stats || that.ino != stats.ino ) {
                return _cb();
            }

            that.emit( 'rotate', that.path, path );
            fs.rename( that.path, path, function ( err ) {

                // if no entry, file has been moved or removed - keep going
                if ( err && err.code != 'ENOENT' ) {
                    return _cb( err );
                }

                _cb();
                that.emit( 'rotated', that.path, path );
            })
        })
    });

    function _cb ( err ) {
        lockfile.unlock( lock, function ( unlockerr ) {
            cb( unlockerr || err );
        })
    }
}


