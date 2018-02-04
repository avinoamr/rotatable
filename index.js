var stream = require('stream');
var pathlib = require('path');
var util = require('util');;
var zlib = require('zlib');
var fs = require('fs')

var bytes = require('bytes');
// var Promise = require('bluebird');
var lockfile = require('lockfile');
var aws;



try {
    aws = require('aws-sdk');
    var s3 = Promise.promisifyAll(new aws.S3())
} catch (err) {}

var DEFAULT_SIZE = '5gb';
var DEFAULT_FLAGS = 'a';

/**
 * Rotateable used for streaming data
 * to multiple files limited by size.
 */
class RotateStream extends fs.WriteStream {
    /**
     * Given an local path and options initialize listeners,
     * and return Stream instance ready to be writen on
     * @param path
     * @param options
     */
    constructor(path, options) {
        options || (options = {});

        if (!options.flags) {
            options.flags = DEFAULT_FLAGS;
        }
        if (!options.size) {
            options.size = DEFAULT_SIZE;
        }
        if (options.flags.indexOf('a') == -1) {
            throw new Error('RotateStream must be opened in append mode');
        }

        super(path, options);
        this.path = path;
        this.compress = options.gzip;
        this.size = isNaN(options.size) ?
            bytes(options.size) : options.size;

        this.suffix = options.suffix || '';

        this.on('open', function () {
            try {
                var stats = fs.fstatSync(this.fd)
                this.ino = stats.ino;
            } catch (err) {
                this.emit('error', err);
            }
        })

        if (options.upload) {
            if (!aws) {
                throw new Error('The aws-sdk module isn\'t installed')
            }

            var comps = options.upload.match(/s3\:\/\/((.*)\@)?([^\/]+)\/(.*)/);

            if (!comps) {
                throw new Error('Malformed S3 upload path');
            }

            var credentials = (comps[2] || '').split(':');
            var bucket = comps[3];
            var prefix = comps[4] || '';

            if (!bucket) {
                throw new Error('S3 Upload bucket is not defined');
            }

            var s3 = {
                signatureVersion: 'v4'
            }

            if (credentials[0]) {
                s3.accessKeyId = credentials[0];
            }

            if (credentials[1]) {
                s3.secretAccessKey = credentials[1];
            }

            this._s3 = s3;
            this._s3.bucket = bucket;
            this._s3.prefix = prefix;
        }
    }

    /**
     * reopen
     * @private
     */
    _reopen() {

        this.bytesWritten = 0;
        this.pos = 0;
        try {
            fs.closeSync(this.fd)
            this.closed = this.destroyed = false;
            this.open();
            this.fd = null;
        } catch (err) {
            this.emit('error', err);
        }
    }

    /**
     * write
     * @private
     */
    _write(data, encoding, callback) {
        var self = this;
        var stats = fs.statSync(this.path)

        if (stats.size >= this.size) {
            var rand = '.' + Math.random().toString(36).substr(2, 6);
            var suffix = stats.birthtime.toISOString() + rand + this.suffix;
            var rotatedPath = _rotate(this.path, suffix, this.compress, self);
            this._reopen();

            (this.compress ?
                _compress(rotatedPath, self) :
                Promise.resolve(rotatedPath))
            .then((path) => {
                    return _upload(path, self._s3, self);
                })
                .then(() => {
                    this._reopen.call(self);
                    super._write.call(self, data, encoding, callback);
                })
                .catch(callback)
        } else {
            super._write.call(self, data, encoding, callback);
        }
    }
}


/**
 * upload
 * @private
 */
function _upload(path, s3, stream) {

    var date = path.match(/(\d{4})-(\d{2})-(\d{2})T\d{2}:\d{2}:\d{2}\.\d{3}Z/)
    var fileName = pathlib.basename(path);
    var key = pathlib.join(s3.prefix, date[1], date[2], date[3], fileName);
    stream.emit('upload', path, s3.bucket + '/' + key);

    return new Promise((resolve, reject) => {
        s3.putObject({
                Bucket: s3.bucket,
                Key: key,
                Body: fs.createReadStream(path)
            })
            .on('httpUploadProgress', (progress) => {
                stream.emit('uploading', path, s3.bucket + '/' + key, progress);
            })
            .on('success', (response) => {
                stream.emit('uploaded', path, s3.bucket + '/' + key);
                // remove the uploaded file
                _unlink(path, stream)
                resolve(response)
            })
            .on('error', (response) => {
                stream.emit('error');
                reject(new Error('Failed to upload file : ' + fileName))
            })
            .send()
    });
}


/**
 * rotate
 * @private
 */
function _rotate(path, suffix, toCompress, stream) {

    var newPath = path + '.' + suffix;
    var lockPath = path + '.lock';

    try {
        lockfile.lockSync(lockPath, {
            stale: 10000
        });
        var stats = fs.statSync(path);
        stream.emit('rotate', path, newPath)
        fs.renameSync(path, newPath);
        if (toCompress) {
            _compress(newPath, stream)
                .then()
        }
        stream.emit('rotated', path, newPath);

        return newPath;
    } catch (err) {
        stream.emit('error', err);
    } finally {
        lockfile.unlockSync(lockPath);
    }
}


/**
 * compress
 * @private
 */
function _compress(path, stream) {
    return new Promise((resolve, reject) => {
        var pathGzip = path + '.gz';
        stream.emit('compress', path, pathGzip);

        var inFile = fs.createReadStream(path);

        fs.createReadStream(path)
            .pipe(zlib.createGzip())
            .pipe(fs.createWriteStream(pathGzip))
            .once('error', (err) => {
                stream.emit('error', err);
                reject(err)
            })
            .once('finish', function () {
                stream.emit('compressed', path, pathGzip);
                // remove the uncompressed file
                _unlink(path, stream);
                resolve(pathGzip)
            });
    });
}


/**
 * unlink
 * @private
 */
function _unlink(path, stream) {
    stream.emit('delete', path);
    try {
        fs.unlinkSync(path)
    } catch (err) {
        stream.emit('error', err);
    }
}


/**
 * Exports RotateStream class
 * @param path
 * @param options
 * @returns {RotateStream}
 */
module.exports.createRotatable = (path, options) => {
    return new RotateStream(path, options);
};

/**
 * Export RotateStream constructor for tests.
 * In code please use createRotateStream func.
 * @type {RotateStream}
 */
module.exports.RotateStream = RotateStream;