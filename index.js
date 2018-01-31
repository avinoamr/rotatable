var util = require('util');
var bytes = require('bytes');
var pathlib = require('path');
var stream = require('stream');
var Promise = require('bluebird');
var fs = Promise.promisifyAll(require('fs'))
var zlib = Promise.promisifyAll(require('zlib'))
var lockfile = Promise.promisifyAll(require('lockfile'))
var aws;
try {
    aws = require('aws-sdk');
} catch (err) {}
const s3 = Promise.promisifyAll(new aws.S3())

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
            fs.fstat(this.fd, function (err, stats) {
                if (err) {
                    this.emit('error', err);
                } else {
                    this.ino = stats.ino;
                }
            })
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

            this._s3 = new aws.S3(s3);
            this._s3.bucket = bucket;
            this._s3.prefix = prefix;
        }
    }

    /**
     * reopen
     * @private
     */
    _reopen = function () {

        this.bytesWritten = 0;
        this.pos = 0;

        return fs.closeAsync(this.fd)
            .then(() => {
                this.closed = this.destroyed = false;
                this.open();
            })
            .catch(() => {
                this.emit('error', err);
            });
        this.fd = null;
    }

    /**
     * write
     * @private
     */
    _write = function (data, encoding, callback) {

        fs.statAsync(this.path)
            .then((stats) => {
                if (stats.size >= this.size) {
                    var rand = '.' + Math.random().toString(36).substr(2, 6);
                    var suffix = stats.birthtime.toISOString() + rand + this.suffix;
                    return this._rotate(suffix)
                        .then(this._reopen)
                        .catch(callback)
                }
            })
            .then(() => {
                // all is well, write the data
                super._write.call(this, data, encoding, callback);
            })

    }

    /**
     * compress
     * @private
     */
    _compress = function (path) {

        return new Promise((resolve, reject) => {
            var pathGzip = path + '.gz';
            this.emit('compress', path, pathGzip);

            fs.createReadStream(path)
                .pipe(zlib.createGzip())
                .pipe(fs.createWriteStream(pathGzip))
                .once('error', (err) => {
                    this.emit.bind(this, 'error');
                    reject(err)
                })
                .once('finish', function () {
                    this.emit('compressed', path, pathGzip);
                    // remove the uncompressed file
                    this._unlink(path);
                    resolve(pathGzip)
                });
        });
    }

    /**
     * upload
     * @private
     */
    _upload = function (path) {

        var date = path.match(/(\d{4})-(\d{2})-(\d{2})T\d{2}:\d{2}:\d{2}\.\d{3}Z/)
        var fname = pathlib.basename(path);
        var s3 = this._s3;
        var key = pathlib.join(s3.prefix, date[1], date[2], date[3], fname);
        this.emit('upload', path, s3.bucket + '/' + key);

        return new Promise((resolve, reject) => {
            s3.putObject({
                    Bucket: s3.bucket,
                    Key: key,
                    Body: fs.createReadStream(path)
                })
                .on('httpUploadProgress', function (progress) {
                    this.emit('uploading', path, s3.bucket + '/' + key, progress);
                })
                .on('success', function (response) {
                    this.emit('uploaded', path, s3.bucket + '/' + key);
                    // remove the uploaded file
                    this._unlink(path, resolve)
                })
                .on('error', (response) => {
                    this.emit.bind(this, 'error');
                    reject(new Error('Failed to upload file : ' + fname))
                })
                .send()
        });
    }

    /**
     * unlink
     * @private
     */
    _unlink = function (path) {

        this.emit('delete', path);
        return fs.unlinkAsync(path)
            .then(() => {
                this.emit('deleted', path);
            })
            .catch(function (err) {
                this.emit('error', err);
            })
    }

    /**
     * rotate
     * @private
     */
    _rotate = function (suffix, callback) {

        var path = this.path + '.' + suffix;
        var lock = this.path + '.lock';

        return lockfile.lockAsync(lock, {
                stale: 10000,
                wait: 5000
            })
            .then(fs.statAsync(this.path))
            .then(fs.renameAsync(this.path, path))
            .then(() => {
                this.emit('rotate', this.path, path)
            })
            .then(() => {
                if (this.compress) {
                    return this._compress(path);
                }
                return path;
            })
            .then(this._upload)
            .then(() => {
                this.emit('rotated', this.path, path);
            })
            .then(callback)
            .catch(callback)
            .finally(() => {
                return lockfile.unlockAsync(lock)
            });
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