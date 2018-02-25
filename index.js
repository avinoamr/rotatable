var pathlib = require('path')
var zlib = require('zlib')

var Promise = require('bluebird')
var bytes = require('bytes')
var lockfile = Promise.promisifyAll(require('lockfile'))
var fs = Promise.promisifyAll(require('fs'))
var aws
// we don't necessarily need aws because we may not want to upload
try {
  aws = require('aws-sdk')
} catch (err) { }

const DEFAULT_OPTIONS = {
  flags: 'a',
  size: '5gb',
  suffix: ''
}
/**
 * @class RotateStream
 * @extends {fs.WriteStream}
 */
class RotateStream extends fs.WriteStream {
  /**
   * Given an local path and options initialize listeners,
   * and return Stream instance ready to be writen on
   * @param path
   * @param options
   */
  constructor(path, options) {
    options = Object.assign({}, DEFAULT_OPTIONS, options)
    if (options.flags.indexOf('a') === -1) {
      throw new Error('RotateStream must be opened in append mode')
    }
    super(path, options)

    this.compress = options.gzip
    this.size = isNaN(options.size) ? bytes(options.size) : options.size
    this.suffix = options.suffix
    if (options.upload) {
      if (!aws) {
        throw new Error('The aws-sdk module isn\'t installed')
      }

      this.toUpload = true
      this._s3 = _getS3(options.upload)
    }
  }

  /**
   * @private
   */
  _openAsync() {
    let self = this
    return fs.openAsync(this.path, this.flags, this.mode)
      .then((fd) => {
        self.fd = fd
        return fs.fstatAsync(fd)
      })
      .then((stats) => {
        self.ino = stats.ino
      })
      .catch((err) => {
        if (self.autoClose) {
          self.destroy()
        }
        throw err
      })
  }

  /**
   * @private
   */
  _reopen() {
    let self = this

    this.bytesWritten = 0
    return fs.closeAsync(self.fd)
      .then(() => {
        self.closed = self.destroyed = false
        self.fd = null
      })
      .bind(self).then(self._openAsync)
  }

  /**
   * @private
   */
  _write(data, encoding, callback) {
    let self = this
    let lock = this.path + '.lock'

    // locking the file to prevent paralel writes
    lockfile.lockAsync(lock, {
      stale: 10000,
      wait: 5000
    }).then(() => {
      return fs.statAsync(self.path)
    }).catch((err) => {
      // if no entry, file has been moved or removed keep going
      if (err.code === "ENOENT") {
        return null // file has been moved or removed. force a re-open.
      } else {
        throw err
      }
    }).then((stats) => {
      // file has been changed
      // (possibly rotated by a different process)
      if (!self.ino || !stats || self.ino && stats.ino !== self.ino) {
        return self._reopen()
          .then(() => {
            return fs.statAsync(self.path)
          })
      } else {
        return stats
      }
    }).then((stats) => {  // rotating log file
      if (stats.size >= self.size) {
        let rand = '.' + Math.random().toString(36).substr(2, 6)
        let suffix =
          stats.birthtime.toISOString() + rand + self.suffix
        return _rotate(self.path, suffix, self)
          .bind(this).then(this._reopen)
      }
    }).then(() => {
      return self.writeAsync(data, encoding)
    }).then(() => {
      return lockfile.unlockAsync(lock)
    }).then(() => {
      callback()
    }).catch((err) => {
      self.emit('error', err)
      callback(err)
    })
  }

  writeAsync(data, encoding) {
    let self = this

    if (!(data instanceof Buffer))
      return Promise.reject(new Error('Invalid data'))

      return fs.writeAsync(this.fd, data, 0, data.length, this.pos)
        .then((bytes) => {
          self.bytesWritten += bytes

          if (self.pos !== undefined)
            self.pos += data.length
        })
        .catch((err) => {
          if (self.autoClose) {
            self.destroy()
          }
          throw err
        })
  }
}

/**
 * @private
 */
function _upload(path, stream) {
  return new Promise((resolve, reject) => {
    if (!stream.toUpload) {
      resolve(path)
    } else {
      let date = path.match(
        /(\d{4})-(\d{2})-(\d{2})T\d{2}:\d{2}:\d{2}\.\d{3}Z/)
      let fileName = pathlib.basename(path)
      let s3 = stream._s3
      let key = pathlib.join(
        s3.prefix,
        date[1],
        date[2],
        date[3],
        fileName
      )
      stream.emit('upload', path, s3.bucket + '/' + key)
      s3.putObject({
        Bucket: s3.bucket,
        Key: key,
        Body: fs.createReadStream(path)
      })
        .on('httpUploadProgress', (progress) => {
          stream.emit(
            'uploading',
            path,
            s3.bucket + '/' + key,
            progress)
        })
        .on('success', () => {
          stream.emit('uploaded', path, s3.bucket + '/' + key)
          // remove the uploaded file
          return _unlink(path, stream)
            .then(() => {
              resolve('S3')
            })
        })
        .on('error', () => {
          reject(new Error('Failed to upload file : ' + fileName))
        })
        .send()
    }
  })
}

/**
 * @private
 */
function _rotate(originalPath, suffix, stream) {

  let newPath = originalPath + '.' + suffix
  stream.emit('rotate', originalPath, newPath)

  return fs.renameAsync(originalPath, newPath)
    .then(() => {
      return _compress(newPath, stream)
    })
    .then((gZipedPath) => {
      return _upload(gZipedPath, stream)
    })
    .tap((gZipedPath) => {
      stream.emit('rotated', originalPath, gZipedPath || '')
    })
}

/**
 * @private
 */
function _compress(path, stream) {
  return new Promise((resolve, reject) => {
    let pathGzip = path + '.gz'

    if (!stream.compress) {
      resolve(path)
    } else {
      stream.emit('compress', path, pathGzip)
      fs.createReadStream(path)
        .pipe(zlib.createGzip())
        .pipe(fs.createWriteStream(pathGzip))
        .once('error', (err) => {
          reject(err)
        })
        .once('finish', () => {
          stream.emit('compressed', path, pathGzip)
          _unlink(path, stream)
            .then(() => {
              resolve(pathGzip)
            }).catch((err) => {
                reject(err)                
            })
        })
    }
  })
}

/**
 * @private
 */
function _getS3(upload) {
  let comps = upload.match(/s3\:\/\/((.*)\@)?([^\/]+)\/(.*)/)
  if (!comps) {
    throw new Error('Malformed S3 upload path')
  }
  let bucket = comps[3]
  if (!bucket) {
    throw new Error('S3 Upload bucket is not defined')
  }

  let credentials = (comps[2] || '').split(':')
  let prefix = comps[4] || ''
  let options = {
    signatureVersion: 'v4'
  }
  if (credentials[0]) {
    options.accessKeyId = credentials[0]
  }
  if (credentials[1]) {
    options.secretAccessKey = credentials[1]
  }
  s3Instance = new aws.S3(options)
  s3Instance.bucket = bucket
  s3Instance.prefix = prefix
  return s3Instance
}

/**
 * @private
 */
function _unlink(path, stream) {
  stream.emit('delete', path)
  return fs.unlinkAsync(path)
    .catch((err) => {
      // if no entry, file has been moved or removed - keep going
      if (err.code !== 'ENOENT') {
        throw err
      }
    })
}

/**
 * Exports RotateStream class
 * @param path
 * @param options
 * @returns {RotateStream}
 */
module.exports.createRotatable = (path, options) => {
  return new RotateStream(path, options)
}

/**
 * Export RotateStream constructor for tests.
 * In code please use createRotateStream func.
 * @type {RotateStream}
 */
module.exports.RotateStream = RotateStream
