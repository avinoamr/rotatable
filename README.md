# rotatable
Subclass of fs.WriteStream that supports automatic file rotations, from multiple parallel processes. Only supports append mode.

### Usage

```javascript
var rotatable = require( "rotatable" );

// create a test.log file, and rotate it every 10mb
var stream = rotatable( "test.log", { size: "10mb" } );
stream.on( "rotate", function ( newpath ) {
    // compress and or upload to s3
})

// pipe some data into the stream
otherstream.pipe( stream );

// or - use it in a Moran log
express()
    .use( morgan( { stream: stream } ) )

```

##### rotatable( path [, options ] ) 

Creates and returns a new RotateStream with the same arguments.

`options` is forwarded as is to the `fs.WriteStream` constructor. It also supports the following properties:

* **size** number of bytes, or a [parsable bytes-string](https://www.npmjs.com/package/bytes), that indicates when the file should be rotated.

##### rotatable.RotateStream

`RotateStream` is subclass of Node's [fs.WriteStream](https://nodejs.org/api/fs.html#fs_class_fs_writestream). 

**Event: 'rotate'**

* **rotated** the path to the rotated file

Emitted when the stream has finished rotating







