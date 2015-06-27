# rotatable
Rotate Node streams into different destinations as data flows through

> You may want to split input data from a stream into several rotated output streams. While other libraries (stream-rotate) work reasonably well for rotating files, they don't support arbitrary writable stream destinations, like s3 uploads, network, etc. This library allows you to construct any complex rotation logic with full customizability.

### Usage

```javascript
var rotatable = require( "rotatable" );

rotatable( fs.createReadStream( "file" ) )
    .rotate({ size: "10mb" })
    .pipe( function ( n ) {
        return fs.createWriteStream( "file." + n )
    })
```

### API

`rotatable([ readable ])`

* **readable** a Node readable stream, or a standard `options` object that is passed to Node's `PassThrough` stream. Optional
* **returns** the `readable` stream itself, augmented with the `.rotate` method

Adds the `.rotate()` method to the provided readable stream, and returns it. This is the simplest way to attach the rotatable functionality to any existing stream. 

If you omit the `readable` argument, a `PassThrough` stream will be created and returned by default. You can also pass in an options object to be passed into the new `PassThrough`, for example:

```javascript
fs.createReadStream( "file.log" )
    .pipe( rotatable({ highWaterMark: 100 }) )
    .rotate( fn );
```

`rotate( fn [, options ] )`

* **fn** a function to determine if the destination should be rotated:
    - **data** the input chunk from the stream
    - **size** the accumlated data size (objects, of buffer length) since the last rotation, excluding the current chunk.
    - **returns** a boolean, if true, the pipe function will be invoked to allow you to determine the new destination stream
* **options** a standard options object that is passed to `PassThrough`
* **returns** a `RotateStream` object, which is a subclass of `PassThrough`

Creates a rotate stream with the provided rotate function, which will be invoked for every input data piped to it from the `readable` stream. The purpose of this function is to determine if the destination should be rotated for the input data.

```javascript
rotatable( stream )
    .rotate( function ( data, size ) {
        return size > 1000;
    })
```

The example above will rotate the destination every 1000 characters (or objects, when objectMode = true). Alternatively, you can pass the number of bytes to rotate on (using the `bytes` module to parse the string)

```javascript
rotatable( stream )
    .rotate({ size: "10mb" })
```

`pipe( fn )`

* **fn** a dynamic pipe stream function
    - **count** the current rotation count number (starts with 0, and increments by 1 for each rotation)
    - **returns** the new `stream.Writable` instance
* **returns** the `RotateStream` instance itself

Unlike to the normal `.pipe()` method, this augmented method receives a function which will dynamically pipe the data to the new destination when the `.rotate()` function evaluates to true.

```javascript
rotatable( stream )
    .rotate( function ( data, size ) {
        return size > 1000;
    })
    .pipe( function ( count ) { 
        return fs.createWriteStream( "out." + count );
    })
```

The example above will create files with the names "out.0", "out.1", "out.2", etc., each with a maximum size of 1000 characters.








