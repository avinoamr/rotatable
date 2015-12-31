var program = require( 'commander' );
var pkg = require( './package.json' );

var rotatable = require( './index' );

var config = null;
try {
    config = require( 'config' );
} catch ( err ) {}

program
    .version( pkg.version )
    .usage( '[options] <FILE>' )
    .arguments( 'FILE' )
    .description( 'Pipe standard input into a rotatable file' )
    .option( '-s, --size [bytes]', 'rotation size in bytes, or bytes strings like \'1gb\'' )
    .option( '-g, --gzip', 'compress rotated files with gzip' )
    .option( '-u, --upload [s3-path]', 'upload rotated files to s3. Requires the aws-sdk module. format: \'s3://[apikey:apisecret@]bucket/prefix/to/file\'' )
    .option( '-c, --config [key]', 'Use the config module to read the options' )
    .option( '-v, --verbose', 'output verbose messages' )
    .parse( process.argv );


if ( !program.args.length ) {
    console.error( 'Error: No destination FILE was specified' )
    program.help();
}

if ( program.config === true ) {
    program.config = 'rotatable';
}

if ( program.config && !config ) {
    console.error( 'Error: The config module isn\'t installed' )
    program.help();
}

var conf = program.config 
    ? config.get( program.config )
    : {};

try {
    var stream = rotatable( program.args[ 0 ], {
        size: program.size || conf.size,
        gzip: program.gzip || conf.gzip,
        upload: program.upload || conf.upload
    });
} catch ( err ) {
    console.error( err.toString() );
    program.help()
}

if ( program.verbose ) {
    stream
        .on( 'rotate', function ( path, to ) {
            console.log( 'Rotating  ', path, '=>', to, '...' );
        })
        .on( 'rotated', function ( path, to ) {
            console.log( 'Rotated   ', path, '=>', to );
        })
        .on( 'compress', function ( path, to ) {
            console.log( 'Gzipping  ', path, '=>', to, '...' );
        })
        .on( 'compressed', function ( path, to ) {
            console.log( 'Gzipped   ', path, '=>', to );
        })
        .on( 'upload', function ( path, to ) {
            console.log( 'Uploading ', path, '=>', to, '...' );
        })
        .on( 'uploaded', function ( path, to ) {
            console.log( 'Uploaded  ', path, '=>', to );
        })
        .on( 'delete', function ( path ) {
            console.log( 'Deleting  ', path );
        })
        .on( 'deleted', function ( path ) {
            console.log( 'Deleted   ', path );
        })
}

process.stdin
    .pipe( stream )
    .on( 'error', function ( err ) {
        console.error( err.toString() );
        process.exit( 1 );
    })

