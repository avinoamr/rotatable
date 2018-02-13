#!/usr/bin/env node

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

try {
    if ( !program.args.length ) {
        throw new Error( 'No destination FILE was specified' );
    }

    if ( program.config === true ) {
        program.config = 'rotatable';
    }

    if ( program.config && !config ) {
        throw new Error( 'The config module isn\'t installed' )
    }

    var conf = program.config
        ? config.get( program.config )
        : {};

    var verbose = program.verbose;
    var stream = rotatable( program.args[ 0 ], {
        size: program.size || conf.size,
        gzip: program.gzip || conf.gzip,
        upload: program.upload || conf.upload
    })
    .on( 'rotate', function ( path, to ) {
        verbose && console.log( 'Rotating  ', path, '=>', to, '...' );
    })
    .on( 'rotated', function ( path, to ) {
        verbose && console.log( 'Rotated   ', path, '=>', to );
    })
    .on( 'compress', function ( path, to ) {
        verbose && console.log( 'Gzipping  ', path, '=>', to, '...' );
    })
    .on( 'compressed', function ( path, to ) {
        verbose && console.log( 'Gzipped   ', path, '=>', to );
    })
    .on( 'upload', function ( path, to ) {
        verbose && console.log( 'Uploading ', path, '=>', to, '...' );
    })
    .on( 'uploaded', function ( path, to ) {
        verbose && console.log( 'Uploaded  ', path, '=>', to );
    })
    .on( 'delete', function ( path ) {
        verbose && console.log( 'Deleting  ', path );
    })
    .on( 'deleted', function ( path ) {
        verbose && console.log( 'Deleted   ', path );
    })

    process.stdin
        .pipe( stream )
} catch ( err ) {
    console.error( err.toString() );
    program.help();
}






