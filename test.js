var rotatable = require( "./index" );

var f = rotatable( "test.log", { flags: "a" } )
    .on( "error", function ( err ) {
        console.error( err )
    })
    .on( "open", function () {
        console.log( "REOPEN" );
    })
    .on( "rotate", function ( path ) {
        console.log( "ROTATED", path )
    })
w();

function w() {
    process.stdout.write( "." );
    f.write( "Hello World\n", function ( err ) {
        if ( err ) {
            console.error( err );
        } else {
            setTimeout( w, 30 );
        }
    })
}