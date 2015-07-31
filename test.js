var rotatable = require( "./index" );

var f = rotatable( "test.log", { flags: "w" } );
w();

function w() {
    process.stdout.write( "." );
    f.write( "Hello World\n", function ( err ) {
        if ( err ) {
            console.error( err );
        } else {
            setTimeout( w, 500 );
        }
    })
}