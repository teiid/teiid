
/**
 * This script will deploy a VDB and then verify the status of its deployment
 *  
 * Can run this by passing parameters or using entering based on the prompts.
 *
 * Passing Parameters:
 *  ./adminshell.sh -Dvdbname=value -Dconntype={NONE, BY_VERSION, ANY} .  ./examples/SetConnectionTypeForVDB.groovy
 *
 * Optional changes to make before executing:
 *     - (optional) set VDB_VERSION by passing -D vdbversion={version}
 *
 */
public class SetConnectionTypeForVDB{
	
    public static VDB_NAME = System.getProperty( 'vdbname' )
    public static VDB_VERSION = System.getProperty( 'vdbversion', '1' )
    public static CONN_TYPE = System.getProperty( 'conntype', 'BY_VERSION' )
    
    // 1=NONE, 2=BY_VERSION, ANY}
        
    public static int vdbversion = 1
    
    public void execute() {

 		org.teiid.adminapi.VDB.ConnectionType conntype = org.teiid.adminapi.VDB.ConnectionType.BY_VERSION
 		
        if ( VDB_NAME.equals( null) ) {
 			println ( 'Must pass in -Dvdbname argument')
 			disconnect()		
 			return	
        }
        
         boolean invalidValue = true
        
        if ( CONN_TYPE.equals( 'NONE' ) ) {
 			invalidValue = false
        }        
        if ( CONN_TYPE.equals( 'ANY' ) ) {
 			invalidValue = false
		}		
		if ( CONN_TYPE.equals( 'BY_VERSION' ) ) {
  			invalidValue = false
		}
		
		if ( invalidValue ) {
		 	println ( 'Invalid -Dconntype value of ' + CONN_TYPE + ' must be set to NONE, BY_VERSION or ANY');	
		 	return
		}
		
		vdbversion = Integer.valueOf( VDB_VERSION )


        connectAsAdmin( )
        
        println( "Setting connection type for VDB " + VDB_NAME + " to " + CONN_TYPE)
        
        changeVDBConnectionType(VDB_NAME, vdbversion, CONN_TYPE)

        println( "Changed connection type for VDB " + VDB_NAME + " to " + CONN_TYPE)
        
        println( "disconnect" );
        disconnect()
        
    } /* main */
    
    
} /* class */

test = new SetConnectionTypeForVDB()
test.execute();