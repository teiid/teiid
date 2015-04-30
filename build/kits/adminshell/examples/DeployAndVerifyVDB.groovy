
/**
 * This script will deploy a VDB and then verify the status of its deployment
 *  
 * Can run this by passing parameters or using entering based on the prompts.
 *
 * Passing Parameters:
 *  ./adminshell.sh -Dvdbname=value -Dvdbfile={path_to_vdb_file} .  ./examples/DeployAndVerifyVDB.groovy
 *
 * Optional changes to make before executing:
 *     - (optional) set VDB_VERSION
 *
 */
public class DeployAndVerifyVDB{
	
    public static VDB_NAME = System.getProperty( 'vdbname' )
    public static VDB_VERSION = System.getProperty( 'vdbversion', '1' )
    public static VDB_FILE = System.getProperty( 'vdbfile' )
    
    public static int vdbversion = 1
    
    public void execute() {

        if ( VDB_NAME.equals( null) ) {
 			println ( 'Must pass in -Dvdbname argument')
 			disconnect()		
 			return	
        }
        
        if ( VDB_FILE.equals( null ) ) {
 			println ( 'Must pass in -Dvdbfilename argument')
 			disconnect()
 			return			
        }        
        
        vdbversion = Integer.valueOf( VDB_VERSION )

        connectAsAdmin( )
        
        println( "Deploying VDB " + VDB_NAME )
        boolean deployed = deployAndVerifyVDB (VDB_FILE, VDB_NAME, vdbversion)
        
        println( "Deployed VDB " + VDB_NAME + " is deployed " + deployed );
        println( "disconnect" );
        disconnect()
        
    } /* main */
    
    
    public boolean deployAndVerifyVDB (VDBfile, VDBname, VDBversion) {
        
        boolean retValue = false
        
        try {
            File f = new File(VDBfile)
            if (! f.exists() ) {
              println( "VDB Doesn't exist " + f.getAbsolutePath() )
              return false
            }
            
            deploy(VDBfile)
        } 
        catch ( Exception e ) {
            println( "deployVDB exception: " + e.getMessage() )
            return false;
        }
        
        println( "Deployed VDB " + VDB_NAME )
        
        def av = getVDBs() 
        for ( v in av ) {    
            println( "Check VDB status:    " + v.getName() + " exists: " + hasVDB( v.getName() ) + " status: " + v.getStatus() ) 
        }        
        
        for ( i in 0..10 ) {
            def org.teiid.adminapi.VDB theVDB = getVDB(VDBname, VDBversion)
            def org.teiid.adminapi.VDB.Status theStatus = theVDB.getStatus()
            println( "(" + i + ") " + theVDB.getName() + " status = " + theStatus.toString())
            if ( theStatus.toString().equals("ACTIVE") ) {
                retValue = true
                break
            }
            sleep 30000
        }
        
        return retValue
        
    } /* method */
    
    
    
} /* class */

test = new DeployAndVerifyVDB()
test.execute();