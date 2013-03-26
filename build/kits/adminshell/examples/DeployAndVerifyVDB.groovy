
/**
 * This script will deploy a VDB and then verify the status of its deployment
 *
 * Changes to make before executing:
 *     - set VDB_NAME
 *     - (optional) set VDB_VERSION
 *    - set VDB_FILE: path to the vdb.xml file
 *
 */
public class DeployAndVerifyVDB{
    

    public final static VDB_NAME = "vdb_name"
    public final static int VDB_VERSION = 1
    public final static VDB_FILE = "{path_to_vdb_xml_file}"
    
    public void execute() {

        if ( VDB_NAME.equals( "vdb_name") ) {
          println( "**** Must change VDB_NAME property to the VDB you want to print")
          return
        }
        
        if ( VDB_FILE.equals( "{path_to_vdb_xml_file}") ) {
          println( "**** Must change VDB_FILE property to the path location to the vdb file")
          return
        }        

        connectAsAdmin( )
        
        println( "Deploying VDB " + VDB_NAME )
        boolean deployed = deployAndVerifyVDB (VDB_FILE, VDB_NAME, VDB_VERSION)
        
        println( "Deployed VDB " + VDB_NAME + " is deployed " + deployed );
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