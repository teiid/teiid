/**
 *
 * This script will print out the details for a deployed VDB.
 *
 * Can run this by passing the -Dvdbname argument.  The following is an example:
 *
 *  ./adminshell.sh -Dvdbname={value}  .  ./examples/PrintVDBInfo.groovy
 *
 *  where {value} is the name of the VDB to be printed
 */
public class PrintVDBInfo {

    public static VDB_NAME = System.getProperty( 'vdbname' )
    
    def execute( ) {  
        if ( VDB_NAME.equals( null) ) {
 			println ( 'Must pass in -Dvdbname argument')
 			disconnect()		
 			return	
        }    

        connectAsAdmin()
        
        printVDBOutput( VDB_NAME )        
        
        disconnect()
        
    } 
    
    
    def printVDBOutput( vdbName ) {
       println( "---- PRINT VDB ---------")
       def v=getVDBs()
 
        for ( vdb in v ) {
            if ( vdb.getName().equals( vdbName ) ) {
                printVdbStuff( vdb )
            }
        }
    }
    
     def printVdbStuff( vdb ) {
        println( "VDB = " + vdb.getName() )
        def models = vdb.getModels()
        for ( m in models ) {
            println( "  Model: " + m.getName() )
            if ( m.isSource() ) {
                def sources = m.getSourceNames()
                for ( s in sources ) {
                    println( "   Source: " + s )
                }
            }
        }
        def policies = vdb.getDataPolicies()
        for (p in policies){
            println( " Policy: " + p.getName() )
            println( " Desc: " + p.getDescription() )
            println( " Role(s): " + p.getMappedRoleNames() )
            def perm = p.getPermissions()
            for (pp in perm) {
                println( "        " + pp.getResourceName() )
                println( "          Create: " + pp.getAllowCreate() )
                println( "            Read: " + pp.getAllowRead() )
                println( "          Update: " + pp.getAllowUpdate() )
                println( "          Delete: " + pp.getAllowDelete() )
            }
        }
    }    
    
} /* class */

test=new PrintVDBInfo()
test.execute();