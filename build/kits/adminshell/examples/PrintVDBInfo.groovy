

/**
 *
 * This script will print out the details for all deployed VDBs.
 *
 * Changes to make to use:
 *     - set property VDB_NAME
 */
public class PrintVDBInfo {

    public VDB_NAME = "vdb_name"
    
    
    def execute( ) {  
        if ( VDB_NAME.equals( "vdb_name") ) {
          println( "**** Must change VDB_NAME property to the VDB you want to print")
        }

        connectAsAdmin()
        
        printVDBOutput( VDB_NAME )        
        
        disconnect()
        
    } 
    
    
    def printVDBOutput( vdbName ) {
       println( "---- PRINT VDBs ---------")
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