

/**
 *
 * This script will assign a datasource, defined via JNDI, to a specific Model for a given VDB.
 *
 * Changes to make before executing:
 * 	- set VDB_NAME
 * 	- (optional) set VDB_VERSION - defaults to 1
 *	- set MODEL_NAME
 *	- set SOURCE_NAME
 * 	- (optional) set TRANSLATOR_NAME - defaults to "oracle"
 *	- set JNDI_NAME - set the jndi name of the data source to map to the MODEL_NAME
 *
 */
public class AssignDatasourceToModel{
	

	public final static VDB_NAME = "vdb-name"
	public final static int VDB_VERSION = 1
	public final static MODEL_NAME = "model-name"
	public final static SOURCE_NAME = "source-name"
	public final static TRANSLATOR_NAME = "oracle"
	public final static JNDI_NAME = "java:/jndi-name"
	
	/**
	 * @param args
	 */
	public static void main(def args){
        if ( VDB_NAME.equals( "vdb-name") ) {
          println( "**** Must change VDB_NAME property to the VDB you want to access")
          return
        }
        if ( MODEL_NAME.equals( "model-name") ) {
          println( "**** Must change MODEL_NAME property to the VDB you want to access")
          return
        }     
    
		connectAsAdmin( )

		try {
			assignToModel(VDB_NAME, VDB_VERSION, MODEL_NAME, SOURCE_NAME, TRANSLATOR_NAME, JNDI_NAME)

		} catch( Exception e ) {
			println( "Assigning Datasource exception: " + e.getMessage() )
		}
		
		disconnect()
		
	} /* main */
	
		
} /* class */
