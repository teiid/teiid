
/**
 *
 * This will print out all the currently deployed VDB's and available Data Sources
 *
 *
 */
public class PrintAllVDBAndDatasources {
	
	def execute( ) {
		
		connectAsAdmin()
		
		println getDataSourceNames()
		println getVDBs()
		
		disconnect()
		
	}	

} /* class */

test=new PrintAllVDBAndDatasources()
test.execute();