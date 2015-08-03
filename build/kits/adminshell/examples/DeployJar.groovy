
/**
 * This script will deploy a module (i.e., UDF jar, jdbc driver, etc.) 
 * by passing in the -Djarfile={path_to_vdb_file} argument
 *  
 * Can run this by passing an argument according to the following example:
 *
 * Passing Parameters:
 *  ./adminshell.sh -Djarfile={path_to_vdb_file} .  ./examples/DeployJar.groovy
 *
 */
public class DeployJar{
	
    public static JAR_FILE = System.getProperty( 'jarfile' )
    
    public static int vdbversion = 1
    
    public void execute() {

        if ( JAR_FILE.equals( null) ) {
 			println ( 'Must pass in -Djarfile argument')
 			disconnect()		
 			return	
        } 

        connectAsAdmin( )
        
        boolean deployed = deployAndVerifyVDB (JAR_FILE)
        
        println( "disconnect" );
        disconnect()
        
    } /* main */
    
    
    public boolean deployAndVerifyVDB (JARFile) {
        
        boolean retValue = false
        
        println( "Deploying JAR " + JARFile )

        try {
            File f = new File(JAR_FILE)
            if (! f.exists() ) {
              println( "Jar file Doesn't exist " + f.getAbsolutePath() )
              return false
            }
            
            deploy(JAR_FILE)
            
            retValue = true
        } 
        catch ( Exception e ) {
            println( "deploy exception: " + e.getMessage() )
            return false;
        }
        
        println( "Deployed Jar file " + JARFile )
        
    } /* method */
    
    
    
} /* class */

test = new DeployJar()
test.execute();