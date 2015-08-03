
/**
 * This script will restart server.
 *
 * This maybe necessary after deploying a JAR.
 *  
 */
public class RestartServer{
	   
    public void execute() {

        connectAsAdmin( )
        
        restart()
        
        disconnect()
        
    } /* main */
    

    
} /* class */

test = new RestartServer()
test.execute();