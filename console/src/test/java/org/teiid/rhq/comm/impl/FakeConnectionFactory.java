package org.teiid.rhq.comm.impl;


import java.util.Properties;

import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionFactory;
import org.teiid.rhq.comm.ConnectionPool;




public class FakeConnectionFactory implements
		ConnectionFactory {
    
    static boolean createdfactory = false;
	private static Properties connEnv;
    private ConnectionPool pool;
	
	public Connection createConnection(){

        return new FakeConnection(pool, connEnv);
           
	}
	
	


	@Override
	public String getURL() {
		// TODO Auto-generated method stub
		return null;
	}




	@Override
	public void initialize(Properties env, ConnectionPool connectionPool)
			throws ConnectionException {
			connEnv = env;
	        this.pool = connectionPool;
	        createdfactory = true;
		
	}


    public void closeConnection(Connection connection)  {

    }
    
  
    public String getProperty(String key) {
        return connEnv.getProperty(key);
    }
   

}
