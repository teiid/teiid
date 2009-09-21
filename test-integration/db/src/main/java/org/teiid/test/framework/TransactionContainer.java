/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework;

import java.sql.Connection;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.test.framework.datasource.DataSourceMgr;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;
import org.teiid.test.framework.connection.ConnectionStrategy;




public abstract class TransactionContainer {
	
		private boolean debug = false;
		
	   protected Properties props;
	   protected ConnectionStrategy connStrategy;
	    
	    protected TransactionContainer(ConnectionStrategy strategy){
	        this.connStrategy = strategy;
	        this.props = new Properties();
	        this.props.putAll(this.connStrategy.getEnvironment());
	        

	    }
	    
	    
	    /**
	     * Returns <code>true</code> if the test <b>CANNOT</b> be run due to not have the right
	     * number of datasources available.  
	     *
	     */
	    protected boolean turnOffTest (int numberofDataSources) {
	    	boolean rtn =  (numberofDataSources > DataSourceMgr.getInstance().numberOfAvailDataSources());
	    	if (rtn) {
	    		System.out.println("Required Number of DataSources is " + numberofDataSources + " but availables sources is " + DataSourceMgr.getInstance().numberOfAvailDataSources());
	    	}
	    	return rtn;
	    } 
	    
	    
	    /**
	     * Returns true when what the test says it needs, in regards to types of data sources (i.e., mysql, oracle,etc), 
	     * is found in the list of defined datatypes
	     * An example of returning false would be the following:
	     * <li>The defined datasources consist of oracle and sqlserver</li>
	     * <li>The test says it only supports mysql and oracle</li>
	     * Then the required datasources for the test are not available and therefore, the
	     * test cannot run.
	     * 
	     * @return true if the required datasources are available
	     *
	     * @since
	     */
	    protected boolean hasRequiredSources() {
	    	return true;
	    }
	    	    
	    protected void before(TransactionQueryTest test){}
	    
	    protected void after(TransactionQueryTest test) {}
	        
	    public void runTransaction(TransactionQueryTest test) {
	    	
	    	if (turnOffTest(test.getNumberRequiredDataSources())) {
	    		detail("Turn Off Transaction test: " + test.getTestName() + ", doesn't have the number of required datasources");
		        return;

	    	} else if (!hasRequiredSources()) {
	    		detail("Turn Off Transaction test: " + test.getTestName() + ",  required datasource types are not available");
		        return;
	    		
	    	}
	    	
	    	detail("Start transaction test: " + test.getTestName());

	        try {  
	        	test.setupDataSource();
	        	
	        	debug("	setConnection");
	            test.setConnection(getConnection());
	            test.setExecutionProperties(this.props);
	            debug("	before(test)");
	                        
	            before(test);
	            debug("	test.before");

	            test.before();
	            
	            debug("	test.testcase");

	            // run the test
	            test.testCase();
	            
	        }catch(Throwable e) {
	        	if (!test.exceptionExpected()) {
	        		e.printStackTrace();
	        	}
	            throw new TransactionRuntimeException(e.getMessage());
	        }finally {
	        	debug("	test.after");

	            test.after();
	            debug("	after(test)");

	            after(test);
	            debug("	test.cleanup");

	            test.cleanup();
	            
	            detail("End transaction test: " + test.getTestName());

	        }
	        
            if (test.exceptionExpected() && !test.exceptionOccurred()) {
            	throw new TransactionRuntimeException("Expected exception, but one did not occur");
            }
	        
	        try {
		        detail("Start validation: " + test.getTestName());

	        	test.validateTestCase();
	        	
	        	detail("End validation: " + test.getTestName());

	        }catch(Exception e) {
	            throw new TransactionRuntimeException(e);
	        }
	    }
	    
	    
	    protected Connection getConnection() throws QueryTestFailedException {
	    	return this.connStrategy.getConnection();
	    }
	        
	    protected XAConnection getXAConnection() throws QueryTestFailedException {
	    	return this.connStrategy.getXAConnection();
	    }
	    
	    
	    public Properties getEnvironmentProperties() {
	    	return props;
	    }
	    
	    protected void debug(String message) {
	    	if (debug) {
	    		System.out.println(message);
	    	}
	    	
	    }
	    
	    protected void detail(String message) {
	    	System.out.println(message);
	    }
    

}
