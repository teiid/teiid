/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework;

import java.sql.Connection;
import java.util.Properties;
import java.util.Set;

import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.datasource.DataSourceMgr;
import org.teiid.test.framework.datasource.DataSourceSetupFactory;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;




public abstract class TransactionContainer {
	
		private boolean debug = false;
		
	   protected Properties props;
	   protected ConnectionStrategy connStrategy;
	    
	    protected TransactionContainer(ConnectionStrategy strategy){
	        this.connStrategy = strategy;
	        this.props = new Properties();
	        this.props.putAll(this.connStrategy.getEnvironment());
	        

	    }
	    
	    
	    protected Set getDataSources() {
	    	Set dss = null;
	    	
	    	
	    	return dss;
	    }
	    
   	    
	    protected void before(TransactionQueryTest test){}
	    
	    protected void after(TransactionQueryTest test) {}
	        
	    public void runTransaction(TransactionQueryTest test) {
	    		    	
	    	detail("Start transaction test: " + test.getTestName());

	        try {  
	        	test.setDataSources(connStrategy.getDataSources());
	        	test.setupDataSources();
	        	
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
	            
	        	debug("	test.after");

	            test.after();
	            debug("	after(test)");

	            after(test);
	            
	            detail("End transaction test: " + test.getTestName());

	            
	        }catch(Throwable e) {
	        	if (!test.exceptionExpected()) {
	        		e.printStackTrace();
	        	}
	            throw new TransactionRuntimeException(e.getMessage());
	        }finally {

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
	        
            debug("	test.cleanup");

            test.cleanup();
            
	    	detail("Completed transaction test: " + test.getTestName());


	    }
	    
	    protected Connection getConnection() throws QueryTestFailedException {
	    	return this.connStrategy.getConnection();
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
