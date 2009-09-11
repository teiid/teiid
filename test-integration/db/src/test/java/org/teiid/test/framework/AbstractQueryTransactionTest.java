/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework;

import static org.junit.Assert.assertNotNull;

import java.sql.Statement;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.test.framework.connection.ConnectionStrategyFactory;
import org.teiid.test.framework.datasource.DataSourceSetupFactory;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.jdbc.api.AbstractQueryTest;
import com.metamatrix.jdbc.api.ExecutionProperties;


/** 
 * The AbstractQueryTransactionTest is the class that should be extended when
 * a testcase is being created to validate certain behavior
 * 
 * @see QueryExecution for use when direct queries to the source are used
 * to validate the results of the testcase.
 * 
 */
public abstract class AbstractQueryTransactionTest  extends AbstractQueryTest implements TransactionQueryTest{
	Properties executionProperties = null;
	String testname = "NA";
	
	
	public AbstractQueryTransactionTest() {
		super();
	}
	
	public AbstractQueryTransactionTest(String testname) {
		super();
		this.testname = testname;
	}
	
	public String getTestName() {
		return this.testname;
	}
	
	
    public void setExecutionProperties(Properties props) {
       	assertNotNull(props);
        this.executionProperties = props;        
    }
    
    @Override protected void assignExecutionProperties(Statement stmt) {
        if (this.executionProperties != null) {           
            if (stmt instanceof com.metamatrix.jdbc.api.Statement) {
                com.metamatrix.jdbc.api.Statement statement = (com.metamatrix.jdbc.api.Statement)stmt;
                if (this.executionProperties.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP) != null) {
                    statement.setExecutionProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP, this.executionProperties.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP));
                }
                
                if (this.executionProperties.getProperty(ExecutionProperties.PROP_FETCH_SIZE) != null) {
                    statement.setExecutionProperty(ExecutionProperties.PROP_FETCH_SIZE, this.executionProperties.getProperty(ExecutionProperties.PROP_FETCH_SIZE));
                }
            }
        }
                
    }
    
    public int getNumberRequiredDataSources() {
    	return 1;
    }
    

	@Override
	public void setupDataSource() {
    	DataSourceSetup dss = null;
    	try {
    		
    		dss = DataSourceSetupFactory.createDataSourceSetup(getNumberRequiredDataSources());
    		dss.setup();
    	} catch(Exception e) {
    		throw new TransactionRuntimeException(e.getMessage());
    	}
    }
	
    
    
	/**
	 * Implement testCase(), it is the entry point to the execution of the test.
	 * @throws Exception
	 *
	 * @since
	 */
    public abstract void testCase() throws Exception;
        
    /**
     * Indicates what should be done when a failure occurs in {@link #testCase()}
     * @return
     *
     * @since
     */
    public boolean rollbackAllways() {
        return false;
    }
    
    /**
     * Override <code>before</code> if there is behavior that needs to be performed
     * prior to {@link #testCase()} being called.
     * 
     *
     * @since
     */
    public void before() {
    }
    
    /**
     * Override <code>after</code> if there is behavior that needs to be performed
     * after {@link #testCase()} being called.
     * 
     *
     * @since
     */
    public void after() {
    }
    
    
    public void cleanup() {
        ConnectionStrategyFactory.destroyInstance();

     	this.closeConnection();
    }
    
	@Override
	public XAConnection getXAConnection() {
		// TODO Auto-generated method stub
		return null;
	}


    
    
}
