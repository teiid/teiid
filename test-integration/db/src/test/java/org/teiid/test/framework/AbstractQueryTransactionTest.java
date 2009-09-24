/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.connection.ConnectionStrategyFactory;
import org.teiid.test.framework.connection.ConnectionUtil;
import org.teiid.test.framework.datasource.DataSource;
import org.teiid.test.framework.datasource.DataSourceSetupFactory;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.jdbc.api.AbstractQueryTest;


/** 
 * The AbstractQueryTransactionTest is the class that should be extended when
 * a testcase is being created to validate certain behavior
 * 
 * @see QueryExecution for use when direct queries to the source are used
 * to validate the results of the testcase.
 * 
 */
public abstract class AbstractQueryTransactionTest  extends AbstractQueryTest implements TransactionQueryTest{
	protected Properties executionProperties = null;
	protected String testname = "NA";
	
	protected Map<String, DataSource> datasources = null;
	
	
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
	
    /**
     * Called to set the datasources used during this test
     * 
     * @since
     */
	public void setDataSources(Map<String, DataSource> datasources) {
		this.datasources = datasources;
	}
	
	
    public void setExecutionProperties(Properties props) {
       	assertNotNull(props);
        this.executionProperties = props;        
    }
    @Override
    protected void compareResults(BufferedReader resultReader, BufferedReader expectedReader) throws IOException {
    	assertEquals(read(expectedReader, compareResultsCaseSensitive()) , read(resultReader, compareResultsCaseSensitive()));
    }
    
    @Override protected void assignExecutionProperties(Statement stmt) {
        if (this.executionProperties != null) {  
             if (stmt instanceof com.metamatrix.jdbc.api.Statement) {
                com.metamatrix.jdbc.api.Statement statement = (com.metamatrix.jdbc.api.Statement)stmt;
                String txnautowrap = this.executionProperties.getProperty(ConnectionStrategy.TXN_AUTO_WRAP);
                if (txnautowrap != null) {
                     statement.setExecutionProperty(ConnectionStrategy.TXN_AUTO_WRAP, txnautowrap);
                }
                
                if (this.executionProperties.getProperty(ConnectionStrategy.FETCH_SIZE) != null) {
                    statement.setExecutionProperty(ConnectionStrategy.FETCH_SIZE, this.executionProperties.getProperty(ConnectionStrategy.FETCH_SIZE));
                }
            }
        }
                
    }
    
    public int getNumberRequiredDataSources() {
    	return 1;
    }
    
    public boolean compareResultsCaseSensitive() {
    	return true;
    }


    /**
     * Override <code>setupDataSource</code> if there is different mechinism for
     * setting up the datasources for the testcase
     * 
     * @since
     */
    @Override
	public void setupDataSources() {
    	DataSourceSetup dss = null;
    	try {
    		
    		dss = DataSourceSetupFactory.createDataSourceSetup(this.datasources);
    		dss.setup();
    	} catch(Exception e) {
    		throw new TransactionRuntimeException(e.getMessage());
    	}
    }	


	public Connection getSource(String identifier) throws QueryTestFailedException {
    	return ConnectionUtil.getConnection(identifier, this.datasources);
    }    
    
    public XAConnection getXASource(String identifier) throws QueryTestFailedException {
       	return ConnectionUtil.getXAConnection(identifier, this.datasources);
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
    

	@Override
	public void validateTestCase() throws Exception {
		
	}
    
    
    /**
     * At end of each test, perfrom any cleanup that your test requires.
     * Note:  Do not cleanup any connections.   That is performed by
     * the {@link TransactionContainer#runTransaction(TransactionQueryTest)} at the end of the test.
     */
    public void cleanup() {
 
    }
    
	@Override
	public XAConnection getXAConnection() {
		// TODO Auto-generated method stub
		return null;
	}


    
    
}
