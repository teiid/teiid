/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTest;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;
import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.connection.ConnectionUtil;
import org.teiid.test.framework.datasource.DataSource;
import org.teiid.test.framework.datasource.DataSourceSetup;
import org.teiid.test.framework.datasource.DataSourceSetupFactory;
import org.teiid.test.framework.exception.QueryTestFailedException;

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
	
	protected ConnectionStrategy connStrategy;
	
	
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
		
	
    @Override
	public void setConnectionStrategy(ConnectionStrategy connStrategy) {
		this.connStrategy = connStrategy;
		
		this.datasources = this.connStrategy.getDataSources();
		
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
                String txnautowrap = this.executionProperties.getProperty(CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP);
                if (txnautowrap != null) {
                     statement.setExecutionProperty(CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP, txnautowrap);
                }
                
                if (this.executionProperties.getProperty(CONNECTION_STRATEGY_PROPS.FETCH_SIZE) != null) {
                    statement.setExecutionProperty(CONNECTION_STRATEGY_PROPS.FETCH_SIZE, this.executionProperties.getProperty(CONNECTION_STRATEGY_PROPS.FETCH_SIZE));
                }
            }
        }
                
    }
    
    public boolean hasRequiredDataSources() {
    	boolean rtn = true;
    	
    	if (getNumberRequiredDataSources() == 0 || getNumberRequiredDataSources() > this.connStrategy.getNumberAvailableDataSources()) {
    		this.print(getTestName() + " will not be run, it requires " + getNumberRequiredDataSources() + 
    				" datasources, but only available is " + this.connStrategy.getNumberAvailableDataSources());
    		return false;

    	}

    
    	
    	
    	return rtn;
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
     * @throws QueryTestFailedException 
     * @throws QueryTestFailedException 
     * 
     * @since
     */
    @Override
    public void setupDataSource() throws QueryTestFailedException {
    	
    	DataSourceSetup dss = DataSourceSetupFactory.createDataSourceSetup(this.getNumberRequiredDataSources());
    	dss.setup(datasources, connStrategy);

    }	


	public Connection getSource(String identifier) throws QueryTestFailedException {
    	return ConnectionUtil.getConnection(identifier, this.datasources, this.connStrategy);
    }    
    
    public XAConnection getXASource(String identifier) throws QueryTestFailedException {
       	return ConnectionUtil.getXAConnection(identifier, this.datasources, this.connStrategy);
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
     * At end of each test, perfoom any cleanup that your test requires.
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
