/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.query;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTestCase;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;
import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.datasource.DataStore;
import org.teiid.test.framework.exception.QueryTestFailedException;



/**
 * The AbstractQueryTransactionTest is the base implementation for the
 * {@link TransactionQueryTestCase}. This provides the default logic for perform a testcase.
 * The only method to implement in order to perform a basic, single datasource, test
 * is the {@link #testCase()} method.
 * 
 * AbstractQueryTransactionTest is the class that should be extended when a
 * testcase is being created to validate certain behavior
 * 
 * <br>
 * The following methods are useful when writing validation logic because they provide
 * a direct connection to the datasource that was used by the VDB.  This enables data
 * validation of expected behavior of Teiid.
 * <li>{@link #getSource(String)}. </li>
 * <li>{@link #getXASource(String)} </li>
 * 
 * <br>
 * 
 * @see QueryExecution for use when direct queries to the source are used to
 *      validate the results of the testcase.
 * 
 */
public abstract class AbstractQueryTransactionTest extends  com.metamatrix.jdbc.api.AbstractQueryTest
	implements TransactionQueryTestCase {
    
    private static boolean initialized = false;

    protected String testname = "NA";
    protected int fetchSize = -1;
    protected int queryTimeout = -1;

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
    public void setConnectionStrategy(ConnectionStrategy connStrategy) throws QueryTestFailedException {
	this.connStrategy = connStrategy;
	
	this.setConnection(connStrategy.getConnection());

    }

//    @Override
//    protected void compareResults(BufferedReader resultReader,
//	    BufferedReader expectedReader) throws IOException {
//	assertEquals(read(expectedReader, compareResultsCaseSensitive()), read(
//		resultReader, compareResultsCaseSensitive()));
//    }

    @Override
    protected void assignExecutionProperties(Statement stmt) {
	if (stmt instanceof com.metamatrix.jdbc.api.Statement) {
	    com.metamatrix.jdbc.api.Statement statement = (com.metamatrix.jdbc.api.Statement) stmt;

	    Properties executionProperties = this.connStrategy.getEnvironment();
	    if (executionProperties != null) {
		String txnautowrap = executionProperties
			.getProperty(CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP);
		if (txnautowrap != null) {
		    statement.setExecutionProperty(
			    CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP,
			    txnautowrap);
		    
		   
//		    this.print("TransactionAutoWrap = " + txnautowrap);
		}
		
		String fetchSizeStr = executionProperties
		    .getProperty(CONNECTION_STRATEGY_PROPS.FETCH_SIZE);
		if (fetchSizeStr != null) {
		    try {
			fetchSize = Integer.parseInt(fetchSizeStr);
			
			this.print("FetchSize = " + fetchSize);
		    } catch (NumberFormatException e) {
			fetchSize = -1;
		    // this.print("Invalid fetch size value: " + fetchSizeStr
		    // + ", ignoring");
		    }
		}

	    }
	    


	    if (this.fetchSize > 0) {
		try {
		    statement.setFetchSize(this.fetchSize);
		} catch (SQLException e) {
//		    this.print(e);
		}
	    }

	    if (this.queryTimeout > 0) {
		try {
		    statement.setQueryTimeout(this.queryTimeout);
		} catch (SQLException e) {
//		    this.print(e);
		}
	    }
	}

    }

    public boolean compareResultsCaseSensitive() {
	return true;
    }

    /**
     * Override <code>setupDataSource</code> if there is different mechanism for
     * setting up the datasources for the testcase
     * 
     * @throws QueryTestFailedException
     * @throws QueryTestFailedException
     * 
     * @since
     */
    @Override
    public void setup() throws QueryTestFailedException {

	if (!initialized) {
	    initialized = true;
	    DataStore.initialize(connStrategy);
	    
	}
	
	DataStore.setup(connStrategy);
	
    }

    /**
     * The source connection must be asked from the connection strategy because only here
     * is it known which model was mapped to which datasource.
     * This is because each test could potentially use an include/exclude datasource option
     * that could change the mappings between tests.
     * @param identifier
     * @return
     * @throws QueryTestFailedException
     */
    public Connection getSource(String identifier)
	    throws QueryTestFailedException {
	
	Connection conn = this.connStrategy.createDriverConnection(identifier);
	// force autocommit back to true, just in case the last user didnt
	try {
		conn.setAutoCommit(true);
	} catch (Exception sqle) {
		throw new QueryTestFailedException(sqle);
	}
	
	return conn;

    }

    public XAConnection getXASource(String identifier)
	    throws QueryTestFailedException {

	return this.connStrategy.createDataSourceConnection(identifier);

    }

    /**
     * Implement testCase(), it is the entry point to the execution of the test.
     * 
     * @throws Exception
     * 
     * @since
     */
    public abstract void testCase() throws Exception;

    /**
     * Indicates what should be done when a failure occurs in
     * {@link #testCase()}
     * 
     * @return
     * 
     * @since
     */
    public boolean rollbackAllways() {
	return false;
    }

    /**
     * Override <code>before</code> if there is behavior that needs to be
     * performed prior to {@link #testCase()} being called.
     * 
     * 
     * @since
     */
    public void before() {
    }

    /**
     * Override <code>after</code> if there is behavior that needs to be
     * performed after {@link #testCase()} being called.
     * 
     * 
     * @since
     */
    public void after() {
    }

    /**
     * At end of each test, perform any cleanup that your test requires. Note:
     * Do not cleanup any connections by calling {@link ConnectionStrategy#shutdown()}. 
     * That is performed by the
     * {@link TransactionContainer#runTransaction(TransactionQueryTestCase)} at the
     * end of the test.
     */
    public void cleanup() {

    }

    @Override
    public XAConnection getXAConnection() {
	return null;
    }

}
