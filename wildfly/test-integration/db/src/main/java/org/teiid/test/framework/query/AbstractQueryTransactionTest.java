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

import org.teiid.jdbc.TeiidSQLException;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTestCase;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;
import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.connection.ConnectionStrategyFactory;
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
@SuppressWarnings("nls")
public abstract class AbstractQueryTransactionTest extends  org.teiid.jdbc.AbstractQueryTest
	implements TransactionQueryTestCase {
    
    private static String initialized = null;

    protected String testname = "NA";
    protected int fetchSize = -1;
    protected int queryTimeout = -1;

    protected ConnectionStrategy connStrategy;
    
    // because only a SQLException is accounted for in AbstractQueryTest, 
    //	the applicationException is used to when unaccounted for exceptions occur.  This could
    // unintentional errors from the driver or ctc client test code.
    private Throwable applicationException=null;

    public AbstractQueryTransactionTest() {
	super();
	
	this.connStrategy = ConnectionStrategyFactory
	    .createConnectionStrategy();
    }

    public AbstractQueryTransactionTest(String testname) {
	this();
	this.testname = testname;
    }

    public String getTestName() {
	return this.testname;
	
    }
    
    @Override
    public ConnectionStrategy getConnectionStrategy() {
	// TODO Auto-generated method stub
	return this.connStrategy;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void assignExecutionProperties(Statement stmt) {
	if (stmt instanceof org.teiid.jdbc.TeiidStatement) {
		org.teiid.jdbc.TeiidStatement statement = (org.teiid.jdbc.TeiidStatement) stmt;

	    Properties executionProperties = this.connStrategy.getEnvironment();
	    if (executionProperties != null) {
		String txnautowrap = executionProperties
			.getProperty(CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP);
		if (txnautowrap != null) {
		    statement.setExecutionProperty(
			    CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP,
			    txnautowrap);
		}
		
		String fetchSizeStr = executionProperties
		    .getProperty(CONNECTION_STRATEGY_PROPS.FETCH_SIZE);
		if (fetchSizeStr != null) {
		    try {
			fetchSize = Integer.parseInt(fetchSizeStr);
			
			TestLogger.log("FetchSize = " + fetchSize);
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
		    TestLogger.log(e.getMessage());
		}
	    }

	    if (this.queryTimeout > 0) {
		try {
		    statement.setQueryTimeout(this.queryTimeout);
		} catch (SQLException e) {
		    TestLogger.log(e.getMessage());
		}
	    }
	}

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
	
	this.applicationException = null;
	this.setConnection(connStrategy.getConnection());
	setupDataStore();
	
    }
    
    protected void setupDataStore() {
	
	
	if (! this.getConnectionStrategy().isDataStoreDisabled()) {
	    TestLogger.logDebug("Perform DataStore setup for test: " + this.testname );
        	if (initialized == null || !initialized.equalsIgnoreCase(this.getClass().getSimpleName()) ) {
        	    initialized = this.getClass().getSimpleName();
        	    DataStore.initialize(connStrategy);
        	    
        	}
        	
        	DataStore.setup(connStrategy);
	} else {
	    TestLogger.logDebug("DataStore setup is disabled for test: " + this.testname );
	}

    }

    /**
     * The source connection must be asked from the connection strategy because only here
     * is it known which model was mapped to which datasource.
     * This is because each test could potentially use an include/exclude datasource option
     * that could change the mappings between tests.
     * @param identifier
     * @return Connection
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
     * @return boolean
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
	ConfigPropertyLoader.reset();

    }

    @Override
    public XAConnection getXAConnection() {
	return null;
	
    }

    @Override
    public void setApplicationException(Throwable t) {
	this.applicationException = t;
	
    }

    @Override
    public boolean exceptionOccurred() {
	return (super.exceptionOccurred() ? super.exceptionOccurred() : this.applicationException != null);

    }
    
    @Override
    public SQLException getLastException() {
	if (super.getLastException() != null) {
	    return super.getLastException();
	}
	if (this.applicationException != null) {
	    if (this.applicationException instanceof SQLException) {
		return (SQLException) this.applicationException;
	    }
	    
	    TeiidSQLException mm = new TeiidSQLException(this.applicationException.getMessage());
	    return mm;

	}
	
	return null;
     }

    @Override
    public Throwable getApplicationException() {
	// TODO Auto-generated method stub
	return this.applicationException;
    }

    
    
    
    

}
