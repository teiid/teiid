package org.teiid.test.framework;

import java.sql.Connection;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.test.framework.connection.ConnectionStrategy;
import org.teiid.test.framework.exception.QueryTestFailedException;

/**
 * The TransactionQueryTest interface represents the transaction test framework from which
 * the @link TransactionContainer operates.
 * @author vanhalbert
 *
 */

public interface TransactionQueryTest {
	
	/**
	 *  Returns the name of the test so that better tracing of what tests are running/completing.
	 *  @return String is test name 
	 */
	String getTestName();
	
	/**
	 * Called by the @link TransactionContainer to set the Teiid connection to be used in the test.
	 * @param conn
	 *
	 * @since
	 */
	void setConnection(Connection conn);
	
		
	/**
	 * Returns the connection being used in the test.
	 * @return
	 *
	 * @since
	 */
	Connection getConnection();
	
	XAConnection getXAConnection();
	
	
	/**
	 * Called to set the properties used to initialize prior
	 * to execution.
	 * @param props
	 *
	 * @since
	 */
     void setExecutionProperties(Properties props) ;
     
     
     /**
      * Called to set the current connection strategy being used.
      * @param connStrategy
      *
      * @since
      */
     void setConnectionStrategy(ConnectionStrategy connStrategy);
     
     /**
      * Indicates if the test has the required datasources in order to execute.
      * If it doesn't have the required datasources, it will be bypassed for execution.
      * @return true if the test has the required sources to execute
      *
      * @since
      */
     
     boolean hasRequiredDataSources();
   
     
     /**
      * Called by the {@link TransactionContainer} prior to testcase processing so that
      * the responsibility for performing datasource setup can be done
      * 
      *
      * @since
      */
     void setupDataSource() throws QueryTestFailedException;
	
    
    /**
     * Override <code>before</code> if there is behavior that needs to be performed
     * prior to {@link #testCase()} being called.
     * 
     *
     * @since
     */
    void before();
    
	/**
	 * Implement testCase(), it is the entry point to the execution of the test.
	 * @throws Exception
	 *
	 * @since
	 */
    void testCase() throws Exception;
    
    
    /**
     * Override <code>after</code> if there is behavior that needs to be performed
     * after {@link #testCase()} being called.
     * 
     *
     * @since
     */
    void after() ;
        
    /**
     * Indicates what should be done when a failure occurs in {@link #testCase()}
     * @return
     *
     * @since
     */
    boolean rollbackAllways() ;
    
    boolean exceptionExpected();
    
    boolean exceptionOccurred();


    /**
     * Called at the end of the test so that the testcase can clean itself up by
     * releasing any resources, closing any open connections, etc.
     * 
     *
     * @since
     */
    void cleanup();
    
    /**
     * validateTestCase is called after the testcase has been completed.   This enables
     * the validation to be performed as part of the overall testcase.
     * @throws Exception
     *
     * @since
     */
    void validateTestCase() throws Exception;


}
