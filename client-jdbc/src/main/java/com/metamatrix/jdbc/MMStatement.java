/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.SqlUtil;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.jdbc.api.Annotation;
import com.metamatrix.jdbc.api.ExecutionProperties;
import com.metamatrix.jdbc.api.PlanNode;
import com.metamatrix.jdbc.api.Statement;

/**
 * <p> This object is used to execute queries and updates against the MetaMatrix
 * server. This object submits queries to MetaMatrix server by sending request messages.
 * The query execution gives a MMXResultSet object that can be used
 * to navigate the results. An update can be executed to return the number of rows
 * affected. The MMXStatement object can be used by multiple threads, the MMXResultSet
 * objects for each object is added to a hashtable so that we could have a ResultSet
 * object open for each executing thread. When the statement object is closed all
 * the ResultSet objects on any given statement are closed. </p>
 */

public class MMStatement extends WrapperImpl implements Statement {
	private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$

    // State constants
    protected static final int TIMED_OUT = 4;
    protected static final int CANCELLED = 3;
    protected static final int NO_TIMEOUT = 0;

    // integer indicating no maximum limit - used in some metadata-ish methods.
    private static final int NO_LIMIT = 0;

    //######## Configuration state #############
    private MMConnection driverConnection;
    private Properties execProps = null;

    // the string which is the XSLT style sheet
    private String styleSheet;

    // fetch size value. This is the default fetch size used by the server
    private int fetchSize = BaseDataSource.DEFAULT_FETCH_SIZE;

    // the fetch direction
    private int fetchDirection = ResultSet.FETCH_FORWARD;

    // the result set type
    private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
    private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;

    //######## Processing state #############

    // boolean to indicate if this statement object is closed
    private boolean isClosed = false;

    // Differentiate timeout from cancel in blocking asynch operation
    protected int commandStatus = -1;

    // number of seconds for the query to timeout.
    protected int queryTimeout = NO_TIMEOUT;

    //########## Per-execution state ########

    // ID for current request
    protected long currentRequestID = -1;

    //  the last query plan description
    private Map currentPlanDescription;

    // the last query debug log
    private String debugLog;

    // the last query annotations
    private List annotations;

    // resultSet object produced by execute methods on the statement.
    protected MMResultSet resultSet;

    private List<Exception> serverWarnings;

    // the per-execution security payload
    private Serializable payload;
    
    /** List of INSERT, UPDATE, DELETE AND SELECT INTO commands */
    private List batchedUpdates;
    
    /** Array of update counts as returned by executeBatch() */
    protected int[] updateCounts;
    
    /** default Calendar instance for converting date/time/timestamp values */
    private Calendar defaultCalendar;
    /** Max rows to be returned by executing the statement */
    private int maxRows = NO_LIMIT;
    private int maxFieldSize = NO_LIMIT;
    
    /** SPIN_TIMEOUT determines how responsive asynch operations will be to
     *  statement cancellation, closure, or execution timeouts.  
     *  1/2 second was chosen as default.
     */
    private static int SPIN_TIMEOUT = 500;
    
    //Map<out/inout/return param index --> index in results>
    protected Map outParamIndexMap = new HashMap();
    
    /**
     * Factory Constructor 
     * @param driverConnection
     * @param resultSetType
     * @param resultSetConcurrency
     */
    static MMStatement newInstance(MMConnection driverConnection, int resultSetType, int resultSetConcurrency) {
        return new MMStatement(driverConnection, resultSetType, resultSetConcurrency);        
    }
    
    /**
     * MMStatement Constructor.
     * @param driverConnection
     * @param resultSetType
     * @param resultSetConcurrency
     */
    MMStatement(MMConnection driverConnection, int resultSetType, int resultSetConcurrency) {
        this.driverConnection = driverConnection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.execProps = PropertiesUtils.clone(getConnectionProperties());
        
        // Set initial fetch size
        String fetchSizeStr = this.execProps.getProperty(ExecutionProperties.PROP_FETCH_SIZE);
        if(fetchSizeStr != null) {
            try {
                this.fetchSize = Integer.parseInt(fetchSizeStr);
            } catch(Exception e) {
                // silently failover to default
            }
        }        
    }

    protected ClientSideDQP getDQP() {
    	return this.driverConnection.getDQP();
    }
    
    protected MMConnection getMMConnection() {
    	return this.driverConnection;
    }
    
    protected TimeZone getServerTimeZone() throws SQLException {
    	return this.driverConnection.getServerConnection().getLogonResult().getTimeZone();
    }
        
    /**
     * Reset all per-execution state - this should be done before executing
     * a new command.
     */
    protected void resetExecutionState() throws SQLException {
        this.currentRequestID = -1;

        this.currentPlanDescription = null;
        this.debugLog = null;
        this.annotations = null;

        if ( this.resultSet != null ) {
            this.resultSet.close();
            this.resultSet = null;
        }

        this.serverWarnings = null;
        
        this.batchedUpdates = null;
        this.updateCounts = null;
        this.outParamIndexMap.clear();
    }

    /**
     * Adds sql to this statement object's current list of commands.
     * @param sql statement to be added to the batch
     */
    public void addBatch(String sql) throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();
        if (batchedUpdates == null) {
            batchedUpdates = new ArrayList();
        }
        batchedUpdates.add(sql);
    }

    /**
     * This method can be used by one thread to cancel a statement that is being
     * executed by another thread.
     * @throws SQLException should never occur.
     */
    public void cancel() throws SQLException {
        /* Defect 19848 - Mark the statement cancelled before sending the CANCEL request.
         * Otherwise, it's possible get into a race where the server response is quicker
         * than the exception in the exception in the conditionalWait(), which results in
         * the statement.executeQuery() call throwing the server's exception instead of the
         * one generated by the conditionalWait() method.
         */
        commandStatus = CANCELLED;
        cancelRequest();
    }

    /**
     * Warning could be schema validation errors or partial results warnings.
     * @throws SQLException should never occur.
     */
    public void clearWarnings() throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();

        // clear all the warnings on this statement, after this, getWarnings() should return null
        serverWarnings = null;
    }

    /**
     * Makes the set of commands in the current batch empty.
     *
     * @throws SQLException if a database access error occurs or the
     * driver does not support batch statements
     */
    public void clearBatch() throws SQLException {
        batchedUpdates.clear();
    }

    /**
     * In many cases, it is desirable to immediately release a Statements's database
     * and JDBC resources instead of waiting for this to happen when it is automatically
     * closed; the close method provides this immediate release.
     * @throws SQLException should never occur.
     */
    public void close() throws SQLException {
        if ( isClosed ) {
            return;
        }

        // close the the server's statement object (if necessary)
        if(currentRequestID > -1) {
            if(resultSet == null) {
            	try {
					this.getDQP().closeRequest(currentRequestID);
				} catch (MetaMatrixProcessingException e) {
					throw MMSQLException.create(e);
				} catch (MetaMatrixComponentException e) {
					throw MMSQLException.create(e);
				}
            } else {
                resultSet.close();
                resultSet = null;
            }
        }

        isClosed = true;

        // Remove link from connection to statement
        this.driverConnection.closeStatement(this);

        logger.fine(JDBCPlugin.Util.getString("MMStatement.Close_stmt_success")); //$NON-NLS-1$
        driverConnection = null;
    }

    /**
     * <p> This utility method checks if the jdbc statement is closed and
     * throws an exception if it is closed. </p>
     * @throws SQLException if the statement object is closed.
     */
    protected void checkStatement() throws SQLException {
        //Check to see the connection is closed and proceed if it is not
        if ( isClosed ) {
            throw new MMSQLException(JDBCPlugin.Util.getString("MMStatement.Stmt_closed")); //$NON-NLS-1$
        }
    }

	//## JDBC4.0-begin ##
	@Override
	//## JDBC4.0-end ##
    public boolean execute(String sql) throws SQLException {
        executeSql(new String[] {sql}, false, null);
        return hasResultSet();
    }
    
	//## JDBC4.0-begin ##
	@Override
	//## JDBC4.0-end ##
    public int[] executeBatch() throws SQLException {
        if (batchedUpdates == null || batchedUpdates.isEmpty()) {
            return new int[0];
        }
        String[] commands = (String[])batchedUpdates.toArray(new String[batchedUpdates.size()]);
        executeSql(commands, true, false);
        return updateCounts;
    }

	//## JDBC4.0-begin ##
	@Override
	//## JDBC4.0-end ##
    public ResultSet executeQuery(String sql) throws SQLException {
        executeSql(new String[] {sql}, false, true);
        return resultSet;
    }

	//## JDBC4.0-begin ##
	@Override
	//## JDBC4.0-end ##
    public int executeUpdate(String sql) throws SQLException {
        String[] commands = new String[] {sql};
        executeSql(commands, false, false);
        return this.updateCounts[0];
    }

    protected boolean hasResultSet() throws SQLException {
        return resultSet != null && resultSet.getMetaData().getColumnCount() > 0;
    }
    
	protected void createResultSet(ResultsMessage resultsMsg) throws SQLException {
		//create out/return parameter index map if there is any
		List listOfParameters = resultsMsg.getParameters();
		if(listOfParameters != null){
		    //get the size of result set
		    int resultSetSize = 0;
		    Iterator iteratorOfParameters = listOfParameters.iterator();
		    while(iteratorOfParameters.hasNext()){
		        ParameterInfo parameter = (ParameterInfo)iteratorOfParameters.next();
		        if(parameter.getType() == ParameterInfo.RESULT_SET){
		            resultSetSize = parameter.getNumColumns();
		            //one ResultSet only
		            break;
		        }
		    }

		    //return needs to be the first
		    int index = 0; //index in user call - {?=call sp(?)}
		    int count = 0;
		    iteratorOfParameters = listOfParameters.iterator();
		    while(iteratorOfParameters.hasNext()){
		        ParameterInfo parameter = (ParameterInfo)iteratorOfParameters.next();
		        if(parameter.getType() == ParameterInfo.RETURN_VALUE){
		            count++;
		            index++;
		            outParamIndexMap.put(new Integer(index), new Integer(resultSetSize + count));
		            break;
		        }
		    }

		    iteratorOfParameters = listOfParameters.iterator();
		    while(iteratorOfParameters.hasNext()){
		        ParameterInfo parameter = (ParameterInfo)iteratorOfParameters.next();
		        if(parameter.getType() != ParameterInfo.RETURN_VALUE && parameter.getType() != ParameterInfo.RESULT_SET){
		            index++;
		            if(parameter.getType() == ParameterInfo.OUT || parameter.getType() == ParameterInfo.INOUT){
		                count++;
		                outParamIndexMap.put(new Integer(index), new Integer(resultSetSize + count));
		            }
		        }
		    }
		}

		resultSet = new MMResultSet(resultsMsg, this, null, outParamIndexMap.size());
		resultSet.setMaxFieldSize(this.maxFieldSize);
	}
    
    protected void executeSql(String[] commands, boolean isBatchedCommand, Boolean requiresResultSet)
        throws SQLException, MMSQLException {
        checkStatement();
        resetExecutionState();
        RequestMessage reqMessage = createRequestMessage(commands,
				isBatchedCommand, requiresResultSet);
    	ResultsMessage resultsMsg = null;
        try {
        	resultsMsg = sendRequestMessageAndWait(reqMessage);
        } catch ( Throwable ex ) {
            String msg = JDBCPlugin.Util.getString("MMStatement.Error_executing_stmt", reqMessage.getCommandString()); //$NON-NLS-1$ 
            logger.log(Level.SEVERE, msg, ex);
            throw MMSQLException.create(ex, msg);
        }
        
        // warnings thrown
        List resultsWarning = resultsMsg.getWarnings();

        setAnalysisInfo(resultsMsg);

        if (resultsMsg.getException() != null) {
            throw MMSQLException.create(resultsMsg.getException());
        }

        // save warnings if have any
        if (resultsWarning != null) {
            accumulateWarnings(resultsWarning);
        }
        
        if (resultsMsg.isUpdateResult()) {
        	List[] results = resultsMsg.getResults();
        	this.updateCounts = new int[results.length];
            for (int i = 0; i < results.length; i++) {
            	updateCounts[i] = (Integer)results[i].get(0);
            }
        } else {
            createResultSet(resultsMsg);
        }
        
        logger.fine(JDBCPlugin.Util.getString("MMStatement.Success_query", reqMessage.getCommandString())); //$NON-NLS-1$
    }

	protected RequestMessage createRequestMessage(String[] commands,
			boolean isBatchedCommand, Boolean requiresResultSet) {
        RequestMessage reqMessage = new RequestMessage();
    	reqMessage.setCommands(commands);
    	reqMessage.setBatchedUpdate(isBatchedCommand);
    	reqMessage.setRequireResultSet(requiresResultSet);
		return reqMessage;
	}

    /**
     * Retreives the fetch direction this Statement object set as a performance hint
     * to the driver. The int returned will be one of the following constants from
     * the ResultSet interface: FETCH_FORWARD, FETCH_REVERSE, or FETCH_UNKNOWN.
     * @return int value indicating the direction in which results need to be fetched
     * @throws SQLException should never occur.
     */
    public int getFetchDirection() throws SQLException {
        return this.fetchDirection;
    }

    /**
     * Retreives the fetch size this Statement object set as a performance hint
     * to the driver. This is the number of rows the server fetches at a time when
     * the result set needs more rows.
     * @return int value indicating the number of rows the server fetches
     * @throws SQLException should never occur.
     */
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    /**
     * Retreives the maximum number of bytes that a result set column may contain.
     * @return int value giving the maximum size of a field
     * @throws SQLException should never occur.
     */
    public int getMaxFieldSize() throws SQLException {
        return maxFieldSize;
    }

    /**
     * Retrives the maximum number of rows that a ResultSet object may contain.
     * If the limit is exceeded the excess rows are dropped.
     * @return Max limit on rows on ResultSet.
     * @throws SQLException should never iccure.
     */
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    /**
     * Moves to this Statement object's next result, returns true if
     * it is a ResultSet object, and implicitly closes any current
     * ResultSet object(s) obtained with the method #getResultSet.
     * @return true if the next result is a ResultSet object;
     * false if it is an update count or there are no more results
     * @throws SQLException if there is an error in database.
     */
    public boolean getMoreResults() throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();

        // close any current ResultSet
        if ( resultSet != null ) {
            resultSet.close();
            resultSet = null;
        }
        // set the existing update count to -1
        // indicating that there are no more results
        this.updateCounts = null;
        return false;
    }

    /**
     * Moves to this Statement object's next result, deals with any current
     * ResultSet object(s) according to the instructions specified by the
     * given flag, and returns true if the next result is a ResultSet object.
     * @param current flag that gives instruction on what should happen
     * to current ResultSet objects obtained using the method getResultSet(
     * CLOSE_CURRENT_RESULT, KEEP_CURRENT_RESULT, or CLOSE_ALL_RESULTS).
     * @return true if the next result is a ResultSet object; false if it
     *  is an update count or there are no more results
     * @throws SQLException if there is an error in database.
     */
    public boolean getMoreResults(int current) throws SQLException {
        checkStatement();

        /*if (current == CLOSE_ALL_RESULTS || current == CLOSE_CURRENT_RESULT) {
            // since MetaMatrix only supports one ResultSet per statement,
            // these two cases are handled the same way.
            if (resultSet != null) {
                resultSet.close();
            }
        } else if (current == KEEP_CURRENT_RESULT) {
            // do nothing
        }

        rowsAffected = -1;
        */
        return false;
    }


    /**
     * Return the number of seconds the driver will wait for a statement object
     * to execute
     * @return int value giving the query timeout in seconds
     * @throws SQLException should never occur
     */
    public int getQueryTimeout() throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();
        return this.queryTimeout;
    }

    /**
     * Returns a ResultSet object that was produced by a call to the method execute.
     * We currently do not support execute method which could return multiple result
     * sets.
     * @return ResultSet object giving the next available ResultSet
     * @throws SQLException should never occur
     */
    public ResultSet getResultSet() throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();
        if (!hasResultSet()) {
        	return null;
        }
        return resultSet;
    }

    /**
     * Retrieves the concurrency mode for the ResultSet objects generated from
     * queries that this Statement object executes. All ResultSets are currently
     * read only.
     * @return intvalue giving the ResultSet concurrency
     * @throws SQLException should never occur
     */
    public int getResultSetConcurrency() throws SQLException {
        return this.resultSetConcurrency;
    }

    /**
     * Retrieves the type of the ResultSet objects generated from queries that this
     * statement executes.
     * @return int value indicating the type of the ResultSet
     * @throws SQLException should never occur
     */
    public int getResultSetType() {
        return this.resultSetType;
    }

    /**
     * This method returns the number of rows affected by a statement modifying a table.
     * @return Number of rows affected.
     * @throws SQLException should never occur
     */
    public int getUpdateCount() throws SQLException {
        checkStatement();
        if (this.updateCounts == null) {
        	return -1;
        }
        return this.updateCounts[0];
    }

    protected void accumulateWarnings(List<Exception> serverWarnings) {
    	if (serverWarnings == null || serverWarnings.isEmpty()) {
    		return;
    	}
    	if (this.serverWarnings == null) {
    		this.serverWarnings = new ArrayList<Exception>();
    	}
    	this.serverWarnings.addAll(serverWarnings);
    }

    /**
     * This method returns warnings returned by server.
     * @return null value as there are no warnings
     * @throws SQLException should never occur
     */
    public SQLWarning getWarnings() throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();

        if (serverWarnings != null && serverWarnings.size() != 0) {
            return WarningUtil.convertWarnings(serverWarnings);
        }
        return null;
    }

    /**
     * This method enbles/disables escape processing. When escape processing is
     * enabled the driver will scan any escape syntax and do escape substitution
     * before sending the escaped sql statement to the server
     * @param enable boolean value indicating if the escape processing should be turned on
     * @throws SQLException should never occur
     */
    public void setEscapeProcessing(boolean enable) throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();
        // do nothing, escape processing is always enabled.
    }

    /**
     * This sets the fetch direction that this Statement object's hint to MetaMatrix
     * to improve performance.
     * @param direction value indicating the direction in which results need to be fetched.
     * @throws SQLException as this method is not currently supported
     */
    public void setFetchDirection(int direction) throws SQLException {
        checkStatement();
        this.fetchDirection = direction;
    }

    /**
     * This sets the fetch size that this Statement object's hint to MetaMatrix for
     * improving performance.
     * @param rows Number of rows to fetch at a time
     * @throws SQLException If an invalid fetch size is set.
     */
    public void setFetchSize(int rows) throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();
        if ( rows < 0 ) {
            String msg = JDBCPlugin.Util.getString("MMStatement.Invalid_fetch_size"); //$NON-NLS-1$
            throw new MMSQLException(msg);
        }
        // sets the fetch size on this statement
        if (rows == 0) {
            this.fetchSize = BaseDataSource.DEFAULT_FETCH_SIZE;
        } else {
            this.fetchSize = rows;
        }
    }

    /**
     * Sets the limit on the maximum number of rows in a ResultSet object. This
     * method is currently implemented to throw an exception as it is not possible
     * to limit the number of rows.
     * @param maxRows int value indicating maximum rows that can be returned in a ResultSet
     */
    public void setMaxRows(int maxRows) throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();
        this.maxRows = maxRows;
    }

    /**
     * This sets to seconds the time limit for the number of seconds for a driver
     * to wait for a statement object to be executed.
     * @param seconds Maximum number of seconds for a statement object to execute.
     * throws SQLException, should never occur
     */
    public void setQueryTimeout(int seconds) throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();
        if (seconds >= 0) {
            queryTimeout = seconds;
        }
        else {
            throw new MMSQLException(JDBCPlugin.Util.getString("MMStatement.Bad_timeout_value")); //$NON-NLS-1$
        }
    }

    protected Properties getConnectionProperties() {
        return driverConnection.propInfo;
    }

    /**
     * Helper method for copy the connection properties to request message.
     * @param res Request message that these properties to be copied to.
     * @param props Connection properties.
     * @throws MMSQLException 
     */
    protected void copyPropertiesToRequest(RequestMessage res, Properties props) throws MMSQLException {
        // Get partial mode
        String partial = getExecutionProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE);
        res.setPartialResults(Boolean.valueOf(partial).booleanValue());

        // Get fetch size
        res.setFetchSize(fetchSize);

        // Get cursor type
        res.setCursorType(this.resultSetType);

        // Get xml validation mode
        String validate = getExecutionProperty(ExecutionProperties.PROP_XML_VALIDATION);
        if(validate == null) {
            res.setValidationMode(false);
        } else {
            res.setValidationMode(Boolean.valueOf(validate).booleanValue());
        }

        // Get xml format mode
        String format = getExecutionProperty(ExecutionProperties.PROP_XML_FORMAT);
        res.setXMLFormat(format);

        // Get transaction auto-wrap mode
        String txnAutoWrapMode = getExecutionProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP);
        try {
			res.setTxnAutoWrapMode(txnAutoWrapMode);
		} catch (MetaMatrixProcessingException e) {
			throw MMSQLException.create(e);
		}
        
        // Get result set cache mode
        String rsCache = getExecutionProperty(ExecutionProperties.RESULT_SET_CACHE_MODE);
        res.setUseResultSetCache(Boolean.valueOf(rsCache).booleanValue());
        
        res.setQueryPlanAllowed(!Boolean.valueOf(getExecutionProperty(ExecutionProperties.PLAN_NOT_ALLOWED)).booleanValue());
    }

    /**
     * Ends the command and sets the status to TIMED_OUT.
     */
    protected void timeoutOccurred() {
        logger.warning(JDBCPlugin.Util.getString("MMStatement.Timeout_ocurred_in_Statement.")); //$NON-NLS-1$
        try {
        	cancel();        
            commandStatus = TIMED_OUT;
            queryTimeout = NO_TIMEOUT;
            currentRequestID = -1;
            if (this.resultSet != null) {
                this.resultSet.close();
            }
        } catch (SQLException se) {
            logger.log(Level.SEVERE, JDBCPlugin.Util.getString("MMStatement.Error_timing_out."), se); //$NON-NLS-1$
        }
    }

    protected void cancelRequest() throws SQLException {
        checkStatement();

        try {
			this.getDQP().cancelRequest(currentRequestID);
		} catch (MetaMatrixProcessingException e) {
			throw MMSQLException.create(e);
		} catch (MetaMatrixComponentException e) {
			throw MMSQLException.create(e);
		}
    }

    /**
     * Set the per-statement security payload.  This optional payload will 
     * accompany each request to the data source(s) so that the connector
     * will have access to it.
     * <br>Once the payload is set, it will be used for each statment
     * execution until it is set to <code>null</code>, a new payload is set on
     * the statement or the statement is closed.</br>
     * 
     * <p>To remove an existing payload from a statement, call this method
     * with a <code>null</code> argument.</p>  
     * @param payload The payload that is to accompany requests executed
     * from this statement.
     * @since 4.2
     */
    public void setPayload(Serializable payload) {
        this.payload = payload;
    }

    public void setExecutionProperty(String name, String value) {
        this.execProps.put(name, value);
    }

    public String getExecutionProperty(String name) {
        return (String) this.execProps.get(name);
    }

    /**
     * Send out request message with necessary states.
     * @param transaction UsertTransaction
     * @param sql String of command or prepared string
     * @param listener Message Listener
     * @param timeout Maybe 0
     * @param isPreparedStatement flag indicating whether this statement is a PreparedStatement
     * @param isCallableStatement flag indicating whether this statement is a CallableStatement
     * @param params Parameters values of either PreparedStatement or CallableStatement
     * @param isBatchedCommand flag indicating whether the statements are being executed as a batch
     * @throws SQLException
     * @throws TimeoutException 
     * @throws InterruptedException 
     * @throws CommunicationException 
     */
    protected ResultsMessage sendRequestMessageAndWait(RequestMessage reqMsg)
        throws SQLException, InterruptedException, TimeoutException {
        
        this.currentRequestID = this.driverConnection.nextRequestID();
        // Create a request message
        reqMsg.markSubmissionStart();        
        reqMsg.setExecutionPayload(this.payload);        
        reqMsg.setDoubleQuotedVariableAllowed(Boolean.valueOf(
                getExecutionProperty(ExecutionProperties.ALLOW_DBL_QUOTED_VARIABLE))
                .booleanValue());
        String sqlOptions = getExecutionProperty(ExecutionProperties.PROP_SQL_OPTIONS);
        if (sqlOptions != null &&
            sqlOptions.toUpperCase().indexOf(ExecutionProperties.SQL_OPTION_SHOWPLAN.toUpperCase()) >= 0) {
            reqMsg.setShowPlan(true);
        }

        reqMsg.setFetchSize(getFetchSize());
        reqMsg.setStyleSheet(this.styleSheet);
        reqMsg.setRowLimit(this.maxRows);

        // Get connection properties and set them onto request message
        copyPropertiesToRequest(reqMsg, getConnectionProperties());

        reqMsg.setExecutionId(this.currentRequestID);
    	
        Future<ResultsMessage> pendingResult = null;
		try {
			pendingResult = this.getDQP().executeRequest(this.currentRequestID, reqMsg);
		} catch (MetaMatrixException e) {
			throw MMSQLException.create(e);
		}
		long timeoutMillis = queryTimeout * 1000;
        long endTime = System.currentTimeMillis() + timeoutMillis;
        ResultsMessage result = null;        
        while (result == null) {

        	if (timeoutMillis > 0 && endTime <= System.currentTimeMillis() && commandStatus != TIMED_OUT && commandStatus != CANCELLED) {
	            timeoutOccurred();
        	}
        	
            checkStatement();
			try {
				result = pendingResult.get(SPIN_TIMEOUT, TimeUnit.MILLISECONDS);
			} catch (ExecutionException e) {
				throw MMSQLException.create(e);
			} catch (TimeoutException e) {
				continue;
			}
        }
        
    	if (commandStatus == CANCELLED) {
            throw new MMSQLException(JDBCPlugin.Util.getString("MMStatement.Cancel_before_execute")); //$NON-NLS-1$
        }
    	 
    	if (commandStatus == TIMED_OUT) {
            throw new TimeoutException(JDBCPlugin.Util.getString("MMStatement.Timeout_before_complete")); //$NON-NLS-1$
        }    	
    	return result;
    }

    long getCurrentRequestID() {
        return this.currentRequestID;
    }

    /**
     * <p> This method sets a style sheet to this object. The style sheet is
     * to perform transformations.
     * @param reader The reader object from which the styleSheet is to be read
     * @throws IOException if unable to read the style sheet from the Reader object.
     */
    public void attachStylesheet(Reader reader) throws IOException {
        BufferedReader bufferedReader = null;
        StringBuffer buffer = new StringBuffer();
        try { 
            bufferedReader = new BufferedReader(reader);
            while(true) {
                String line = bufferedReader.readLine();
                if(line == null) {
                    break;
                }
                buffer.append( line );
            }
        } finally {
            if(bufferedReader != null) {
                bufferedReader.close();                
            }
        }
        this.styleSheet = buffer.toString();
    }

    /**
     * <p> This method removes any existing style sheet on this object.
     */
    public void clearStylesheet() {
        this.styleSheet = null;
    }

    void setPlanDescription(Map planDescription) {
        this.currentPlanDescription = planDescription;
    }

    void setDebugLog(String debugLog) {
        this.debugLog = debugLog;
    }

    void setAnnotations(List annotations) {
        this.annotations = annotations;
    }

    /**
     * Get Query plan description.
     * If the Statement has a resultSet, we get the plan description from the result set
     * If that plan description is null, though, we return the very first plan description
     * that was created from the resultsMessage in the method: setAnalysisInfo.
     * The plan description from the result set can be null if the resultsMsg stored in the
     * result set hasn't been created when getPlanDescription is called.
     * @return Query plan description, if it exists, otherwise null
     */
    public PlanNode getPlanDescription() {
        Map planDescription = null;
        if(this.resultSet != null) {
			planDescription = this.resultSet.getUpdatedPlanDescription();
        }
        if(planDescription != null) {
            this.currentPlanDescription = planDescription;
            return PlanNodeImpl.constructFromMap(this.currentPlanDescription);
        }else if(this.currentPlanDescription != null) {
            return PlanNodeImpl.constructFromMap(this.currentPlanDescription);
        }
        return null;
    }

    /**
     * Get query planner debug log.
     * @return Query planner debug log, or null if it doesn't exist
     */
    public String getDebugLog() {
        return this.debugLog;
    }

    /**
     * Get annotations
     * @return Query planner annotations - Collection of Annotation
     */
    public Collection getAnnotations() {
        return this.annotations;
    }

    public void setPartialResults(boolean isPartialResults){
        if(isPartialResults){
            this.execProps.put(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE, "true"); //$NON-NLS-1$
        }
    }

    /*
     * @see com.metamatrix.jdbc.api.Statement#getRequestIdentifier()
     */
    public String getRequestIdentifier() {
        if(this.currentRequestID >= 0) {
            return Long.toString(this.currentRequestID);
        }
        return null;
    }
    
    /**
     * Check is the statement is closed. Used primarily by the unit tests.
     * @return true if the statement is closed; false otherwise.
     */
    public boolean isClosed() {
        return this.isClosed;
    }

	protected void setAnalysisInfo(ResultsMessage resultsMsg) {
        this.debugLog = resultsMsg.getDebugLog();
        this.currentPlanDescription = resultsMsg.getPlanDescription();
        Collection serverAnnotations = resultsMsg.getAnnotations();
        if(serverAnnotations != null) {
            List annotations = new ArrayList(serverAnnotations.size());
            Iterator annIter = serverAnnotations.iterator();
            while(annIter.hasNext()) {
                String[] serverAnnotation = (String[]) annIter.next();
                Annotation annotation = new AnnotationImpl(serverAnnotation);
                annotations.add(annotation);                
            }
            this.annotations = annotations;            
        }
	}
    
    Calendar getDefaultCalendar() {
        if (defaultCalendar == null) {
            defaultCalendar = Calendar.getInstance();
        }
        return defaultCalendar;
    }
    
    void setDefaultCalendar(Calendar cal) {
        this.defaultCalendar = cal;
    }
            
	public boolean isPoolable() throws SQLException {
		checkStatement();
		return false;
	}

	public void setPoolable(boolean arg0) throws SQLException {
		checkStatement();
	}
	
	public Connection getConnection() throws SQLException {
		return this.driverConnection;
	}

	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public int getResultSetHoldability() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void setCursorName(String name) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void setMaxFieldSize(int max) throws SQLException {
		checkStatement();
        if ( max < 0 ) {
            throw new MMSQLException(JDBCPlugin.Util.getString("MMStatement.Invalid_field_size")); //$NON-NLS-1$
        }
		this.maxFieldSize = max;
	}
}