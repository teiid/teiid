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

package org.teiid.jdbc;

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.client.DQP;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.RequestMessage.ShowPlan;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.client.metadata.ResultsMetadataConstants;
import org.teiid.client.metadata.ResultsMetadataDefaults;
import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.util.SqlUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.jdbc.CancellationTimer.CancelTask;


public class StatementImpl extends WrapperImpl implements TeiidStatement {
	private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
	
	static CancellationTimer cancellationTimer = new CancellationTimer("Teiid Statement Timeout"); //$NON-NLS-1$
	
	private static final class QueryTimeoutCancelTask extends CancelTask {
		private WeakReference<StatementImpl> ref;
		private QueryTimeoutCancelTask(long delay, StatementImpl stmt) {
			super(delay);
			this.ref = new WeakReference<StatementImpl>(stmt);
		}

		@Override
		public void run() {
			StatementImpl stmt = ref.get();
			if (stmt != null) {
				stmt.timeoutOccurred();
			}
		}
	}

	enum State {
		RUNNING,
		DONE,
		TIMED_OUT,
		CANCELLED
	}
    protected static final int NO_TIMEOUT = 0;

    // integer indicating no maximum limit - used in some metadata-ish methods.
    private static final int NO_LIMIT = 0;

    //######## Configuration state #############
    private ConnectionImpl driverConnection;
    private Properties execProps;

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
    protected volatile State commandStatus = State.RUNNING;

    // number of seconds for the query to timeout.
    protected long queryTimeoutMS = NO_TIMEOUT;

    //########## Per-execution state ########

    // ID for current request
    protected long currentRequestID = -1;

    //  the last query plan description
    private PlanNode currentPlanDescription;

    // the last query debug log
    private String debugLog;

    // the last query annotations
    private Collection<Annotation> annotations;

    // resultSet object produced by execute methods on the statement.
    protected volatile ResultSetImpl resultSet;

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
    
    //Map<out/inout/return param index --> index in results>
    protected Map outParamIndexMap = new HashMap();
    
    private static Pattern TRANSACTION_STATEMENT = Pattern.compile("\\s*(commit|rollback|(start\\s*transaction))\\s*;?", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
    private static Pattern SET_STATEMENT = Pattern.compile("\\s*set\\s*(\\w+)\\s*(\\w*);?", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
    private static Pattern SHOW_STATEMENT = Pattern.compile("\\s*show\\s*(\\w*);?", Pattern.CASE_INSENSITIVE); //$NON-NLS-1$
    /**
     * Factory Constructor 
     * @param driverConnection
     * @param resultSetType
     * @param resultSetConcurrency
     */
    static StatementImpl newInstance(ConnectionImpl driverConnection, int resultSetType, int resultSetConcurrency) {
        return new StatementImpl(driverConnection, resultSetType, resultSetConcurrency);        
    }
    
    /**
     * MMStatement Constructor.
     * @param driverConnection
     * @param resultSetType
     * @param resultSetConcurrency
     */
    StatementImpl(ConnectionImpl driverConnection, int resultSetType, int resultSetConcurrency) {
        this.driverConnection = driverConnection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.execProps = new Properties(this.driverConnection.getExecutionProperties());
        
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

    protected DQP getDQP() {
    	return this.driverConnection.getDQP();
    }
    
    protected ConnectionImpl getMMConnection() {
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
        commandStatus = State.CANCELLED;
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
        if(resultSet != null) {
            resultSet.close();
            resultSet = null;
        }

        isClosed = true;

        // Remove link from connection to statement
        this.driverConnection.closeStatement(this);

        logger.fine(JDBCPlugin.Util.getString("MMStatement.Close_stmt_success")); //$NON-NLS-1$
    }

    /**
     * <p> This utility method checks if the jdbc statement is closed and
     * throws an exception if it is closed. </p>
     * @throws SQLException if the statement object is closed.
     */
    protected void checkStatement() throws TeiidSQLException {
        //Check to see the connection is closed and proceed if it is not
        if ( isClosed ) {
            throw new TeiidSQLException(JDBCPlugin.Util.getString("MMStatement.Stmt_closed")); //$NON-NLS-1$
        }
    }
    
    public ResultsFuture<Boolean> submitExecute(String sql) throws SQLException {
    	return executeSql(new String[] {sql}, false, ResultsMode.EITHER, false);
    }

	@Override
    public boolean execute(String sql) throws SQLException {
        executeSql(new String[] {sql}, false, ResultsMode.EITHER, true);
        return hasResultSet();
    }
    
	@Override
    public int[] executeBatch() throws SQLException {
        if (batchedUpdates == null || batchedUpdates.isEmpty()) {
            return new int[0];
        }
        String[] commands = (String[])batchedUpdates.toArray(new String[batchedUpdates.size()]);
        executeSql(commands, true, ResultsMode.UPDATECOUNT, true);
        return updateCounts;
    }

	@Override
    public ResultSet executeQuery(String sql) throws SQLException {
        executeSql(new String[] {sql}, false, ResultsMode.RESULTSET, true);
        return resultSet;
    }

	@Override
    public int executeUpdate(String sql) throws SQLException {
        String[] commands = new String[] {sql};
        executeSql(commands, false, ResultsMode.UPDATECOUNT, true);
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

		resultSet = new ResultSetImpl(resultsMsg, this, null, outParamIndexMap.size());
		resultSet.setMaxFieldSize(this.maxFieldSize);
	}
    
	@SuppressWarnings("unchecked")
	protected ResultsFuture<Boolean> executeSql(String[] commands, boolean isBatchedCommand, ResultsMode resultsMode, boolean synch)
        throws SQLException {
        checkStatement();
        resetExecutionState();
        if (logger.isLoggable(Level.FINER)) {
			logger.finer("Executing: requestID " + getCurrentRequestID() + " commands: " + Arrays.toString(commands) + " expecting: " + resultsMode); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}
        if (commands.length == 1) {
        	Matcher match = SET_STATEMENT.matcher(commands[0]);
        	if (match.matches()) {
        		if (resultsMode == ResultsMode.RESULTSET) {
        			throw new TeiidSQLException(JDBCPlugin.Util.getString("StatementImpl.set_result_set")); //$NON-NLS-1$
        		}
        		String key = match.group(1);
        		String value = match.group(2);
        		if (ExecutionProperties.NEWINSTANCE.equalsIgnoreCase(key) && Boolean.valueOf(value)) {
        			this.getMMConnection().getServerConnection().cleanUp();
        		} else {
        			JDBCURL.addNormalizedProperty(key, value, this.driverConnection.getExecutionProperties());
        		}
        		this.updateCounts = new int[] {0};
        		return booleanFuture(true);
        	}
        	match = TRANSACTION_STATEMENT.matcher(commands[0]);
        	if (match.matches()) {
    			logger.finer("Executing as transaction statement"); //$NON-NLS-1$
        		if (resultsMode == ResultsMode.RESULTSET) {
        			throw new TeiidSQLException(JDBCPlugin.Util.getString("StatementImpl.set_result_set")); //$NON-NLS-1$
        		}
        		String command = match.group(1);
        		Boolean commit = null;
        		if (StringUtil.startsWithIgnoreCase(command, "start")) { //$NON-NLS-1$
        			//TODO: this should force a start and through an exception if we're already in a txn
        			this.getConnection().setAutoCommit(false);
        		} else if (command.equalsIgnoreCase("commit")) { //$NON-NLS-1$
        			commit = true;
        			if (synch) {
        				this.getConnection().setAutoCommit(true);
        			}
        		} else if (command.equalsIgnoreCase("rollback")) { //$NON-NLS-1$
        			commit = false;
        			if (synch) {
        				this.getConnection().rollback(false);
        			}
        		}
        		this.updateCounts = new int[] {0};
        		if (commit != null && !synch) {
        			ResultsFuture<?> pending = this.getConnection().submitSetAutoCommitTrue(commit);
        			final ResultsFuture<Boolean> result = new ResultsFuture<Boolean>();
        			pending.addCompletionListener(new ResultsFuture.CompletionListener() {
        				@Override
        				public void onCompletion(ResultsFuture future) {
        					try {
								future.get();
								result.getResultsReceiver().receiveResults(false);
							} catch (Throwable t) {
								result.getResultsReceiver().exceptionOccurred(t);
							}
        				}
					});
        			return result;
        		}
        		return booleanFuture(false);
        	}
        	match = SHOW_STATEMENT.matcher(commands[0]);
        	if (match.matches()) {
    			logger.finer("Executing as show statement"); //$NON-NLS-1$
        		if (resultsMode == ResultsMode.UPDATECOUNT) {
        			throw new TeiidSQLException(JDBCPlugin.Util.getString("StatementImpl.show_update_count")); //$NON-NLS-1$
        		}
        		String show = match.group(1);
        		if (show.equalsIgnoreCase("PLAN")) { //$NON-NLS-1$
        			List<ArrayList<Object>> records = new ArrayList<ArrayList<Object>>(1);
        			PlanNode plan = driverConnection.getCurrentPlanDescription();
        			if (plan != null) {
        				ArrayList<Object> row = new ArrayList<Object>(3);
            			row.add(DataTypeTransformer.getClob(plan.toString()));
        				row.add(new SQLXMLImpl(plan.toXml()));
        				row.add(DataTypeTransformer.getClob(driverConnection.getDebugLog()));
        				records.add(row);
        			}
        			createResultSet(records, new String[] {"PLAN_TEXT", "PLAN_XML", "DEBUG_LOG"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        					new String[] {JDBCSQLTypeInfo.CLOB, JDBCSQLTypeInfo.XML, JDBCSQLTypeInfo.CLOB});
        			return booleanFuture(true);
        		}
        		if (show.equalsIgnoreCase("ANNOTATIONS")) { //$NON-NLS-1$
        			List<ArrayList<Object>> records = new ArrayList<ArrayList<Object>>(1);
        			Collection<Annotation> annos = driverConnection.getAnnotations();
        			for (Annotation annotation : annos) {
        				ArrayList<Object> row = new ArrayList<Object>(4);
            			row.add(annotation.getCategory());
            			row.add(annotation.getPriority().name());
            			row.add(annotation.getAnnotation());
            			row.add(annotation.getResolution());
        				records.add(row);
        			}
        			createResultSet(records, new String[] {"CATEGORY", "PRIORITY", "ANNOTATION", "RESOLUTION"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        					new String[] {JDBCSQLTypeInfo.STRING, JDBCSQLTypeInfo.STRING, JDBCSQLTypeInfo.STRING, JDBCSQLTypeInfo.STRING});
        			return booleanFuture(true);
        		}
        		if (show.equalsIgnoreCase("ALL")) { //$NON-NLS-1$
        			List<ArrayList<Object>> records = new ArrayList<ArrayList<Object>>(1);
        			for (String key : driverConnection.getExecutionProperties().stringPropertyNames()) {
        				ArrayList<Object> row = new ArrayList<Object>(4);
            			row.add(key);
            			row.add(driverConnection.getExecutionProperties().get(key));
        				records.add(row);
        			}
        			createResultSet(records, new String[] {"NAME", "VALUE"}, //$NON-NLS-1$ //$NON-NLS-2$
        					new String[] {JDBCSQLTypeInfo.STRING, JDBCSQLTypeInfo.STRING});
        			return booleanFuture(true);
        		}
        		List<List<String>> records = Collections.singletonList(Collections.singletonList(driverConnection.getExecutionProperties().getProperty(JDBCURL.getValidKey(show))));
    			createResultSet(records, new String[] {show}, new String[] {JDBCSQLTypeInfo.STRING});
        		return booleanFuture(true);
        	}
        }
        
        final RequestMessage reqMessage = createRequestMessage(commands, isBatchedCommand, resultsMode);
        reqMessage.setSync(synch);
    	ResultsFuture<ResultsMessage> pendingResult = execute(reqMessage, synch);
    	final ResultsFuture<Boolean> result = new ResultsFuture<Boolean>();
    	pendingResult.addCompletionListener(new ResultsFuture.CompletionListener<ResultsMessage>() {
    		@Override
    		public void onCompletion(ResultsFuture<ResultsMessage> future) {
    			try {
					postReceiveResults(reqMessage, future.get());
					result.getResultsReceiver().receiveResults(hasResultSet());
				} catch (Throwable t) {
					result.getResultsReceiver().exceptionOccurred(t);
				}
    		}
		});
    	if (synch) {
    		try {
    			if (queryTimeoutMS > 0) {
    				pendingResult.get(queryTimeoutMS, TimeUnit.MILLISECONDS);
    			} else {
    				pendingResult.get();
    			}
    			result.get(); //throw an exception if needed
    			return result;
    		} catch (ExecutionException e) {
    			if (e.getCause() instanceof SQLException) {
    				throw (SQLException)e.getCause();
    			}
    			throw TeiidSQLException.create(e);
    		} catch (InterruptedException e) {
    			timeoutOccurred();
    		} catch (TimeoutException e) {
    			timeoutOccurred();
			}
    		throw new TeiidSQLException(JDBCPlugin.Util.getString("MMStatement.Timeout_before_complete")); //$NON-NLS-1$
    	}
    	return result;
    }

	private ResultsFuture<ResultsMessage> execute(final RequestMessage reqMsg, boolean synch) throws SQLException,
			TeiidSQLException {
		this.getConnection().beginLocalTxnIfNeeded();
        this.currentRequestID = this.driverConnection.nextRequestID();
        // Create a request message
        reqMsg.setExecutionPayload(this.payload);        
        reqMsg.setCursorType(this.resultSetType);
        reqMsg.setFetchSize(this.fetchSize);
        reqMsg.setRowLimit(this.maxRows);
        reqMsg.setTransactionIsolation(this.driverConnection.getTransactionIsolation());

        // Get connection properties and set them onto request message
        copyPropertiesToRequest(reqMsg);

        reqMsg.setExecutionId(this.currentRequestID);
        
        ResultsFuture.CompletionListener<ResultsMessage> compeletionListener = null;
		if (queryTimeoutMS > 0 && !synch) {
			final CancelTask c = new QueryTimeoutCancelTask(queryTimeoutMS, this);
			cancellationTimer.add(c);
			compeletionListener = new ResultsFuture.CompletionListener<ResultsMessage>() {
				@Override
				public void onCompletion(ResultsFuture<ResultsMessage> future) {
					cancellationTimer.remove(c);
				}
			};
		} 
        
    	ResultsFuture<ResultsMessage> pendingResult = null;
		try {
			pendingResult = this.getDQP().executeRequest(this.currentRequestID, reqMsg);
		} catch (TeiidException e) {
			throw TeiidSQLException.create(e);
		}
		if (compeletionListener != null) {
			pendingResult.addCompletionListener(compeletionListener);
		}
    	return pendingResult;
    }

	public static ResultsFuture<Boolean> booleanFuture(boolean isTrue) {
		ResultsFuture<Boolean> rs = new ResultsFuture<Boolean>();
		rs.getResultsReceiver().receiveResults(isTrue);
		return rs;
	}
	
	private void postReceiveResults(RequestMessage reqMessage,
			ResultsMessage resultsMsg) throws TeiidSQLException, SQLException {
		commandStatus = State.DONE;
		// warnings thrown
        List resultsWarning = resultsMsg.getWarnings();

        setAnalysisInfo(resultsMsg);

        if (resultsMsg.getException() != null) {
            throw TeiidSQLException.create(resultsMsg.getException());
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
            if (logger.isLoggable(Level.FINER)) {
            	logger.fine(JDBCPlugin.Util.getString("Recieved update counts: " + Arrays.toString(updateCounts))); //$NON-NLS-1$
            }
            // In update scenarios close the statement implicitly
            try {
				getDQP().closeRequest(getCurrentRequestID());
			} catch (TeiidProcessingException e) {
				throw TeiidSQLException.create(e);
			} catch (TeiidComponentException e) {
				throw TeiidSQLException.create(e);
			}            
        } else {
            createResultSet(resultsMsg);
        }
        
        logger.fine(JDBCPlugin.Util.getString("MMStatement.Success_query", reqMessage.getCommandString())); //$NON-NLS-1$
	}

	protected RequestMessage createRequestMessage(String[] commands,
			boolean isBatchedCommand, ResultsMode resultsMode) {
        RequestMessage reqMessage = new RequestMessage();
    	reqMessage.setCommands(commands);
    	reqMessage.setBatchedUpdate(isBatchedCommand);
    	reqMessage.setResultsMode(resultsMode);
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
        return (int)this.queryTimeoutMS/1000;
    }

    /**
     * Returns a ResultSet object that was produced by a call to the method execute.
     * We currently do not support execute method which could return multiple result
     * sets.
     * @return ResultSet object giving the next available ResultSet
     * @throws SQLException should never occur
     */
    public ResultSetImpl getResultSet() throws SQLException {
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

    public void setFetchDirection(int direction) throws SQLException {
        checkStatement();
    }

    public void setFetchSize(int rows) throws SQLException {
        //Check to see the statement is closed and throw an exception
        checkStatement();
        if ( rows < 0 ) {
            String msg = JDBCPlugin.Util.getString("MMStatement.Invalid_fetch_size"); //$NON-NLS-1$
            throw new TeiidSQLException(msg);
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
            queryTimeoutMS = seconds*1000;
        }
        else {
            throw new TeiidSQLException(JDBCPlugin.Util.getString("MMStatement.Bad_timeout_value")); //$NON-NLS-1$
        }
    }
    
    void setQueryTimeoutMS(int queryTimeoutMS) {
		this.queryTimeoutMS = queryTimeoutMS;
	}

    /**
     * Helper method for copy the connection properties to request message.
     * @param res Request message that these properties to be copied to.
     * @throws TeiidSQLException 
     */
    protected void copyPropertiesToRequest(RequestMessage res) throws TeiidSQLException {
        // Get partial mode
        String partial = getExecutionProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE);
        res.setPartialResults(Boolean.valueOf(partial).booleanValue());

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
		} catch (TeiidProcessingException e) {
			throw TeiidSQLException.create(e);
		}
        
        // Get result set cache mode
        String rsCache = getExecutionProperty(ExecutionProperties.RESULT_SET_CACHE_MODE);
        res.setUseResultSetCache(Boolean.valueOf(rsCache).booleanValue());
        
        res.setAnsiQuotedIdentifiers(Boolean.valueOf(
                getExecutionProperty(ExecutionProperties.ANSI_QUOTED_IDENTIFIERS))
                .booleanValue());
        String showPlan = getExecutionProperty(ExecutionProperties.SQL_OPTION_SHOWPLAN);
        if (showPlan != null) {
        	try {
        		res.setShowPlan(ShowPlan.valueOf(showPlan.toUpperCase()));
        	} catch (IllegalArgumentException e) {
        		
        	}
        }
        String noExec = getExecutionProperty(ExecutionProperties.NOEXEC);
        if (noExec != null) {
    		res.setNoExec(noExec.equalsIgnoreCase("ON")); //$NON-NLS-1$
        }
    }

    /**
     * Ends the command and sets the status to TIMED_OUT.
     */
    protected void timeoutOccurred() {
    	if (this.commandStatus != State.RUNNING) {
    		return;
    	}
        logger.warning(JDBCPlugin.Util.getString("MMStatement.Timeout_ocurred_in_Statement.")); //$NON-NLS-1$
        try {
        	cancel();        
            commandStatus = State.TIMED_OUT;
            queryTimeoutMS = NO_TIMEOUT;
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
		} catch (TeiidProcessingException e) {
			throw TeiidSQLException.create(e);
		} catch (TeiidComponentException e) {
			throw TeiidSQLException.create(e);
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
        this.execProps.setProperty(name, value);
    }

    public String getExecutionProperty(String name) {
        return this.execProps.getProperty(name);
    }

    long getCurrentRequestID() {
        return this.currentRequestID;
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
        if(this.resultSet != null) {
			return this.resultSet.getUpdatedPlanDescription();
        }
        if(currentPlanDescription != null) {
            return this.currentPlanDescription;
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
    public Collection<Annotation> getAnnotations() {
        return this.annotations;
    }
    
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
        this.annotations = resultsMsg.getAnnotations(); 
        this.driverConnection.setDebugLog(debugLog);
        this.driverConnection.setCurrentPlanDescription(currentPlanDescription);
        this.driverConnection.setAnnotations(annotations);
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
	
	public ConnectionImpl getConnection() throws SQLException {
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
            throw new TeiidSQLException(JDBCPlugin.Util.getString("MMStatement.Invalid_field_size", max)); //$NON-NLS-1$
        }
		this.maxFieldSize = max;
	}
	
	ResultSetImpl createResultSet(List records, String[] columnNames, String[] dataTypes) throws SQLException {
        Map[] metadata = new Map[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            metadata[i] = getColumnMetadata(null, columnNames[i], dataTypes[i], ResultsMetadataConstants.NULL_TYPES.UNKNOWN, driverConnection);
        }
		return createResultSet(records, metadata);
	}
	
    ResultSetImpl createResultSet(List records, Map[] columnMetadata) throws SQLException {
        ResultSetMetaData rsmd = new ResultSetMetaDataImpl(new MetadataProvider(columnMetadata));

        return createResultSet(records, rsmd);
    }

    ResultSetImpl createResultSet(List records, ResultSetMetaData rsmd) throws SQLException {
    	rsmd.getScale(1); //force the load of the metadata
        ResultsMessage resultsMsg = createDummyResultsMessage(null, null, records);
		resultSet = new ResultSetImpl(resultsMsg, this, rsmd, 0);
		resultSet.setMaxFieldSize(this.maxFieldSize);
        return resultSet;
    }

    static ResultsMessage createDummyResultsMessage(String[] columnNames, String[] dataTypes, List records) {
        ResultsMessage resultsMsg = new ResultsMessage();
        resultsMsg.setColumnNames(columnNames);
        resultsMsg.setDataTypes(dataTypes);
        resultsMsg.setFirstRow(1);
        resultsMsg.setLastRow(records.size());
        resultsMsg.setFinalRow(records.size());
        resultsMsg.setResults((List[])records.toArray(new List[records.size()]));
        return resultsMsg;
    }
    
    static Map<Integer, Object> getColumnMetadata(String tableName, String columnName, String dataType, Integer nullable, ConnectionImpl driverConnection) throws SQLException {
        return getColumnMetadata(tableName, columnName, dataType, nullable, ResultsMetadataConstants.SEARCH_TYPES.UNSEARCHABLE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, driverConnection);
	}
	
	static Map<Integer, Object> getColumnMetadata(String tableName, String columnName, String dataType, Integer nullable, Integer searchable, Boolean writable, Boolean signed, Boolean caseSensitive, ConnectionImpl driverConnection) throws SQLException {
	
	    // map that would contain metadata details
	    Map<Integer, Object> metadataMap = new HashMap<Integer, Object>();
	
	    /*******************************************************
	     HardCoding Column metadata details for the given column
	    ********************************************************/
	
	    metadataMap.put(ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, driverConnection.getVDBName());
	    metadataMap.put(ResultsMetadataConstants.GROUP_NAME, tableName);
	    metadataMap.put(ResultsMetadataConstants.ELEMENT_NAME, columnName);
	    metadataMap.put(ResultsMetadataConstants.DATA_TYPE, dataType);
	    metadataMap.put(ResultsMetadataConstants.PRECISION, ResultsMetadataDefaults.getDefaultPrecision(dataType));
	    metadataMap.put(ResultsMetadataConstants.RADIX, new Integer(10));
	    metadataMap.put(ResultsMetadataConstants.SCALE, new Integer(0));
	    metadataMap.put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.FALSE);
	    metadataMap.put(ResultsMetadataConstants.CASE_SENSITIVE, caseSensitive);
	    metadataMap.put(ResultsMetadataConstants.NULLABLE, nullable);
	    metadataMap.put(ResultsMetadataConstants.SEARCHABLE, searchable);
	    metadataMap.put(ResultsMetadataConstants.SIGNED, signed);
	    metadataMap.put(ResultsMetadataConstants.WRITABLE, writable);
	    metadataMap.put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
	    metadataMap.put(ResultsMetadataConstants.DISPLAY_SIZE, ResultsMetadataDefaults.getMaxDisplaySize(dataType));
	
	    return metadataMap;
	}
}