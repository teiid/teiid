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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.types.BlobType;
import com.metamatrix.common.types.ClobType;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.common.util.SqlUtil;
import com.metamatrix.common.util.TimestampWithTimezone;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.client.impl.StreamingLobChunckProducer;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.jdbc.BatchResults.Batch;

/**
 * <p>
 * The MMResultSet is the way query results are returned to the requesting
 * client based upon a query given to the server. This abstract class that
 * implements java.sql.ResultSet. This class represents access to results
 * produced by any of the classes on the driver.
 * </p>
 */

public class MMResultSet extends WrapperImpl implements com.metamatrix.jdbc.api.ResultSet, BatchFetcher {
	private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$

	private static final int BEFORE_FIRST_ROW = 0;

	// the object which was last read from Results
	private Object currentValue;

	// This object represents metadata for this result set.
	private com.metamatrix.jdbc.api.ResultSetMetaData rmetadata;
	// Statement that causes this results
	private MMStatement statement;

	// This object represents the time when command is submitted to the server.
	private java.util.Date processingTimestamp;
	// This object represents the time when results are produced on the server.
	private java.util.Date completedTimestamp;

    // Cursor related state
    private int cursorType;
    private boolean isClosed;

    // reuse the original request's state
    private long requestID;
    private BatchResults batchResults;
    private int columnCount;
    private int resultColumns;
    private int parameters;
    private TimeZone serverTimeZone;
    private Map updatedPlanDescription;
    private ResultsMessage resultsMsg;
    private int maxFieldSize;

	/**
	 * Constructor.
	 * 
	 * @param resultsMsg
	 * @param statement
	 * @throws SQLException
	 */
	MMResultSet(ResultsMessage resultsMsg, MMStatement statement) throws SQLException {
		this(resultsMsg, statement, null, 0);
	}

	MMResultSet(ResultsMessage resultsMsg, MMStatement statement,
			com.metamatrix.jdbc.api.ResultSetMetaData metadata, int parameters) throws SQLException {
		this.resultsMsg = resultsMsg;
		this.statement = statement;
		this.parameters = parameters;
		// server latency-related timestamp
		this.processingTimestamp = resultsMsg.getProcessingTimestamp();
        this.requestID = statement.getCurrentRequestID();
        this.batchResults = new BatchResults(this, resultsMsg.getFetchSize(), getCurrentBatch(resultsMsg));
        setResultsData(resultsMsg);
        cursorType = statement.getResultSetType();
        this.serverTimeZone = statement.getServerTimeZone();

		if (metadata == null) {
			ResultsMetadataProvider provider = DeferredMetadataProvider.createWithInitialData(resultsMsg.getColumnNames(),
							resultsMsg.getDataTypes(), statement,
							statement.getCurrentRequestID());
			rmetadata = ResultsMetadataWithProvider.newInstance(provider);
		} else {
			rmetadata = metadata;
		}
        // Cache the column count and isLOB values since every call to getObject uses these.
		this.columnCount = rmetadata.getColumnCount();
		
		this.resultColumns = columnCount - parameters;
		if (this.parameters > 0) {
			rmetadata = FilteredResultsMetadata.newInstance(rmetadata, resultColumns);
		}
	}
	
	public void setMaxFieldSize(int maxFieldSize) {
		this.maxFieldSize = maxFieldSize;
	}
	
    /**
     * Close this result set.
     */
    public void close() throws SQLException{
    	if(!isClosed) {
            // close the the server's statement object (if necessary)
    		if(this.requestID >= 0){
	            try {
					this.statement.getDQP().closeRequest(requestID);
				} catch (MetaMatrixProcessingException e) {
					throw MMSQLException.create(e);
				} catch (MetaMatrixComponentException e) {
					throw MMSQLException.create(e);
				}
    		}
            isClosed = true;
        }
    }
    
	public boolean isClosed() throws SQLException {
		return isClosed;
	}
    
    protected void checkClosed() throws SQLException {
    	if (isClosed) {
    		String msg = JDBCPlugin.Util
    				.getString("MMResultSet.Cant_call_closed_resultset"); //$NON-NLS-1$
    		throw new MMSQLException(msg);
    	}
    }
    
    /**
     * Return a stored procedures parameters (may be null)
     * @return List of stored procedure parameters which are set from the response
     */
     public List getParameters(){
         return this.resultsMsg.getParameters();
     }
     
     /**
      * Return the value for output/return parameter given the index
      * of the parameter in the ResultSet
      * @param index Index of the parameter to be retrieved.
      */
     Object getOutputParamValue(int index) throws SQLException {
         if (index <= resultColumns || index > resultColumns + parameters) {
             throw new MMSQLException(JDBCPlugin.Util.getString("StoredProcedureResultsImpl.Invalid_parameter_index__{0}_2", index)); //$NON-NLS-1$
         }
         // Mark the row we're on
         final int originalRow = getAbsoluteRowNumber();
         
         this.batchResults.absolute(-1);
         try {
         	return getObjectDirect(index);
         } finally {
         	this.batchResults.absolute(originalRow);
         }
     }
     
     /**
      * <p>Get a java object based on the column index for the current row.</p>
      * @param The index of the column whose value needs to be fetched.
      * @return The value of the column as an object.
      * @throws SQLException if a results access error occurs or transform fails.
      */
     public Object getObject(int column) throws SQLException {
         if (isAfterLast()) {
             throw new MMSQLException(JDBCPlugin.Util.getString("StoredProcedureResultsImpl.ResultSet_cursor_is_after_the_last_row._1")); //$NON-NLS-1$
         }
         // only get the Object of the result set
         if(column > resultColumns){
             throw new MMSQLException(JDBCPlugin.Util.getString("ResultsImpl.Invalid_col_index", column)); //$NON-NLS-1$
         }
         return getObjectDirect(column);
     }

    /**
     * Get fetch size that will be used if this result set is backed
     * by a cursor.  This may be the hint fetch size provided by
     * {@link com.metamatrix.jdbc.ResultsImpl#getFetchSize} or it may
     * have been reset by the server.
     */
    public int getFetchSize() throws SQLException {
        return this.batchResults.getFetchSize();
    }

    /**
     * Move row pointer forward one row.  This may cause the cursor
     * to fetch more rows.
     * @return True if the current index is on a valid row, false if
     * the pointer is past the end of the rows
     * @throws SQLException if this result set has an exception
     */
    public boolean next() throws SQLException {
        checkClosed();
        if (hasNext()) {
    		return batchResults.next();
    	}
        batchResults.next();
    	return false;
    }

    /**
     * Move row pointer backward one row.  This may cause the cursor
     * to fetch more rows.
     * @return True if the current index is on a valid row, false if
     * the pointer is before the beginning of the rows
     * @throws SQLException if this result set has an exception
     */
    public boolean previous() throws SQLException {
        checkClosed();
        checkNotForwardOnly();
        return batchResults.previous();
    }

    /**
     * Get current row pointer.
     * @return Index of current row
     * @throws SQLException if this result set has an exception
     */
    public int getRow() throws SQLException {
        checkClosed();
        if (isAfterLast()) {
        	return 0;
        }
        return getAbsoluteRowNumber();
    }

    /**
     * Get the value of the current row at the column index specified.
     * @param column Column index
     * @return Value at column, which may be null
     * @throws SQLException if this result set has an exception
     */
    public Object getObjectDirect(int column) throws SQLException {
    	checkClosed();
    	if(column < 1 || column > columnCount) {
            throw new IllegalArgumentException(JDBCPlugin.Util.getString("ResultsImpl.Invalid_col_index", column)); //$NON-NLS-1$
        }
        List cursorRow = batchResults.getCurrentRow();
        
        if (cursorRow == null) {
            throw new MMSQLException(JDBCPlugin.Util.getString("ResultsImpl.The_cursor_is_not_on_a_valid_row._1")); //$NON-NLS-1$
        }

        // defect 13539 - set the currentValue (defined in MMResultSet) so that wasNull() accurately returns whether this value was null
        currentValue = cursorRow.get(column-1);
                
        if(currentValue instanceof ClobType){
            currentValue = MMClob.newInstance(new StreamingLobChunckProducer.Factory(this.statement.getDQP(), this.requestID, (Streamable)currentValue), (ClobType) currentValue);
        }
        else if (currentValue instanceof BlobType) {
            currentValue = MMBlob.newInstance(new StreamingLobChunckProducer.Factory(this.statement.getDQP(), this.requestID, (Streamable)currentValue), (BlobType) currentValue);
        }
        else if (currentValue instanceof XMLType) {
            currentValue = MMSQLXML.newInstance(new StreamingLobChunckProducer.Factory(this.statement.getDQP(), this.requestID, (Streamable)currentValue), (XMLType) currentValue);
        } 
        else if (currentValue instanceof java.util.Date) {
            return TimestampWithTimezone.create((java.util.Date)currentValue, serverTimeZone, getDefaultCalendar(), currentValue.getClass());
        }
        else if (maxFieldSize > 0 && currentValue instanceof String) {
        	String val = (String)currentValue;
        	currentValue = val.substring(0, Math.min(maxFieldSize/2, val.length()));
        }
        return currentValue;
    }

    /**
     * Get all values in current record in column order
     * @return List of Object values in current row
     * @throws SQLException if an access error occurs.
     */
    public List getCurrentRecord() throws SQLException {
    	checkClosed();
        List wholeRecord = batchResults.getCurrentRow();
        return wholeRecord.subList(0, wholeRecord.size() - getOffset());
    }
    /*
     * @see java.sql.ResultSet#getType()
     */
    public int getType() throws SQLException {
        return this.cursorType;
    }

    public boolean absolute( int row) throws SQLException {
        checkClosed();
        checkNotForwardOnly();
        return batchResults.absolute(row, getOffset());
    }
    
    protected Map getUpdatedPlanDescription() {
    	return updatedPlanDescription;
    }
    
    public Batch requestBatch(int beginRow, int endRow) throws SQLException{
    	logger.fine("CursorResultsImpl.requestBatch] thread name: " + Thread.currentThread().getName() + " requestID: " + requestID + " beginRow: " + beginRow + " endinRow: " + endRow ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    	checkClosed();
        try {
        	ResultsFuture<ResultsMessage> results = statement.getDQP().processCursorRequest(requestID, beginRow, endRow);
        	ResultsMessage currentResultMsg = results.get();
    		this.setResultsData(currentResultMsg);
    		this.updatedPlanDescription = currentResultMsg.getPlanDescription();
    		return getCurrentBatch(currentResultMsg);
        } catch (MetaMatrixProcessingException e) {
			throw MMSQLException.create(e);
		} catch (InterruptedException e) {
			throw MMSQLException.create(e);
		} catch (ExecutionException e) {
			throw MMSQLException.create(e);
		}
    }

	private Batch getCurrentBatch(ResultsMessage currentResultMsg) {
		boolean isLast = currentResultMsg.getResults().length == 0 || currentResultMsg.getFinalRow() == currentResultMsg.getLastRow();
		return new Batch(currentResultMsg.getResults(), currentResultMsg.getFirstRow(), currentResultMsg.getLastRow(), isLast);
	}
    
	protected int getFinalRowNumber() {
    	return Math.max(-1, batchResults.getFinalRowNumber() - getOffset());
	}

	protected boolean hasNext() throws SQLException {
		return batchResults.hasNext(getOffset() + 1);
	}
	
	protected int getOffset() {
		return parameters;
	}

	protected int getAbsoluteRowNumber() {
		return batchResults.getCurrentRowNumber();
	}

	/**
	 * Cancels the updates made to a row. Since resultSet row updates are not
	 * supported, calling this method should not cause any effect. *
	 * 
	 * @throws SQLException
	 * 		if a metamatrix access error occurs.
	 */
	public void cancelRowUpdates() throws SQLException {
		// do nothing.
		checkClosed(); // check to see if the ResultSet is closed
	}

	//## JDBC4.0-begin ##
	@Override
	//## JDBC4.0-end ##
	public void clearWarnings() throws SQLException {
		// do nothing
		checkClosed(); // check to see if the ResultSet is closed
	}

	/**
	 * This method will return the value in the current at the given column
	 * index row as a java.math.BigDecimal.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a BigDecimal object.
	 * @throws SQLException
	 * 		if there is an error accessing metamatrix results.
	 */
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return DataTypeTransformer.getBigDecimal(getObject(columnIndex));
	}

	/**
	 * This method will return the value in the current row at the given column
	 * as a java.math.BigDecimal.
	 * 
	 * @param The
	 * 		name of the column whose value needs to be fetched in the current
	 * 		row.
	 * @return The value of the column as a BigDecimal object.
	 * @throws SQLException
	 * 		if there is an error accessing metamatrix results.
	 */
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getBigDecimal(findColumn(columnName));
	}

	/**
	 * Gets the value of a column in the current row as a java.math.BigDecimal
	 * object.
	 * 
	 * @param columnIndex
	 * 		the first column is 1, the second is 2, ...
	 * @param scale
	 * 		the number of digits to the right of the decimal
	 * @return the column value; if the value is SQL NULL, the result is null
	 * @throws SQLException
	 * 		if there is an error accessing metamatrix results.
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {

		// do the necessary transformation depending on the datatype of the
		// object at the given index.
		BigDecimal bigDecimalObject = DataTypeTransformer
				.getBigDecimal(getObject(columnIndex));
		
		if (bigDecimalObject == null) {
			return null;
		}

		// set the scale on the bigDecimal
		return bigDecimalObject.setScale(scale);
	}

	/**
	 * Gets the value of a column in the current row as a java.math.BigDecimal
	 * object.
	 * 
	 * @param columnName
	 * 		the SQL name of the column
	 * @param scale
	 * 		the number of digits to the right of the decimal
	 * @return the column value; if the value is SQL NULL, the result is null
	 * @throws SQLException
	 * 		if a metamatrix access error occurs
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(String columnName, int scale)
			throws SQLException {
		// find the columnIndex for the given column name.
		return getBigDecimal(findColumn(columnName), scale);
	}

	/**
	 * This method will read in the data byte by byte and write it to an
	 * InputStream to be returned.
	 * 
	 * @param columnIndex
	 * 		The column position in the current row whose value is to be read.
	 * @return An InputStream object that delivers the column value as a binary
	 * 	stream.
	 */
	public java.io.InputStream getBinaryStream(int columnIndex)
			throws SQLException {
		Object value = getObject(columnIndex);
		if (value == null) {
			return null;
		}

		if (value instanceof Blob) {
			return ((Blob) value).getBinaryStream();
		}

		throw new MMSQLException(JDBCPlugin.Util.getString("MMResultSet.cannot_convert_to_binary_stream")); //$NON-NLS-1$
	}

	/**
	 * This method will read in the data byte by byte and write it to an
	 * InputStream to be returned.
	 * 
	 * @param columnName
	 * 		The column name in the current row whose value is to be updated.
	 * @return An InputStream object that delivers the column value as a binary
	 * 	stream.
	 */
	public java.io.InputStream getBinaryStream(String columnName)
			throws SQLException {
		return getBinaryStream(findColumn(columnName));
	}

	/**
	 * This method will read in a SQL BLOB value stored in the given column as a
	 * Blob object.
	 * 
	 * @param columnIndex
	 * 		The column position in the current row whose value is to be read.
	 * @return The value of the column at columnIndex as a Blob object.
	 * @throws SQLException
	 * 		if there is an error accessing metamatrix results.
	 */
	public Blob getBlob(int columnIndex) throws SQLException {
		return DataTypeTransformer.getBlob(getObject(columnIndex));
	}

	/**
	 * This method will read in a SQL BLOB value stored in the given column as a
	 * Blob object.
	 * 
	 * @param columnName
	 * 		The column name in the current row whose value is to be updated.
	 * @return The value of the column at columnIndex as a Blob object.
	 * @throws SQLException
	 * 		if there is an error accessing metamatrix results.
	 */
	public Blob getBlob(String columnName) throws SQLException {
		return getBlob(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as a boolean value
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a boolean value.
	 * @throws SQLException
	 * 		if there is an error accessing metamatrix results.
	 */
	public boolean getBoolean(int columnIndex) throws SQLException {
		return DataTypeTransformer.getBoolean(getObject(columnIndex));
	}

	/**
	 * This method will return the value in the current row at the given column
	 * as a boolean value.
	 * 
	 * @param The
	 * 		name of the column whose value needs to be fetched in the current
	 * 		row.
	 * @return The value of the column as a boolean value.
	 * @throws SQLException
	 * 		if there is an error accessing metamatrix results.
	 */
	public boolean getBoolean(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getBoolean(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as a byte value.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a byte value.
	 * @throws SQLException
	 * 		if there is an error accessing metamatrix results.
	 */
	public byte getByte(int columnIndex) throws SQLException {
		return DataTypeTransformer.getByte(getObject(columnIndex));
	}

	/**
	 * This method will return the value in the current row as a byte value
	 * 
	 * @param name
	 * 		of the column whose byte value is to be returned.
	 * @return value of the column as a byte.
	 * @throw a SQLException if a resultSet access error occurs.
	 */
	public byte getByte(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getByte(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as an array of byte
	 * values
	 * 
	 * @param columnIndex
	 * 		The column position in the current row whose value is to be read.
	 * @return The value of the column at columnIndex as an array of bytes.
	 * @throws SQLException
	 * 		if there is an error accessing or converting the result value
	 */
	public byte[] getBytes(int columnIndex) throws SQLException {
		return DataTypeTransformer.getBytes(getObject(columnIndex));
	}

	/**
	 * This method will return the value in the current row as an array of byte
	 * values
	 * 
	 * @param columnName
	 * 		The column name in the current row whose value is to be updated.
	 * @return byte[]. The value of the column at columnIndex as an array of
	 * 	bytes.
	 * @throws SQLException
	 * 		if there is an error accessing or converting the result value
	 */
	public byte[] getBytes(String columnName) throws SQLException {
		return getBytes(findColumn(columnName));
	}

	/**
	 * Get the concurrency type for this ResultSet object. The concurrency was
	 * set by the Statement object. The possible concurrency types are
	 * CONCUR_READ_ONLY and CONCUR_UPDATABLE.
	 * 
	 * @return The resultSets are not updatable, this method returns
	 * 	CONCUR_READ_ONLY.
	 * @throws SQLException
	 * 		if the there is an error accesing results
	 */
	public int getConcurrency() throws SQLException {

		checkClosed(); // check to see if the ResultSet is closed
		return ResultSet.CONCUR_READ_ONLY;
	}

	/**
	 * This method will attempt to return the value contained at the index as a
	 * java.io.Reader object.
	 * 
	 * @param columnIndex
	 * 		The column position in the current row whose value is to be read.
	 * @return The value of the column as a java.io.Reader object.
	 */
	public java.io.Reader getCharacterStream(int columnIndex)
			throws SQLException {
		Object value = getObject(columnIndex);
		if (value == null) {
			return null;
		}

		if (value instanceof Clob) {
			return ((Clob) value).getCharacterStream();
		}

		throw new MMSQLException(JDBCPlugin.Util.getString("MMResultSet.cannot_convert_to_character_stream")); //$NON-NLS-1$
	}

	/**
	 * This method will attempt to return the value at the designated column
	 * determined by the columName as a java.io.Reader object.
	 * 
	 * @param columnName
	 * 		The column name in the current row whose value is to be updated.
	 * @return The value of the column as a java.io.Reader object.
	 */
	public java.io.Reader getCharacterStream(String columnName)
			throws SQLException {
		return getCharacterStream(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as a Date object.
	 * This will use the timeZone info of the calendar object.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @param Calender
	 * 		object used to get the date value.
	 * @return The value of the column as a Date object.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		Date value = DataTypeTransformer.getDate(getObject(columnIndex));

		if (value != null && cal != null) {
			value = TimestampWithTimezone.createDate(value,
					getDefaultCalendar().getTimeZone(), cal);
		}

		return value;
	}

	/**
	 * Get the Date value for the given column.
	 * 
	 * @param columnName
	 * 		. The name of the column whose value needs to be fetched.
	 * @param Calender
	 * 		object used to get the date value.
	 * @return value of the column as an int.
	 * @throws SQLException
	 * 		unable to obtain date value for the given column.
	 */
	public Date getDate(String columnName, Calendar cal) throws SQLException {
		// find the columnIndex for the given column name.
		return getDate(findColumn(columnName), cal);
	}

	/**
	 * This method will return the value in the current row as a Date object.
	 * This will assume the default timeZone.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a Date object.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public Date getDate(int columnIndex) throws SQLException {
		return getDate(columnIndex, null);
	}

	/**
	 * Get the column value as a Date object
	 * 
	 * @param name
	 * 		of the column in the resultset whose value is to be fetched.
	 * @return value of the column as an int.
	 * @throw a SQLException if a resultSet access error occurs.
	 */
	public Date getDate(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getDate(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as a double value.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a double value.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public double getDouble(int columnIndex) throws SQLException {
		return DataTypeTransformer.getDouble(getObject(columnIndex));
	}

	/**
	 * Get a double value based on the column name.
	 * 
	 * @param name
	 * 		of the column in the resultset whose value is to be fetched.
	 * @return value of the column as a double.
	 * @throw a SQLException if a resultSet access error occurs.
	 */
	public double getDouble(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getDouble(findColumn(columnName));
	}

	/**
	 * Gets the direction suggested to the driver as the direction in which to
	 * fetch rows.
	 * 
	 * @return fetch direction for this ResultSet. This cannot be set and is
	 * 	alwayd FETCH_FORWARD.
	 * @throws SQLException
	 */
	public int getFetchDirection() throws SQLException {
		checkClosed(); // check to see if the ResultSet is closed
		return ResultSet.FETCH_FORWARD;
	}

	/**
	 * This method will return the value in the current row as a float value.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a float value.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public float getFloat(int columnIndex) throws SQLException {
		return DataTypeTransformer.getFloat(getObject(columnIndex));
	}

	/**
	 * Get a float value based on the column name.
	 * 
	 * @param name
	 * 		of the column in the resultset whose value is to be fetched.
	 * @return value of the column as a float.
	 * @throw a SQLException if a resultSet access error occurs.
	 */
	public float getFloat(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getFloat(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as a int value.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a int value.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public int getInt(int columnIndex) throws SQLException {
		return DataTypeTransformer.getInteger(getObject(columnIndex));
	}

	/**
	 * Get an integer based on the column index.
	 * 
	 * @param name
	 * 		of the column in the resultset whose value is to be fetched.
	 * @return value of the column as an int.
	 * @throw a SQLException if a resultSet access error occurs.
	 */
	public int getInt(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getInt(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as a long value.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a long value.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public long getLong(int columnIndex) throws SQLException {
		return DataTypeTransformer.getLong(getObject(columnIndex));
	}

	/**
	 * Get a long based on the column name.
	 * 
	 * @param name
	 * 		of the column in the resultset whose value is to be fetched.
	 * @return value of the column as a long.
	 * @throw a SQLException if a resultSet access error occurs.
	 */
	public long getLong(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getLong(findColumn(columnName));
	}

	/**
	 * Get a java object based on the column name.
	 * 
	 * @param name
	 * 		of the column in the resultset whose value is to be fetched.
	 * @return object which gives the column value.
	 * @throw a SQLException if a resultSet access error occurs.
	 */
	public Object getObject(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getObject(findColumn(columnName));
	}

	/**
	 * Get a primitive short based on the column index.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a short value.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public short getShort(int columnIndex) throws SQLException {
		return DataTypeTransformer.getShort(getObject(columnIndex));
	}

	/**
	 * Get a short based on the column name.
	 * 
	 * @param String
	 * 		representing name of the column.
	 * @return short value of the column.
	 * @throws SQLException
	 * 		if a results access error occurs.
	 */
	public short getShort(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getShort(findColumn(columnName));
	}

	/**
	 * Get a String based on the column index.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a string value.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public String getString(int columnIndex) throws SQLException {
		return DataTypeTransformer.getString(getObject(columnIndex));
	}

	/**
	 * Get a string based on the column name.
	 * 
	 * @param String
	 * 		representing name of the column.
	 * @return String value of the column.
	 * @throws SQLException
	 * 		if a results access error occurs.
	 */
	public String getString(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getString(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as a Time object.
	 * This will assume the default timeZone.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a Time object.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public Time getTime(int columnIndex) throws SQLException {
		return getTime(columnIndex, null);
	}

	/**
	 * Get a java.sql.Time based on the column name.
	 * 
	 * @param name
	 * 		of the column whose value is to be fetched as a timestamp
	 * @return value of the column as a Timestamp object
	 * @throws SQLException
	 * 		if a results access error occurs.
	 */
	public Time getTime(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getTime(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as a Time object.
	 * This will use the timeZone info of the calendar object.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @param Calendar
	 * 		object to be used to construct the Time object.
	 * @return The value of the column as a Time object.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		Time value = DataTypeTransformer.getTime(getObject(columnIndex));

		if (value != null && cal != null) {
			value = TimestampWithTimezone.createTime(value,
					getDefaultCalendar().getTimeZone(), cal);
		}

		return value;
	}

	/**
	 * Get a java.sql.Time based on the column name.
	 * 
	 * @param name
	 * 		of the column whose value is to be fetched as a timestamp
	 * @param calender
	 * 		object to include the timezone info in the object returned
	 * @return value of the column as a Timestamp object
	 * @throws SQLException
	 * 		if a results access error occurs.
	 */
	public Time getTime(String columnName, Calendar cal) throws SQLException {
		// find the columnIndex for the given column name.
		return getTime((findColumn(columnName)), cal);
	}

	/**
	 * This method will return the value in the current row as a Timestamp
	 * object. This will assume the default timeZone.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @return The value of the column as a Timestamp object.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return getTimestamp(columnIndex, null);
	}

	/**
	 * Get a java.sql.Timestamp based on the column name.
	 * 
	 * @param name
	 * 		of the column whose value is to be fetched as a timestamp
	 * @return value of the column as a Timestamp object
	 * @throws SQLException
	 * 		if a results access error occurs.
	 */
	public Timestamp getTimestamp(String columnName) throws SQLException {
		// find the columnIndex for the given column name.
		return getTimestamp(findColumn(columnName));
	}

	/**
	 * This method will return the value in the current row as a Timestamp
	 * object. This will use the timeZone info of the calendar object.
	 * 
	 * @param The
	 * 		index of the column whose value needs to be fetched.
	 * @param Calendar
	 * 		object to be used to construct the Timestamp object.
	 * @return The value of the column as a Timestamp object.
	 * @throws SQLException
	 * 		if a results access error occurs or transform fails.
	 */
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		Timestamp value = DataTypeTransformer.getTimestamp(getObject(columnIndex));

		if (value != null && cal != null) {
			value = TimestampWithTimezone.createTimestamp(value,
					getDefaultCalendar().getTimeZone(), cal);
		}

		return value;
	}

	/**
	 * Get a java.sql.Timestamp based on the column name.
	 * 
	 * @param name
	 * 		of the column whose value is to be fetched as a timestamp
	 * @param calender
	 * 		object to include the timezone info in the object returned
	 * @return value of the column as a Timestamp object
	 * @throws SQLException
	 * 		if a results access error occurs.
	 */
	public Timestamp getTimestamp(String columnName, Calendar cal)
			throws SQLException {
		// find the columnIndex for the given column name.
		return getTimestamp(findColumn(columnName), cal);
	}

	/**
	 * Moves the cursor to the remembered cursor position, usually the current
	 * row. This method does not have any effect if the cursor is not on the
	 * insert row. ResultSet cannot currently be updated.
	 */
	public void moveToCurrentRow() throws SQLException {
		// do nothing
	}

	/**
	 * This will return a boolean value if the last column that was read had a
	 * value equal to null. If the last column read was null return true, else
	 * return false.
	 * 
	 * @return A boolean value showing if the lastvalue read in was null or not.
	 * @throws SQLException
	 */
	public boolean wasNull() throws SQLException {
		
		checkClosed(); // check to see if the ResultSet is closed

		return currentValue == null;
	}

	protected void setResultsData(ResultsMessage resultsMsg) {
		this.completedTimestamp = resultsMsg.getCompletedTimestamp();
		this.statement.accumulateWarnings(resultsMsg.getWarnings());
	}

	/**
	 * <p>
	 * This method returns the meta data of the result set, such as the number,
	 * types and properties of this resultSets columns.
	 * </p>
	 * 
	 * @return ResultSerMetaData object for these results.
	 * @throws SQLException
	 * 		if results access error occurs
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		checkClosed();
		return rmetadata;
	}

	/**
	 * Retrieves the RequestID for the query that created this ResultSet.
	 * 
	 * @return The requestID for the query that created these results
	 * @throws SQLException
	 */
	public String getCursorName() throws SQLException {
		return null;
	}

	/**
	 * <p>
	 * Retrieves the Statement object that produced this ResultSet object.
	 * 
	 * @return a Statement object.
	 * 	</p>
	 * @throws SQLException
	 * 		if the there is an error accesing results
	 */
	public MMStatement getStatement() throws SQLException {
		checkClosed();
		return statement;
	}

	/**
	 * Retrieves the first warning reported by calls on this ResultSet object.
	 * Subsequent warnings on this ResultSet object will be chained to the
	 * SQLWarning object that this method returns.
	 * 
	 * @return SQLWarning object representing any MetaMatrix warnings.
	 * @throws SQLException
	 * 		if a database access error occurs or this method is called on a
	 * 		closed result set.
	 */
	public SQLWarning getWarnings() throws SQLException {
		checkClosed();

		return null;
	}

	/**
	 * Gets the time the command was submitted to the server.
	 * 
	 * @return Date object representing time submitted to the server.
	 */
	public java.util.Date getProcessingTimestamp() throws SQLException {
		checkClosed();
		return processingTimestamp;
	}

	/**
	 * Gets the time command execution is compleated on the server.
	 * 
	 * @return Date object representing time the commond finished execution.
	 */
	public java.util.Date getCompletedTimestamp() throws SQLException {
		checkClosed();
		return completedTimestamp;
	}

	/**
	 * This gives the total time taken for execution of the command
	 * 
	 * @return long. Time for command execution on the server.
	 */
	public long getProcessingTime() throws SQLException {
		checkClosed();
		if (completedTimestamp != null && processingTimestamp != null) {
			return completedTimestamp.getTime() - processingTimestamp.getTime();
		}
		return -1;
	}

	/**
	 * True if current record is the first in the result set.
	 * 
	 * @return True if current row is first
	 * @throws QueryResultsException
	 * 		if this result set has an exception
	 * @throws InvalidatedResultsException
	 * 		if the results were obtained during a transaction and the
	 * 		transaction has been rolled back
	 */
	public boolean isFirst() throws SQLException {
		return this.getAbsoluteRowNumber() == BEFORE_FIRST_ROW + 1 && hasNext();
	}

	/**
	 * True if current record is the last in the result set.
	 * 
	 * @return True if current row is last
	 * @throws QueryResultsException
	 * 		if this result set has an exception
	 * @throws InvalidatedResultsException
	 * 		if the results were obtained during a transaction and the
	 * 		transaction has been rolled back
	 */
	public boolean isLast() throws SQLException {
		return !hasNext() && this.getAbsoluteRowNumber() > BEFORE_FIRST_ROW && this.getAbsoluteRowNumber() == getFinalRowNumber();
	}
	
	/**
	 * <p>
	 * Determines whether the cursor is after the last row in this ResultSet
	 * object. This method should be called only if the result set is
	 * scrollable.
	 * </p>
	 * 
	 * @return true if the cursor is after the last row in the resultSet.
	 * @throws SQLException
	 */
	public boolean isAfterLast() throws SQLException {
		// return true if the current row has a next row
		// it is also not the last
		return !hasNext() && this.getAbsoluteRowNumber() > BEFORE_FIRST_ROW && this.getAbsoluteRowNumber() > getFinalRowNumber();
	}

	/**
	 * <p>
	 * Determines whether the cursor is before the first row in this ResultSet
	 * object. This method should be called only if the result set is
	 * scrollable.
	 * </p>
	 * 
	 * @return true if the cursor is before the last row in the resultSet;false
	 * 	if the cursor is at any other position or the result set contains no
	 * 	rows.
	 * @throws SQLException
	 */
	public boolean isBeforeFirst() throws SQLException {
		// return true if there are rows and the current row is before first
		return getAbsoluteRowNumber() == BEFORE_FIRST_ROW && hasNext();
	}

	/**
	 * <p>
	 * Moves the cursor a number of rows relative to the current row in this
	 * ResultSet object. The number of rows may be positive or negative.
	 * </p>
	 * 
	 * @param number
	 * 		of rows to move relative to the present row.
	 * @return true if the cursor is on a valid row in the resultSet.
	 * @throws SQLException
	 * 		if the there is an error accessing results
	 */
	public boolean relative(int rows) throws SQLException {
		if (isBeforeFirst() || isAfterLast() || getFinalRowNumber() == 0) {
			throw new MMSQLException(
					JDBCPlugin.Util
							.getString("ResultsImpl.The_cursor_is_not_on_a_valid_row._1")); //$NON-NLS-1$
		}

		checkNotForwardOnly();
		
    	return this.absolute(Math.max(0, getAbsoluteRowNumber() + rows));
	}
	
	/**
	 * <p>
	 * Moves the cursor to the last row in the in this ResultSet object. This
	 * method should be called only if the result set is scrollable.
	 * </p>
	 * 
	 * @return true if the cursor is on a validRow, false otherwise or if no
	 * 	rows exist.
	 * @throws SQLException
	 * 		if the type of the ResultSet is TYPE_FORWARD_ONLY
	 */
	public boolean last() throws SQLException {
		checkNotForwardOnly();
		
    	return absolute(-1);
	}

	protected void checkNotForwardOnly() throws SQLException {
		if (getType() == ResultSet.TYPE_FORWARD_ONLY) {
			String msg = JDBCPlugin.Util
					.getString("ResultsImpl.Op_invalid_fwd_only"); //$NON-NLS-1$
			throw new MMSQLException(msg);
		}
	}

	/**
	 * <p>
	 * Moves the cursor to the end of the result set, just after the last row.
	 * Has no effect if the result set contains no rows.
	 * </p>
	 * 
	 * @throws SQLException
	 * 		if a results access error occurs or the result set type is
	 * 		TYPE_FORWARD_ONLY
	 */
	public void afterLast() throws SQLException {
		if (last()) {
    		next();
    	}
	}

	/**
	 * <p>
	 * Moves the cursor to the front of the result set, just before the first
	 * row. Has no effect if the result set contains no rows.
	 * </p>
	 * 
	 * @exception SQLException
	 * 		if a results can not be accessed or the result set type is
	 * 		TYPE_FORWARD_ONLY
	 */
	public void beforeFirst() throws SQLException {
		if (first()) {
    		previous();
    	}
	}
	
	/**
	 * <p>
	 * Moves the cursor to the first row in this ResultSet object.
	 * </p>
	 * 
	 * @return true if the cursor is on valid row, false if there are no rows in
	 * 	the resultset.
	 * @throws SQLException
	 * 		if the ResulSet is of TYPE_FORWARD_ONLY.
	 */
	public boolean first() throws SQLException {
		checkNotForwardOnly();
		return absolute(1);
	}

	/**
	 * <p>
	 * This method returns the integer that represents which column represents
	 * the column name parameter.
	 * </p>
	 * 
	 * @param Name
	 * 		of the column whose position is to be returned.
	 * @return Position of column amoung the columns in the ResultSet.
	 * @throws SQLException
	 * 		if a database access error occurs.
	 */
	public int findColumn(String columnName) throws SQLException {
		checkClosed();

		// get the column index using ResultsMetadata object
		return findColumnIndex(columnName);
	}

	protected int findColumnIndex(String columnName) throws SQLException {
		int colCount = getMetaData().getColumnCount();
		for (int i = 1; i <= colCount; i++) {
			if (getMetaData().getColumnName(i).equalsIgnoreCase(columnName)) {
				return i;
			}
		}

		String msg = JDBCPlugin.Util.getString(
				"MMResultsImpl.Col_doesnt_exist", columnName); //$NON-NLS-1$
		throw new MMSQLException(msg);
	}
		
	protected Calendar getDefaultCalendar() {
		return statement.getDefaultCalendar();
	}

	public void deleteRow() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public Array getArray(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public Array getArray(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public Clob getClob(int columnIndex) throws SQLException {
		return DataTypeTransformer.getClob(getObject(columnIndex));
	}
	
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		return DataTypeTransformer.getSQLXML(getObject(columnIndex));
	}

	public Clob getClob(String columnLabel) throws SQLException {
		return getClob(findColumn(columnLabel));
	}

	
	public int getHoldability() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	//## JDBC4.0-begin ##
	public NClob getNClob(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##
	
	public String getNString(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public String getNString(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public Ref getRef(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public Ref getRef(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	//## JDBC4.0-begin ##
	public RowId getRowId(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	//## JDBC4.0-begin ##
	public RowId getRowId(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
	
	public URL getURL(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public URL getURL(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void insertRow() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void moveToInsertRow() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void refreshRow() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public boolean rowDeleted() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public boolean rowInserted() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public boolean rowUpdated() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void setFetchDirection(int direction) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void setFetchSize(int rows) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}


	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
	
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
		throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
	
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
	
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
		throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
	

	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
	
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
	
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}	

	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
	
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Reader reader, long length)
		throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Reader reader)
		throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}		

	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateInt(String columnLabel, int x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateLong(String columnLabel, long x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
		
	}

	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
		
	}

	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
		
	}

	//## JDBC4.0-begin ##
	public void updateNClob(int columnIndex, NClob clob) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	//## JDBC4.0-begin ##
	public void updateNClob(String columnLabel, NClob clob) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateNString(int columnIndex, String string)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateNString(String columnLabel, String string)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateNull(int columnIndex) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateNull(String columnLabel) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateRow() throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	//## JDBC4.0-begin ##
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	//## JDBC4.0-begin ##
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}
	//## JDBC4.0-end ##

	public void updateShort(int columnIndex, short x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateShort(String columnLabel, short x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();
	}

	public void updateString(int columnIndex, String x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateString(String columnLabel, String x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}

	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throw SqlUtil.createFeatureNotSupportedException();	
	}
}
