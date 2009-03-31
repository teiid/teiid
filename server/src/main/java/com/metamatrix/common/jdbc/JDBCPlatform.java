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

package com.metamatrix.common.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.jdbc.metadata.Column;
import com.metamatrix.common.jdbc.metadata.Table;
import com.metamatrix.common.jdbc.metadata.UniqueKey;
import com.metamatrix.common.jdbc.syntax.ExpressionOperator;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.ReflectionHelper;

/**
* The JDBCPlatform represents a single datasource connection.  The
* default platform is that of MetaMatrix.  Use the JDBCPlatformFactory
* to create a JDBCPlatform that is representive of the type of
* connection.
*/

public class JDBCPlatform implements Serializable, Cloneable {

   // temporarily stored so that other methods can be initialized
    private DatabaseMetaData metadata;

      /** Indicates that streams will be used to store BLOB data. NOTE: does not work with ODBC */
    protected boolean usesStreamsForBlobBinding;
    protected boolean usesStreamsForClobBinding;
    protected boolean isSecure=false;

      // this is the product name from the metedata, if it can be determined;
    private String platformName;

    /** Holds a hashtable of values used to map JAVA types to database types for table creation */
    protected transient Map fieldTypes;

    protected transient String[] tableTypes;

	/** Operators specific to this platform */
    protected transient Map platformOperators;
    protected transient Map classTypes;
    protected transient Map minimumValues;
    protected transient Map maximumValues;

    protected static final String SPACE = " "; //$NON-NLS-1$
    protected static final String COMMA = ", "; //$NON-NLS-1$
    protected static final String PARAM = "?"; //$NON-NLS-1$
    protected static final String PERIOD = "."; //$NON-NLS-1$

    protected static final String INSERT_INTO = JDBCReservedWords.INSERT + SPACE + JDBCReservedWords.INTO + SPACE;
    protected static final String DELETE_FROM = JDBCReservedWords.DELETE + SPACE + JDBCReservedWords.FROM + SPACE;
    protected static final String UPDATE      = JDBCReservedWords.UPDATE + SPACE;
    protected static final String SELECT      = JDBCReservedWords.SELECT + SPACE;
    protected static final String EQUAL       =  " = "; //$NON-NLS-1$
    protected static final String FROM        = SPACE + JDBCReservedWords.FROM + SPACE;
    protected static final String WHERE       = SPACE + JDBCReservedWords.WHERE + SPACE;
    protected static final String ORDER_BY    = SPACE + JDBCReservedWords.ORDER_BY + SPACE;
    protected static final String GROUP_BY    = SPACE + "GROUP BY" + SPACE; //$NON-NLS-1$
    protected static final String SET         = SPACE + JDBCReservedWords.SET + SPACE;
    protected static final String ON          = SPACE + JDBCReservedWords.ON + SPACE;
    protected static final String INTO        = SPACE + JDBCReservedWords.INTO + SPACE;
    protected static final String IN            = SPACE + JDBCReservedWords.IN + SPACE;
//    protected static final String INNER_JOIN  = SPACE + JDBCReservedWords.INNER_JOIN + SPACE;
    protected static final String DISTINCT    = SPACE + JDBCReservedWords.DISTINCT + SPACE;
    protected static final String VALUES      = SPACE + JDBCReservedWords.VALUES + SPACE;
    protected static final String AND         = SPACE + JDBCReservedWords.AND + SPACE;
    protected static final String LENGTH      = SPACE + "LEN"; //$NON-NLS-1$

    public interface TableTypes {
        public static final String TABLE = "TABLE"; //$NON-NLS-1$
        public static final String VIEW = "VIEW"; //$NON-NLS-1$
        public static final String SYSTEM_TABLE = "SYSTEM TABLE"; //$NON-NLS-1$
        public static final String GLOBAL_TEMPORARY = "GLOBAL TEMPORARY"; //$NON-NLS-1$
        public static final String LOCAL_TEMPORARY = "LOCAL TEMPORARY"; //$NON-NLS-1$
        public static final String ALIAS = "ALIAS"; //$NON-NLS-1$
        public static final String SYNONYM = "SYNONYM"; //$NON-NLS-1$
    }

    protected JDBCPlatform () {
        usesStreamsForBlobBinding = false;
        tableTypes = null;
    }

    public void setConnection(Connection conn) throws MetaMatrixException {

      try {
          metadata = conn.getMetaData();
          getTableTypes();
          metadata = null;
      } catch (SQLException sqle){
          throw new MetaMatrixException(sqle);
      }
    }

    public boolean isClosed(Connection connection) {
    	try {
	    	return connection.isClosed();
    	} catch(SQLException e) {
    		return true;
        }
    }
    
    public void setIsSecure(boolean secure) {
        this.isSecure = secure;
   }    

    public void setPlatformName(String platformName) {
         this.platformName = platformName;
    }

    public String getPlatformName() {
        return platformName;
    }

    public boolean isOracle() {
        return false;
    }

    public boolean isDefault() {
        return true;
    }

    public boolean isMetaMatrix() {
        return false;
    }

    public boolean isDB2() {
        return false;
    }

    public boolean isSybase() {
        return false;
    }

    public boolean isMSSQL() {
        return false;
    }
    
    public boolean isMYSQL() {
        return false;
    }    

    public boolean isInformix() {
        return false;
    }
    
    public boolean isDerby() {
        return false;
    }
    
    public boolean isPostgres() {
        return false;
    }    

    public boolean isSecure() {
        return isSecure;
    }    

    public boolean usesStreamsForBlobBinding() {
        return usesStreamsForBlobBinding;
    }

    public boolean usesStreamsForClobBinding() {
        return usesStreamsForClobBinding;
    }
    
    public int setBlob(ResultSet results, byte[] data, String columnName) throws SQLException, IOException {
        Blob blob = results.getBlob(columnName);
        OutputStream l_blobOutputStream = null;
        
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        InputStream is = new BufferedInputStream(bais);
                
        try { 
            ReflectionHelper helper = new ReflectionHelper(blob.getClass());
            final Object[] args = new Object[]{};
            final Method m = helper.findBestMethodOnTarget("getBinaryOutputStream",args); //$NON-NLS-1$
            
                l_blobOutputStream = (OutputStream) m.invoke(blob,args);
            byte[] l_buffer = new byte[10* 1024];
    
            int cnt = is.available();
    
            int l_nread = 0;   // Number of bytes read
            while ((l_nread= is.read(l_buffer)) != -1) { // Read from file
                  l_blobOutputStream.write(l_buffer,0,l_nread); // Write to BLOB    
            }
    
            return cnt;
        } catch (Exception nsme) {
            throw new IOException(nsme.getMessage());
        } finally {
            // Close both streams
            if( is != null) {
                is.close();
                is = null;
            }
            if( l_blobOutputStream != null) {
                l_blobOutputStream.close();
                l_blobOutputStream = null;
            }
        }
    }    
    
    public void setBlob(PreparedStatement statement, byte[] data, int column) throws SQLException, IOException {

        statement.setBytes(column, data);
    }  
    
    public void setClob(ResultSet results, byte[] data, String columnName) throws SQLException, IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        BufferedInputStream is = new BufferedInputStream(bais);

        setClob(results, is, columnName);
    }
    
    public void setClob(ResultSet results, InputStream is, String columnName) throws SQLException, IOException {
     
      OutputStream l_clobOutputStream = null;
      
      Clob clob = results.getClob(columnName);
      try { 
          ReflectionHelper helper = new ReflectionHelper(clob.getClass());
          final Object[] args = new Object[]{};
          final Method m = helper.findBestMethodOnTarget("getAsciiOutputStream",args); //$NON-NLS-1$
          
          l_clobOutputStream = (OutputStream) m.invoke(clob,args);
          
          byte[] l_buffer = new byte[10* 1024];
          
//           int cnt = is.available();
  
          int l_nread = 0;   // Number of bytes read
          while ((l_nread= is.read(l_buffer)) != -1) { // Read from file
                l_clobOutputStream.write(l_buffer,0,l_nread); // Write to BLOB
  
          }
          return;
//          return cnt;
      } catch (Exception nsme) {
          nsme.printStackTrace();
          throw new IOException(nsme.getMessage());
      } finally {
          // Close both streams
          if( is != null) {
              is.close();
              is = null;
          }
          if( l_clobOutputStream != null) {
              l_clobOutputStream.close();
              l_clobOutputStream = null;
          }
      }
  }
      


  public void setClob(PreparedStatement statement, byte[] data, int column) throws SQLException, IOException {
            
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        BufferedInputStream is = new BufferedInputStream(bais);
        
        statement.setAsciiStream(column, is, is.available());
    }    
    
 
    
    /**
    * This will create an select statement that can be used
    * to select rows.
	  * @param tableMetadata the definition of the table in which
	  * columns will be used to create the select statement.
    */

    public String createSelectStatement( Table tableMetadata) {
        return createSelectStatement(tableMetadata, null);

    }

    public String createSelectStatement( Table tableMetadata, String tablePrefix) {
        return createSelectStatement(tableMetadata, tablePrefix, null);
    }

    /**
     * Create the select SQL string based on the table metadata.  If the
     * table prefix is passed, then prefix the table with this value.
     * Also, if the where columns are passed the sql will build a
     * WHERE clause containing parameters for each column
     * @param tabelMetadata is the table that drives the sql statement
     * @param tablePrefix is a prefix to add to the table; optional and nullable
     * @param whereColumns are columns to use in the where clause; optional and nullable
     */
    public String createSelectStatement(Table tableMetadata, String tablePrefix, Column[] whereColumns) {
        StringBuffer sql = new StringBuffer();
        sql.append(SELECT);

        Collection columns = tableMetadata.getColumns();

        String columnString = buildCommaSeperatedColumns(columns);
        sql.append(columnString);
        sql.append(FROM);

        if (tablePrefix != null && tablePrefix.length() > 0) {
            sql.append(tablePrefix);
            sql.append("."); //$NON-NLS-1$
        } else {
            sql.append(" "); //$NON-NLS-1$
        }

        sql.append(tableMetadata.getFullName());

        if (whereColumns != null) {
            String whereClause = buildWhereParameterClause(whereColumns);
            sql.append(SPACE);
            sql.append(whereClause);
        }

        return sql.toString();
    }

    /**
     * This will create an insert statement that can be used
     * to insert rows using a PreparedStatement.
     * @param tableMetadata the definition of the table in which
     * records are to be updated; may not be null;
     */
	  public String createInsertStatement( Table tableMetadata ) {
        return createInsertStatement(tableMetadata, ""); //$NON-NLS-1$
      }

    public String createInsertStatement( Table tableMetadata, String tablePrefix) {

        StringBuffer sql = new StringBuffer();
        sql.append(INSERT_INTO);

        if (tablePrefix != null && tablePrefix.length() > 0) {
            sql.append(tablePrefix);
            sql.append("."); //$NON-NLS-1$
        }
        sql.append(tableMetadata.getFullName());
        sql.append(" " + JDBCReservedWords.LEFT_PAREN); //$NON-NLS-1$
        Collection columns = tableMetadata.getColumns();

        String columnString = buildCommaSeperatedColumns(columns);
/*
        for (Iterator cit=columns.iterator(); cit.hasNext(); i++) {
            Column column = (Column) cit.next();
            sql.append(column.getFullName());
            if (i < size) {
                sql.append(COMMA);
            }
        }
*/
        sql.append(columnString);
        int size = columns.size();
        sql.append(JDBCReservedWords.RIGHT_PAREN);
        sql.append(VALUES);
        sql.append(JDBCReservedWords.LEFT_PAREN);
        for(int k=1;k<=size;k++){
            sql.append(PARAM);
            if(k < size){
                sql.append(COMMA);
            }
        }
        sql.append(JDBCReservedWords.RIGHT_PAREN);
        return sql.toString();
    }

    /**
      * This will create an insert statement that will contain the actual values to
      * be inserted into the table.
	  * @param tableMetadata the definition of the table in which
	  * records are to be updated; may not be null;
	  * @param values to be inserted
     */
	  public String createInsertStatement( Table tableMetadata, String[] values ) {
        return ""; //$NON-NLS-1$
    }

	/**
	 * Obtain the statement that can be used to update records
	 * in a table using a PreparedStatement.
	 * @param tableMetadata the definition of the table in which
	 * records are to be updated; may not be null;
	 */
	  public String createUpdateStatement( Table tableMetadata ) {
        return createUpdateStatement(tableMetadata, null); 
    }

    /**
     * Obtain the statement which will contain the actual data values
     * to update records in a table.
     * @param tableMetadata the definition of the table in which
     * records are to be updated; may not be null;
     * @param values to be inserted
     */
      public String createUpdateStatement( Table tableMetadata, String[] values ) {
            StringBuffer sql = new StringBuffer();
            sql.append(UPDATE);
            
            sql.append(tableMetadata.getFullName());
            
            if (values == null) {
                String setClause = buildSetParmClause(tableMetadata.getColumns().toArray());
                sql.append(setClause);
            } else {
                //TODO Not coded yet to build the set clause with the
                //  values.
                return "NEED TO UPDATE method createUpdateStatement"; //$NON-NLS-1$
            }
            
            if (!tableMetadata.getUniqueKeys().isEmpty()) {
                String whereClause = buildWhereUsingUiqueKeys(tableMetadata.getUniqueKeys());
                sql.append(SPACE);
                sql.append(whereClause);
                
            }

            return sql.toString();

    }
          
          
      protected String buildSetParmClause(Object[] setColumns) {

          StringBuffer sql = new StringBuffer();
          sql.append(SET);

          int size = setColumns.length;

          for(int k=0;k<size;k++){
              Column col = (Column) setColumns[k];
              sql.append(col.getName());
              sql.append(EQUAL);
              sql.append(PARAM);
              if(k < size - 1){
                  sql.append(COMMA);
              }
          }

          return sql.toString();

      }    
      
      
      protected String buildWhereUsingUiqueKeys(Collection uniqueKeys) {

          StringBuffer sql = new StringBuffer();
          sql.append(WHERE);

          int size = uniqueKeys.size();
          int k=0;
          for (Iterator it=uniqueKeys.iterator(); it.hasNext(); k++) {
              UniqueKey key = (UniqueKey) it.next();
              sql.append(key.getName());
              sql.append(EQUAL);
              sql.append(PARAM);
              if(k < size - 1){
                  sql.append(AND);
              }
          }

          return sql.toString();

      }      

	/**
	 * Obtain the statement that may be used to delete records
	 * from a table with the specified metadata.
	 * @param tableMetadata the definition of the table from which
	 * records are to be deleted; may not be null;
	 */
	  public String createDeleteStatement( Table table ) {
        return createDeleteStatement(table, null);
    }

    public String createDeleteStatement( Table table, String tablePrefix) {
        if (tablePrefix != null && tablePrefix.length() > 0) {
            return  DELETE_FROM + tablePrefix + "." + table.getName(); //$NON-NLS-1$
        }
        return DELETE_FROM+ table.getName();
    }

    public String createDeleteStatement( Table table, Column[] whereColumns, String tablePrefix) {
        String prefix;
        if (tablePrefix != null && tablePrefix.length() > 0) {
            prefix =  DELETE_FROM + tablePrefix + "." + table.getName(); //$NON-NLS-1$
        } else {
            prefix = DELETE_FROM+ table.getName();
        }

        StringBuffer sql = new StringBuffer(prefix);

        if (whereColumns != null) {
            String whereClause = buildWhereParameterClause(whereColumns);
            sql.append(SPACE);
            sql.append(whereClause);
        }

        return sql.toString();
    }


	  public String createTruncateStatement( String tablename ) {
	    return createTruncateStatement(tablename, null);
	  }

	  public String createTruncateStatement( String tablename , String tablePrefix) {
        if (tablePrefix != null && tablePrefix.length() > 0) {
            return  DELETE_FROM + tablePrefix + "." + tablename; //$NON-NLS-1$
        }

        return DELETE_FROM + tablename;
      }

    public int getMaxFieldNameSize() {
      return 50;
    }

    public String[] getTableTypes() throws MetaMatrixException {
        if (tableTypes != null) {
            return tableTypes;
        }

        Set tableTypesSet = new HashSet();
        try {
            ResultSet tableTypesResults = metadata.getTableTypes();
            while ( tableTypesResults.next() ) {
                tableTypesSet.add(tableTypesResults.getString(1).trim());
            }
        } catch ( SQLException e ){
            throw new MetaMatrixException(e);
//            errorMessages.add("Error using 'getTableTypes': " + e.getMessage());
        }

        tableTypes = new String[tableTypesSet.size()];
        int index = -1;
        Iterator iter = tableTypesSet.iterator();
        while ( iter.hasNext() ) {
            tableTypes[++index] = iter.next().toString();
        }

        return tableTypes;
    }


    public Map getClassTypes()
    {
      if (classTypes == null)
        classTypes = buildClassTypes();
      return classTypes;
    }

    public Map getFieldTypes() {
      if (fieldTypes == null)
          fieldTypes = buildFieldTypes();
      return fieldTypes;
    }

    /**
    * Call to determine what is the maximum value allowed for the
    * current database platform.
    * @param clazz is the java object data type that is of type Number
    * @return Number that represents the maximum value allowed on the
    *   current database platform.
    */
    public synchronized Number getMaximumValue(Class clazz) {
        if (clazz == null) return null;

        if (maximumValues == null) {
            maximumValues = maximumNumericValues();
        }

        Object obj = maximumValues.get(clazz);
        if (obj != null) {
            return (Number) obj;
        }
        return null;
    }

    /**
    * Call to determine what is the minimum value allowed for the
    * current database platform.
    * @param clazz is the java object data type that is of type Number
    * @return Number that represents the minimum value allowed on the
    *   current database platform.
    */
    public synchronized Number getMinimumValue(Class clazz) {
        if (clazz == null) return null;

        if (minimumValues == null) {
            minimumValues = minimumNumericValues();
        }

        Object obj = minimumValues.get(clazz);
        if (obj != null) {
            return (Number) obj;
        }
        return null;
    }


    public ExpressionOperator getOperator(String name) {
        return (ExpressionOperator) getPlatformOperators().get(name);
    }


/**
 * Return any platform-specific operators
 */
    public synchronized Map getPlatformOperators() {
      if (platformOperators == null)
          platformOperators = buildPlatformOperators();
      return platformOperators;
    }


  void initializePlatform() {
  }

  protected Map buildClassTypes() {
      HashMap types = new HashMap();
      return types;
  }

  protected Map buildFieldTypes() {
      HashMap types = new HashMap();
      return types;
  }

    protected void addOperator(ExpressionOperator op)
    {
      for (Iterator it=op.getSelectors().iterator(); it.hasNext(); ) {
        platformOperators.put(it.next(), op);
      }
    }

  protected Map buildPlatformOperators() {

	this.platformOperators = new Hashtable();

	// Ordering
	addOperator(ExpressionOperator.ascending());
	addOperator(ExpressionOperator.descending());

	// String
	addOperator(ExpressionOperator.toUpperCase());
	addOperator(ExpressionOperator.toLowerCase());
    addOperator(ExpressionOperator.maximum());
    addOperator(ExpressionOperator.minimum());

//	addOperator(ExpressionOperator.chr());
//	addOperator(ExpressionOperator.concat());
//	addOperator(ExpressionOperator.hexToRaw());
//	addOperator(ExpressionOperator.initcap());
//	addOperator(ExpressionOperator.instring());
//	addOperator(ExpressionOperator.leftPad());
//	addOperator(ExpressionOperator.leftTrim());
//	addOperator(ExpressionOperator.replace());
//	addOperator(ExpressionOperator.rightPad());
//	addOperator(ExpressionOperator.rightTrim());
//	addOperator(ExpressionOperator.substring());
//	addOperator(ExpressionOperator.toNumber());
//	addOperator(ExpressionOperator.translate());
//	addOperator(ExpressionOperator.trim());
//	addOperator(ExpressionOperator.ascii());

	// Date
//	addOperator(ExpressionOperator.addMonths());
//	addOperator(ExpressionOperator.dateToString());
//	addOperator(ExpressionOperator.lastDay());
//	addOperator(ExpressionOperator.monthsBetween());
//	addOperator(ExpressionOperator.nextDay());
//	addOperator(ExpressionOperator.roundDate());
//	addOperator(ExpressionOperator.toDate());

	// Math
    addOperator(ExpressionOperator.sum());
    addOperator(ExpressionOperator.count());
    addOperator(ExpressionOperator.average());

//	addOperator(ExpressionOperator.ceil());
//	addOperator(ExpressionOperator.cos());
//	addOperator(ExpressionOperator.cosh());
//	addOperator(ExpressionOperator.abs());
//	addOperator(ExpressionOperator.acos());
//	addOperator(ExpressionOperator.asin());
//	addOperator(ExpressionOperator.atan());
//	addOperator(ExpressionOperator.exp());
//	addOperator(ExpressionOperator.floor());
//	addOperator(ExpressionOperator.ln());
//	addOperator(ExpressionOperator.log());
//	addOperator(ExpressionOperator.mod());
//	addOperator(ExpressionOperator.power());
//	addOperator(ExpressionOperator.round());
//	addOperator(ExpressionOperator.sign());
//	addOperator(ExpressionOperator.sin());
//	addOperator(ExpressionOperator.sinh());
//	addOperator(ExpressionOperator.tan());
//	addOperator(ExpressionOperator.tanh());
//	addOperator(ExpressionOperator.trunc());

	// Object-relational
//	addOperator(ExpressionOperator.deref());
//	addOperator(ExpressionOperator.ref());
//	addOperator(ExpressionOperator.refToHex());
//	addOperator(ExpressionOperator.refToValue());

	// Other
//	addOperator(ExpressionOperator.greatest());
//	addOperator(ExpressionOperator.least());

	// ?
	addOperator(ExpressionOperator.today());

  return platformOperators;


  }

	/**
	 *	Builds a table of maximum numeric values keyed on java class. This is used for type testing but
	 * might also be useful to end users attempting to sanitize values.
	 * <p><b>NOTE</b>: BigInteger & BigDecimal maximums are dependent upon their precision & Scale
	 */
	public Map maximumNumericValues()
	{
		Map values = new HashMap(1);
/*
		values.put(Integer.class, new Integer(Integer.MAX_VALUE));
		values.put(Long.class, new Long(Long.MAX_VALUE));
		values.put(Double.class, new Double(Double.MAX_VALUE));
		values.put(Short.class, new Short(Short.MAX_VALUE));
		values.put(Byte.class, new Byte(Byte.MAX_VALUE));
		values.put(Float.class, new Float(Float.MAX_VALUE));
		values.put(java.math.BigInteger.class, new java.math.BigInteger("999999999999999999999999999999999999999")); //$NON-NLS-1$
		values.put(java.math.BigDecimal.class, new java.math.BigDecimal("99999999999999999999.9999999999999999999")); //$NON-NLS-1$
*/
        		return values;
	}

	/**
	 *	Builds a table of minimum numeric values keyed on java class. This is used for type testing but
	 * might also be useful to end users attempting to sanitize values.
	 * <p><b>NOTE</b>: BigInteger & BigDecimal minimums are dependent upon their precision & Scale
	 */
	public Map minimumNumericValues()
	{
		Map values = new HashMap(1);
/*
		values.put(Integer.class, new Integer(Integer.MIN_VALUE));
		values.put(Long.class, new Long(Long.MIN_VALUE));
		values.put(Double.class, new Double(Double.MIN_VALUE));
		values.put(Short.class, new Short(Short.MIN_VALUE));
		values.put(Byte.class, new Byte(Byte.MIN_VALUE));
		values.put(Float.class, new Float(Float.MIN_VALUE));
		values.put(java.math.BigInteger.class, new java.math.BigInteger("-99999999999999999999999999999999999999")); //$NON-NLS-1$
		values.put(java.math.BigDecimal.class, new java.math.BigDecimal("-9999999999999999999.9999999999999999999")); //$NON-NLS-1$
*/
        		return values;
	}

  public byte[] convertToByteArray(Object sourceObject) throws TransformationException{
	   if (sourceObject instanceof byte[]) {
	        return (byte[]) sourceObject;
	    } else if(sourceObject instanceof java.sql.Clob) {
	        return convertToByteArray( (java.sql.Clob) sourceObject);
        } else if (sourceObject instanceof java.sql.Blob)  {
	        return convertToByteArray( (java.sql.Blob) sourceObject);
	    } else {
	          throw new TransformationException(ErrorMessageKeys.JDBC_ERR_0001, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0001, sourceObject.getClass().getName()));
	    }
  }

   protected byte[] convertToByteArray(java.sql.Blob sourceObject) throws TransformationException{

        try {

//                long size = sourceObject.length();
//                System.out.println("@@@@@@@@@@ Blob to bytes: " + size);

            // Open a stream to read the BLOB data
            InputStream l_blobStream = sourceObject.getBinaryStream();

            // Open a file stream to save the BLOB data
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BufferedOutputStream bos = new BufferedOutputStream(out);

            // Read from the BLOB data input stream, and write to the file output
            // stream
            byte[] l_buffer = new byte[1024]; // buffer holding bytes to be transferred
            int l_nbytes = 0;  // Number of bytes read
            while ((l_nbytes = l_blobStream.read(l_buffer)) != -1) // Read from BLOB stream
              bos.write(l_buffer,0,l_nbytes); // Write to file stream

            // Flush and close the streams
            bos.flush();
            bos.close();
            l_blobStream.close();

            return out.toByteArray();

        } catch (IOException ioe) {
              throw new TransformationException(ioe, ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0002, sourceObject.getClass().getName()));
        } catch (SQLException sqe) {
              throw new TransformationException(sqe, ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0002, sourceObject.getClass().getName()));

        }
   
  } 
   

   
   public byte[] convertClobToByteArray(ResultSet results, String columName) throws TransformationException{
       byte[] data = null;
       Clob clobResults = null;
       try {
           if(usesStreamsForClobBinding()) {
             clobResults = results.getClob(columName);
             if (clobResults != null) {
                 data = convertToByteArray(clobResults);
             } 
           } else {
               String s = results.getString(columName);
               data = s.getBytes();
              
           }
//     } catch (IOException ioe) {
//           throw new TransformationException(ioe, ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0002, columName));
     } catch (SQLException sqe) {
           throw new TransformationException(sqe, ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0002, columName));
    
     }
     
     return data;
       
       
   }

   protected byte[] convertToByteArray(java.sql.Clob sourceObject) throws TransformationException{

       try {

//               long size = sourceObject.length();
//               System.out.println("@@@@@@@@@@ Blob to bytes: " + size);

           // Open a stream to read the BLOB data
           InputStream l_clobStream = sourceObject.getAsciiStream();

           // Open a file stream to save the BLOB data
           ByteArrayOutputStream out = new ByteArrayOutputStream();
           BufferedOutputStream bos = new BufferedOutputStream(out);

           // Read from the BLOB data input stream, and write to the file output
           // stream
           byte[] l_buffer = new byte[1024]; // buffer holding bytes to be transferred
           int l_nbytes = 0;  // Number of bytes read
           while ((l_nbytes = l_clobStream.read(l_buffer)) != -1) // Read from BLOB stream
             bos.write(l_buffer,0,l_nbytes); // Write to file stream

           // Flush and close the streams
           bos.flush();
           bos.close();
           l_clobStream.close();

           return out.toByteArray();

       } catch (IOException ioe) {
             throw new TransformationException(ioe, ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0002, sourceObject.getClass().getName()));
       } catch (SQLException sqe) {
             throw new TransformationException(sqe, ErrorMessageKeys.JDBC_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0002, sourceObject.getClass().getName()));

       }
 }

  /**
  * Takes a columns of Columns and returns a comma seperated string
  */
  protected String buildCommaSeperatedColumns(Collection columns) {
        StringBuffer sql = new StringBuffer();
        int i=1;
        int size = columns.size();
        for (Iterator cit=columns.iterator(); cit.hasNext(); i++) {
            Column column = (Column) cit.next();
            sql.append(column.getName());
            if (i < size) {
                sql.append(COMMA);
            }
        }

        return sql.toString();

  }

  protected String buildWhereParameterClause(Column[] whereColumns) {

        StringBuffer sql = new StringBuffer();
        sql.append(WHERE);

        int size = whereColumns.length;

        for(int k=0;k<size;k++){
            Column col = whereColumns[k];
            sql.append(col.getName());
            sql.append(EQUAL);
            sql.append(PARAM);
            if(k < size - 1){
                sql.append(AND);
            }
        }

        return sql.toString();

    }
    
    public int getDatabaseColumnSize(String tableName, String columnName, Connection jdbcConnection) throws SQLException {
        DatabaseMetaData dbMetadata = jdbcConnection.getMetaData();
        String catalogName = jdbcConnection.getCatalog();

        ResultSet columns = dbMetadata.getColumns(catalogName, null, tableName, "%"); //$NON-NLS-1$
        int s = -1;
         while ( columns.next() ) {
             String nis = columns.getString(4);
             if (columnName.equals(nis) ) {
                 s = columns.getInt(7);  
             }

         }    
        return s;
    }

//    public Map getDatabaseColumns(String tableName, Connection jdbcConnection) throws SQLException {
//        Map table = new HashMap();
//
//        DatabaseMetaData dbMetadata = jdbcConnection.getMetaData();
//        String catalogName = jdbcConnection.getCatalog();
////        String schemaName = jdbcConnection.getMetaData().getSchemaTerm();
//
//        ResultSet columns = dbMetadata.getColumns(catalogName, null, tableName, "%"); //$NON-NLS-1$
//
//        while ( columns.next() ) {
//
//            // Assume that the catalog, schema and table name all match ...
//            DatabaseColumn column = new DatabaseColumn( columns.getString(4) );
//            column.setDataType( columns.getShort(5) );
//            column.setDataTypeName( columns.getString(6) );
//            column.setSize( columns.getInt(7) );
//            //column.( columns.getShort(8) );     // buffer length is not used
//            column.setDecimalDigits( columns.getInt(9) );
//            column.setRadix( columns.getInt(10) );
////                column.setNullability( Nullability.getInstance( columns.getInt(11) ) );
//            column.setRemarks( columns.getString(12) );
//            column.setDefaultValue( columns.getString(13) );
//            //column.( columns.getInt(14) );          // current unused
//            //column.( columns.getInt(15) );          // current unused
//            column.setCharOctetLength( columns.getInt(16) );
//            column.setPosition( columns.getInt(17) );
//            //column.( columns.getString(18) );       // nullability string
//
//            table.put(column.getNameInSource(), column);
//        }
//
//        return table;
//    }
    
    
    public List parseToExecutableStatements(BufferedReader reader, String delimiter) throws SQLException {

        StringBuffer buffer = new StringBuffer();
        LinkedList listOfStatements = new LinkedList();

        try {
                List remove = getNonExecutableDelimiters();
                while (reader.ready()) {
                    String line = reader.readLine();
                    if(line == null) {
                        // End of stream
                        break;
                    }
                    
                    line = line.trim(); 
                    boolean doNotRemove=true;
                    // check if the line starts with a nonexecutable delimiter
                    // if so, then the line is not included
                    if (line == null || line.length() == 0) {
                        continue;
                    }
                    String UCASELINE = line.toUpperCase();
                    for (Iterator it=remove.iterator(); it.hasNext();) {
                        String removeItem = (String) it.next();
                        if (UCASELINE.startsWith(removeItem)) {                            
                            // line is to not be included in the statement list   
                            // i.e., it may be a comment
                            doNotRemove=false;
                            break;
                        }   
                    }
                    
                    if (doNotRemove) {
                        if (line.endsWith(delimiter)) {
                            int x = line.lastIndexOf(delimiter);
                            buffer.append(line.substring(0, x ));
                            listOfStatements.add(buffer);
                            buffer = new StringBuffer();
                        } else {
                                buffer.append(line + SPACE);
                                
                        }
                    
                    }
                   
                        
                }

        } catch(Exception e) {
            throw new SQLException(e.getMessage());
        }

        return listOfStatements;

    }
    
    /**
     * These nonexecutable delimiters are words or symbols that
     * cannot be excuted via an execute statement.  These generally
     * are platform specific or are required to be handled
     * by the processing logic (i.e., commit, set spool off, etc.)
     * 
     * the extending class should copy this forward and then
     * add to it.  Also, make an alpha characters upper case.
     * @return
     */
    protected List getNonExecutableDelimiters() {
        
        List re = new ArrayList(1);
        re.add("--");  //$NON-NLS-1$
        re.add("SET");//$NON-NLS-1$
        re.add("COMMIT");//$NON-NLS-1$
        re.add("SPOOL");//$NON-NLS-1$
        return re;
        
    }

}
