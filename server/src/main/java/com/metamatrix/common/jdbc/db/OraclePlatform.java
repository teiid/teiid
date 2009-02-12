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

package com.metamatrix.common.jdbc.db;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.jdbc.JDBCReservedWords;
import com.metamatrix.common.jdbc.metadata.Column;
import com.metamatrix.common.jdbc.metadata.Table;
import com.metamatrix.common.jdbc.syntax.ExpressionOperator;
import com.metamatrix.common.jdbc.syntax.FieldType;
import com.metamatrix.core.util.ReflectionHelper;

public class OraclePlatform extends JDBCPlatform {
    private static final String EMPTY_BLOB = "empty_blob()"; //$NON-NLS-1$
    private static final String EMPTY_CLOB = "empty_clob()"; //$NON-NLS-1$


    public OraclePlatform() {
        super();

        usesStreamsForBlobBinding = true;
        usesStreamsForClobBinding = true;
    }

    public boolean isOracle() {
        return true;
    }

    /**
     * INTERNAL:
     * returns the maximum number of characters that can be used in a field
     * name on this platform.
     */
    public int getMaxFieldNameSize()  {
      return 37;
    }

  protected  Map buildFieldTypes()  {
      Map fieldTypeMapping;

      fieldTypeMapping = new HashMap();
      fieldTypeMapping.put(Boolean.class, new FieldType("NUMBER(1) default 0", false)); //$NON-NLS-1$

      fieldTypeMapping.put(Integer.class, new FieldType("NUMBER", 10)); //$NON-NLS-1$
      fieldTypeMapping.put(Long.class, new FieldType("NUMBER", 19)); //$NON-NLS-1$
      fieldTypeMapping.put(Float.class, new FieldType("NUMBER", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Double.class, new FieldType("NUMBER", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Short.class, new FieldType("NUMBER", 5)); //$NON-NLS-1$
      fieldTypeMapping.put(Byte.class, new FieldType("NUMBER", 3)); //$NON-NLS-1$
      fieldTypeMapping.put(java.math.BigInteger.class, new FieldType("NUMBER", 38)); //$NON-NLS-1$
      fieldTypeMapping.put(java.math.BigDecimal.class, new FieldType("NUMBER", 38).setLimits(38, -38, 38)); //$NON-NLS-1$

      fieldTypeMapping.put(String.class, new FieldType("VARCHAR2", 20)); //$NON-NLS-1$
      fieldTypeMapping.put(Character.class, new FieldType("CHAR", 1)); //$NON-NLS-1$

      fieldTypeMapping.put(Byte[].class, new FieldType("LONG RAW")); //$NON-NLS-1$
      fieldTypeMapping.put(Character[].class, new FieldType("LONG")); //$NON-NLS-1$

      fieldTypeMapping.put(java.sql.Date.class, new FieldType("DATE", false)); //$NON-NLS-1$
      fieldTypeMapping.put(java.sql.Time.class, new FieldType("DATE", false)); //$NON-NLS-1$
      fieldTypeMapping.put(java.sql.Timestamp.class, new FieldType("DATE", false)); //$NON-NLS-1$

      return fieldTypeMapping;
    }

     protected Map buildPlatformOperators() {
            Map operators = super.buildPlatformOperators();

            // override for Oracle specfic
            addOperator(ExpressionOperator.simpleFunction("toUpperCase","UPPER")); //$NON-NLS-1$ //$NON-NLS-2$

            return operators;

      }

	/**
	 *	Builds a table of maximum numeric values keyed on java class. This is used for type testing but
	 * might also be useful to end users attempting to sanitize values.
	 * <p><b>NOTE</b>: BigInteger & BigDecimal maximums are dependent upon their precision & Scale
	 */
	public Map maximumNumericValues()	{
		Map values = new HashMap();

		values.put(Integer.class, new Integer(Integer.MAX_VALUE));
		values.put(Long.class, new Long(Long.MAX_VALUE));
		values.put(Double.class, new Double(9.9999E125));
		values.put(Short.class, new Short(Short.MAX_VALUE));
		values.put(Byte.class, new Byte(Byte.MAX_VALUE));
		values.put(Float.class, new Float(Float.MAX_VALUE));
		values.put(java.math.BigInteger.class, new java.math.BigInteger("0")); //$NON-NLS-1$
		values.put(java.math.BigDecimal.class, new java.math.BigDecimal(new java.math.BigInteger("0"), 38)); //$NON-NLS-1$
		return values;
	}

	/**
	 *	Builds a table of minimum numeric values keyed on java class. This is used for type testing but
	 * might also be useful to end users attempting to sanitize values.
	 * <p><b>NOTE</b>: BigInteger & BigDecimal minimums are dependent upon their precision & Scale
	 */
	public Map minimumNumericValues()	{
		Map values = new HashMap();

		values.put(Integer.class, new Integer(Integer.MIN_VALUE));
		values.put(Long.class, new Long(Long.MIN_VALUE));
		values.put(Double.class, new Double(-1E-129));
		values.put(Short.class, new Short(Short.MIN_VALUE));
		values.put(Byte.class, new Byte(Byte.MIN_VALUE));
		values.put(Float.class, new Float(Float.MIN_VALUE));
		values.put(java.math.BigInteger.class, new java.math.BigInteger("0")); //$NON-NLS-1$
		values.put(java.math.BigDecimal.class, new java.math.BigDecimal(new java.math.BigInteger("0"), 38)); //$NON-NLS-1$
		return values;
	}


    public boolean isClosed(Connection connection) {
    	if(!super.isClosed(connection)) {
            Statement statement = null;
            try {
                statement = connection.createStatement();
    			statement.executeQuery("Select 'x' from DUAL"); //$NON-NLS-1$
    			return false;
    		} catch(SQLException e) {
    			return true;
            } finally {
                if ( statement != null ) {
                    try {
                        statement.close();
                        statement=null;
                    } catch ( SQLException e ) {
                    }
                }
            }
    	}
    	return true;
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
    
//        
//    public void setClob(ResultSet results, byte[] data, String columnName) throws SQLException, IOException {
//       
//        ByteArrayInputStream bais = new ByteArrayInputStream(data);
//        InputStream is = new BufferedInputStream(bais);
//        
//        
//        OutputStream l_clobOutputStream = null;
//        
//        Clob clob = results.getClob(columnName);
//        try { 
//            ReflectionHelper helper = new ReflectionHelper(clob.getClass());
//            final Object[] args = new Object[]{};
//            final Method m = helper.findBestMethodOnTarget("getAsciiOutputStream",args); //$NON-NLS-1$
//            
//            l_clobOutputStream = (OutputStream) m.invoke(clob,args);
//            
//            byte[] l_buffer = new byte[10* 1024];
//            
// //           int cnt = is.available();
//    
//            int l_nread = 0;   // Number of bytes read
//            while ((l_nread= is.read(l_buffer)) != -1) { // Read from file
//                  l_clobOutputStream.write(l_buffer,0,l_nread); // Write to BLOB
//    
//            }
//            return;
////            return cnt;
//        } catch (Exception nsme) {
//            nsme.printStackTrace();
//            throw new IOException(nsme.getMessage());
//        } finally {
//            // Close both streams
//            if( is != null) {
//                is.close();
//                is = null;
//            }
//            if( l_clobOutputStream != null) {
//                l_clobOutputStream.close();
//                l_clobOutputStream = null;
//            }
//        }
//    }
        

    /**
    * This will create an insert statement that can be used
    * to insert rows using a PreparedStatement. This method override
    * its parent to handle the Blob specialized for Oracle platform.
    * @param tableMetadata the definition of the table in which
	* records are to be updated; may not be null;
    */
	  public String createInsertStatement( Table tableMetadata ) {

        List columns = new ArrayList(tableMetadata.getColumns());

        StringBuffer sql = new StringBuffer();
        sql.append(INSERT_INTO);
        sql.append(tableMetadata.getFullName());
        sql.append(" " + JDBCReservedWords.LEFT_PAREN); //$NON-NLS-1$

        String columnString = buildCommaSeperatedColumns(columns);
        int size = columns.size();

/*
        int i=1;
        for (Iterator cit=columns.iterator(); cit.hasNext(); i++) {
            Column column = (Column) cit.next();
            sql.append(column.getFullName());
            if (i < size) {
                sql.append(COMMA);
            }
        }
*/
        sql.append(columnString);
        sql.append(JDBCReservedWords.RIGHT_PAREN);
        sql.append(VALUES);
        sql.append(JDBCReservedWords.LEFT_PAREN);
        for(int k=1;k<=size;k++){
            //got Types.OTHER, not BLOB. Temp work around.
            if(((Column)columns.get(k-1)).getDataType() == Types.BLOB
                || ((Column)columns.get(k-1)).getDataType() == Types.OTHER){
                sql.append(EMPTY_BLOB);
            } else if(((Column)columns.get(k-1)).getDataType() == Types.CLOB) {
                sql.append(EMPTY_CLOB);
                
            }else{
                sql.append(PARAM);
            }
            if(k < size){
                sql.append(COMMA);
            }
        }
        sql.append(JDBCReservedWords.RIGHT_PAREN);
        return sql.toString();
    }

     public boolean isDefault() {
        return false;
    }

}
