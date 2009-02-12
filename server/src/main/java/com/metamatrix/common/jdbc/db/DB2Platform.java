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

import java.util.HashMap;
import java.util.Map;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;

import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.jdbc.syntax.ExpressionOperator;
import com.metamatrix.common.jdbc.syntax.FieldType;

public class DB2Platform extends JDBCPlatform {

    public DB2Platform() {
        super();
    }

    public boolean isDB2() {
        return true;
    }

    public boolean isDefault() {
        return false;
    }
    
    public boolean isClosed(Connection connection) {
    	if(!super.isClosed(connection)) {
            Statement statement = null;
    		try {
	    		statement = connection.createStatement();
    			statement.executeQuery("Select 'x' from sysibm.systables where 1 = 2"); //$NON-NLS-1$
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

    protected  Map buildFieldTypes()  {
      Map fieldTypeMapping = new HashMap();

      fieldTypeMapping.put(Boolean.class, new FieldType("SMALLINT DEFAULT 0", false)); //$NON-NLS-1$

      fieldTypeMapping.put(Integer.class, new FieldType("INTEGER", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Long.class, new FieldType("INTEGER", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Float.class, new FieldType("FLOAT", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Double.class, new FieldType("FLOAT", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Short.class, new FieldType("SMALLINT", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Byte.class, new FieldType("SMALLINT", false)); //$NON-NLS-1$
      fieldTypeMapping.put(java.math.BigInteger.class, new FieldType("DECIMAL", 15)); //$NON-NLS-1$
      fieldTypeMapping.put(java.math.BigDecimal.class, new FieldType("DECIMAL", 15).setLimits(15,0,15)); //$NON-NLS-1$

      fieldTypeMapping.put(String.class, new FieldType("VARCHAR", 20)); //$NON-NLS-1$
      fieldTypeMapping.put(Character.class, new FieldType("CHAR", 1)); //$NON-NLS-1$
      fieldTypeMapping.put(Byte[].class, new FieldType("BLOB", 64000)); //$NON-NLS-1$
      fieldTypeMapping.put(Character[].class, new FieldType("CLOB", 64000)); //$NON-NLS-1$

      fieldTypeMapping.put(java.sql.Date.class, new FieldType("DATE", false)); //$NON-NLS-1$
      fieldTypeMapping.put(java.sql.Time.class, new FieldType("TIME", false)); //$NON-NLS-1$
      fieldTypeMapping.put(java.sql.Timestamp.class, new FieldType("TIMESTAMP", false)); //$NON-NLS-1$

      return fieldTypeMapping;
    }

      protected Map buildPlatformOperators() {
            Map operators = super.buildPlatformOperators();

            // override for DB2 specfic
            addOperator(ExpressionOperator.simpleFunction("toUpperCase","UCASE")); //$NON-NLS-1$ //$NON-NLS-2$

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
		values.put(Long.class, new Long(Integer.MAX_VALUE));
		values.put(Float.class, new Float(Float.MAX_VALUE));
		values.put(Double.class, new Double(Float.MAX_VALUE));
		values.put(Short.class, new Short(Short.MAX_VALUE));
		values.put(Byte.class, new Byte(Byte.MAX_VALUE));
		values.put(java.math.BigInteger.class, new java.math.BigInteger("999999999999999")); //$NON-NLS-1$
		values.put(java.math.BigDecimal.class, new java.math.BigDecimal("0.999999999999999")); //$NON-NLS-1$
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
		values.put(Long.class, new Long(Integer.MIN_VALUE));
		values.put(Float.class, new Float(Float.MIN_VALUE));
		values.put(Double.class, new Double(Float.MIN_VALUE));
		values.put(Short.class, new Short(Short.MIN_VALUE));
		values.put(Byte.class, new Byte(Byte.MIN_VALUE));
		values.put(java.math.BigInteger.class, new java.math.BigInteger("-999999999999999")); //$NON-NLS-1$
		values.put(java.math.BigDecimal.class, new java.math.BigDecimal("-0.999999999999999")); //$NON-NLS-1$
		return values;
	}
      
}
