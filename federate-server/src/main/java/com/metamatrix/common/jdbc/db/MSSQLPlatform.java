/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;

import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.jdbc.syntax.ExpressionOperator;
import com.metamatrix.common.jdbc.syntax.FieldType;

public class MSSQLPlatform extends JDBCPlatform {

    public MSSQLPlatform() {
        super();
    }

    public boolean isMSSQL() {
        return true;
    }

    public boolean isDefault() {
        return false;
    }    

    /**
     * INTERNAL:
     * returns the maximum number of characters that can be used in a field
     * name on this platform.
     */
    public int getMaxFieldNameSize() {
      return 22;
    }
    
    public boolean isClosed(Connection connection) {
    	if(!super.isClosed(connection)) {
            Statement statement = null;
            try {
                statement = connection.createStatement();
    			statement.executeQuery("Select 'x'"); //$NON-NLS-1$
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

    protected  Map buildFieldTypes()   {
      Map fieldTypeMapping;

      fieldTypeMapping = new HashMap();
      fieldTypeMapping.put(Boolean.class, new FieldType("BIT default 0", false)); //$NON-NLS-1$

      fieldTypeMapping.put(Integer.class, new FieldType("INTEGER", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Long.class, new FieldType("NUMERIC", 19)); //$NON-NLS-1$
      fieldTypeMapping.put(Float.class, new FieldType("FLOAT(16)", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Double.class, new FieldType("FLOAT(32)", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Short.class, new FieldType("SMALLINT", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Byte.class, new FieldType("SMALLINT", false)); //$NON-NLS-1$
      fieldTypeMapping.put(java.math.BigInteger.class, new FieldType("NUMERIC", 28)); //$NON-NLS-1$
      fieldTypeMapping.put(java.math.BigDecimal.class, new FieldType("NUMERIC", 28).setLimits(28, -19, 19)); //$NON-NLS-1$

      fieldTypeMapping.put(String.class, new FieldType("VARCHAR", 20)); //$NON-NLS-1$
      fieldTypeMapping.put(Character.class, new FieldType("CHAR", 1)); //$NON-NLS-1$
      fieldTypeMapping.put(Byte[].class, new FieldType("IMAGE", false)); //$NON-NLS-1$
      fieldTypeMapping.put(Character[].class, new FieldType("TEXT", false)); //$NON-NLS-1$

      fieldTypeMapping.put(java.sql.Date.class, new FieldType("DATETIME", false)); //$NON-NLS-1$
      fieldTypeMapping.put(java.sql.Time.class, new FieldType("DATETIME", false)); //$NON-NLS-1$
      fieldTypeMapping.put(java.sql.Timestamp.class, new FieldType("DATETIME", false)); //$NON-NLS-1$

      return fieldTypeMapping;
    }

      protected Map buildPlatformOperators() {
            Map operators = super.buildPlatformOperators();

            // override for MSSQLPlatform specfic
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
		values.put(Double.class, new Double(0));
		values.put(Short.class, new Short(Short.MAX_VALUE));
		values.put(Byte.class, new Byte(Byte.MAX_VALUE));
		values.put(Float.class, new Float(0));
		values.put(java.math.BigInteger.class, new java.math.BigInteger("9999999999999999999999999999")); //$NON-NLS-1$
		values.put(java.math.BigDecimal.class, new java.math.BigDecimal("999999999.9999999999999999999")); //$NON-NLS-1$
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
		values.put(Double.class, new Double(-9));
		values.put(Short.class, new Short(Short.MIN_VALUE));
		values.put(Byte.class, new Byte(Byte.MIN_VALUE));
		values.put(Float.class, new Float(-9));
		values.put(java.math.BigInteger.class, new java.math.BigInteger("-9999999999999999999999999999")); //$NON-NLS-1$
		values.put(java.math.BigDecimal.class, new java.math.BigDecimal("-999999999.9999999999999999999")); //$NON-NLS-1$
		return values;
	}
    
} 
