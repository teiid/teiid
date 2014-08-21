/*
* JBoss, Home of Professional Open Source.
* See the COPYRIGHT.txt file distributed with this work for information
* regarding copyright ownership. Some portions may be licensed
* to Red Hat, Inc. under one or more contributor license agreements.
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either
* version 2.1 of the License, or (at your option) any later version.
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this library; if not, write to the Free Software
* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
* 02110-1301 USA.
*/
package org.teiid.eclipselink.platform;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Hashtable;

import org.eclipse.persistence.internal.databaseaccess.FieldTypeDefinition;
import org.eclipse.persistence.platform.database.DatabasePlatform;

public class TeiidPlatform extends DatabasePlatform{

	private static final long serialVersionUID = 6894570254643353289L;
	
	public TeiidPlatform() {
		super();
		this.pingSQL = "SELECT 1";	
		this.printOuterJoinInWhereClause = false;
	}

	
	protected Hashtable buildFieldTypes() {
		
		Hashtable fieldTypeMapping = super.buildFieldTypes();
		
		fieldTypeMapping.put(Boolean.class, new FieldTypeDefinition("varchar", false));
		fieldTypeMapping.put(byte[].class, new FieldTypeDefinition("varbinary", false));
		fieldTypeMapping.put(Character.class, new FieldTypeDefinition("char", false));
		fieldTypeMapping.put(Boolean.class, new FieldTypeDefinition("boolean", false));
		fieldTypeMapping.put(Byte.class, new FieldTypeDefinition("tinyint", false));
		
		fieldTypeMapping.put(Short.class, new FieldTypeDefinition("smallint", false));
		fieldTypeMapping.put(Integer.class, new FieldTypeDefinition("integer", false));
		fieldTypeMapping.put(Long.class, new FieldTypeDefinition("bigint", false));
		fieldTypeMapping.put(BigInteger.class, new FieldTypeDefinition("biginteger", false));
		fieldTypeMapping.put(Float.class, new FieldTypeDefinition("float", false));
		
		fieldTypeMapping.put(Double.class, new FieldTypeDefinition("double", false));
		fieldTypeMapping.put(BigDecimal.class, new FieldTypeDefinition("bigdecimal", false));
		fieldTypeMapping.put(Date.class, new FieldTypeDefinition("date", false));
		fieldTypeMapping.put(Time.class, new FieldTypeDefinition("time", false));
		fieldTypeMapping.put(Timestamp.class, new FieldTypeDefinition("timestamp", false));
		
		
		fieldTypeMapping.put(Object.class, new FieldTypeDefinition("object", false));
		fieldTypeMapping.put(Blob.class, new FieldTypeDefinition("blob", false));
		fieldTypeMapping.put(Clob.class, new FieldTypeDefinition("clob", false));
		fieldTypeMapping.put(SQLXML.class, new FieldTypeDefinition("xml", false));
		
		return fieldTypeMapping;
	}
	

	/**
	 * Avoid alter/create Constraint/index
	 */
	public boolean supportsDeleteOnCascade() {
		return false;
	}

	public boolean supportsForeignKeyConstraints() {
		return false;
	}

	public boolean requiresUniqueConstraintCreationOnTableCreate() {
		return false;
	}
	
	public boolean supportsIndexes() {
		return false;
	}

	public boolean supportsTempTables() {
		return true;
	}

	public boolean supportsLocalTempTables() {
		return true;
	}

	public boolean supportsGlobalTempTables() {
		return false;
	}

	public String getCreateViewString() {
		throw new RuntimeException("Teiid Server don't support create view in runtime");
	}

	@Override
	protected String getCreateTempTableSqlPrefix() {
		return "create local temporary table ";
	}


	@Override
	protected String getCreateTempTableSqlSuffix() {
		return "";
	}
	
	

}
