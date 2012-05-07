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

package org.teiid.translator.object;

import java.lang.reflect.Method;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;


/**
 * Reads from {@link ObjectMethodManager} and creates metadata through the {@link MetadataFactory}.
 */
public class ObjectMetadataProcessor {
	

	private boolean widenUnsingedTypes = true;
	private Set<String> unsignedTypes = new HashSet<String>();
	private MetadataFactory metadataFactory;
	private ObjectExecutionFactory connectorEnv;
	
	private Map<String, Table> tableMap = new HashMap<String, Table>();
//	private List<Relationship> relationships = new ArrayList<Relationship>();

	
	public ObjectMetadataProcessor(MetadataFactory metadataFactory, ObjectExecutionFactory env) {
		this.metadataFactory = metadataFactory;
		this.connectorEnv = env;
	}
	
	public void processMetadata() throws TranslatorException {
			
			ObjectMethodManager mgr = connectorEnv.getObjectMethodManager();
			
			Iterator<String> cnIt = mgr.keySet().iterator();
			while (cnIt.hasNext()) {
				String className = (String) cnIt.next();
						
				ObjectMethodManager.ClassMethods cm = mgr.getClassMethods(className);

				addTable(cm);
				
				
			}

//			addRelationships();
	}

	
	private void addTable(ObjectMethodManager.ClassMethods cm) throws TranslatorException {

		if (cm == null || !cm.hasMethods()) {
			
	        LogManager.logWarning(LogConstants.CTX_CONNECTOR, ObjectPlugin.Util
	    			.getString(
	    					"ObjectExecutionFactory.missingClassMethods", new Object[] { cm.getClassIdentifier().getName() })); //$NON-NLS-1$  

		}
		
		String className = cm.getClassName();
		String tableName = getTableName(className);
		
		Table table = metadataFactory.addTable(tableName);
		table.setNameInSource(className);
		table.setSupportsUpdate(false);
		table.setAnnotation("Class: "+ className);
		
		tableMap.put(tableName, table);
//		getRelationships(objectMetadata);

		addColumns(cm, table);
		
		
	}
	
	
	private String getTableName(String className) {
		String tableName = null;
		int idx = className.lastIndexOf(".");
		if (idx > 0) {
			tableName = className.substring(idx + 1);
		} else {
			tableName = className;
		}
		return tableName;

	}
	
	private void addColumns(ObjectMethodManager.ClassMethods cm, Table table) throws TranslatorException {
		Map<String, Method> methods = cm.getGetters();
		
		Column column = null;
		Iterator<String> mIts = methods.keySet().iterator();
		while (mIts.hasNext()) {
			String methodName = mIts.next();
			Method m = methods.get(methodName);

			methodName=methodName.substring( methodName.indexOf("get") + 3);
			
			String runtimeType = getRuntimeType(m.getReturnType());
			
			column = metadataFactory.addColumn(methodName, runtimeType, table);
			String simpleName = m.getReturnType().getSimpleName();
			column.setNativeType(simpleName);
			
//			String columnName = columns.getString(4);
//			int type = columns.getInt(5);
//			String typeName = columns.getString(6);
//			int columnSize = columns.getInt(7);
//			String runtimeType = getRuntimeType(type, typeName, columnSize);
//			//note that the resultset is already ordered by position, so we can rely on just adding columns in order
//			Column column = metadataFactory.addColumn(columnName, runtimeType, tableInfo.table);
//			column.setNameInSource(quoteName(columnName));
//			column.setPrecision(columnSize);
//			column.setLength(columnSize);
//			column.setNativeType(typeName);
//			column.setRadix(columns.getInt(10));
//			column.setNullType(NullType.values()[columns.getShort(11)]);
//			column.setUpdatable(true);
//			String remarks = columns.getString(12);
//			column.setAnnotation(remarks);
//			String defaultValue = columns.getString(13);
//			column.setDefaultValue(defaultValue);			
			
			
			if (runtimeType.equalsIgnoreCase("blob") || runtimeType.equalsIgnoreCase("clob") || runtimeType.equalsIgnoreCase("object")) {
				column.setSearchType(SearchType.Unsearchable);
				column.setSelectable(false);
			}
			
			if (runtimeType.equalsIgnoreCase("string")) {
				column.setLength(4000);
			}

		}
		
		methods = cm.getIses();
		
		mIts = methods.keySet().iterator();
		while (mIts.hasNext()) {
			String methodName = mIts.next();
			Method m = methods.get(methodName);
			
			methodName=methodName.substring( methodName.indexOf("is") + 2);							
			
			String simpleName = m.getReturnType().getSimpleName();
			String runtimeType = getRuntimeType(m.getReturnType());
			
			column = metadataFactory.addColumn(methodName, runtimeType, table);
			
			column.setNativeType(simpleName);
			
		}	
	}
	
	
	private String getRuntimeType(Class attributeType) {
		
		Class datatypeClass = DataTypeManager.getDataTypeClass(attributeType.getSimpleName());
		
		int sqlType = TypeFacility.getSQLTypeFromRuntimeType(datatypeClass);
			
		sqlType = checkForUnsigned(sqlType, attributeType.getSimpleName());
		
		return TypeFacility.getDataTypeNameFromSQLType(  sqlType );
	}
	


	private int checkForUnsigned(int sqlType, String typeName) {
		if (widenUnsingedTypes && unsignedTypes.contains(typeName)) {
			switch (sqlType) {
			case Types.TINYINT:
				sqlType = Types.SMALLINT;
				break;
			case Types.SMALLINT:
				sqlType = Types.INTEGER;
				break;
			case Types.INTEGER:
				sqlType = Types.BIGINT;
				break;
			}
		}
		return sqlType;
	}
	
}
