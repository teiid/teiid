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

package org.teiid.translator.object.metadata;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.script.ScriptException;

import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.eval.TeiidScriptEngine;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;


/**
 * The BaseMetadataProcess is the core logic for providing metadata to the translator.
 */
public class JavaBeanMetadataProcessor {
	public static final String KEY_ASSOSIATED_WITH_FOREIGN_TABLE = "assosiated_with_table";  //$NON-NLS-1$
	public static final String ENTITYCLASS= "entity_class"; //$NON-NLS-1$
	
	public static final String GET = "get"; //$NON-NLS-1$
	public static final String IS = "is"; //$NON-NLS-1$
		
	public static final String VIEWTABLE_SUFFIX = "View"; //$NON-NLS-1$
	public static final String OBJECT_COL_SUFFIX = "Object"; //$NON-NLS-1$
	
	protected boolean isUpdatable = false;
	private TeiidScriptEngine engine = new TeiidScriptEngine();

	public void getMetadata(MetadataFactory mf, ObjectConnection conn, ObjectExecutionFactory env) {
		
		Map<String, Class<?>> cacheTypes = conn.getCacheNameClassTypeMapping();
		for (String cacheName : cacheTypes.keySet()) {
			Class<?> type = cacheTypes.get(cacheName);
			String pkField = conn.getPkField(cacheName);
			createSourceTable(mf, type, cacheName, pkField);
		}

	}
	
	private Table createSourceTable(MetadataFactory mf, Class<?> entity, String cacheName, String pkField) {
		String tableName = getTableName(entity);
		Table table = mf.getSchema().getTable(tableName);
		if (table != null) {
			//TODO: probably an error
			return table;
		}
		table = mf.addTable(tableName);
		table.setSupportsUpdate(isUpdateable(entity));
		table.setNameInSource(cacheName); 

		table.setProperty(ENTITYCLASS, entity.getName());
		
		String columnName = tableName + OBJECT_COL_SUFFIX;
		addColumn(mf, entity, entity, columnName, "this", SearchType.Unsearchable, table); //$NON-NLS-1$
		Map<String, Method> methods;
		try {
			methods = engine.getMethodMap(entity);
		} catch (ScriptException e) {
			throw new MetadataException(e);
		}
		
		Method pkMethod = null;
		if (pkField != null) {
			pkMethod = methods.get(pkField);
			if (pkMethod != null) {
				addColumn(mf, entity, pkMethod.getReturnType(), pkField, pkField, SearchType.Searchable, table);
			} else {
				//TODO: warning/error?
			}
		}
		
		//we have to filter the duplicate names, isFoo vs. foo
		Map<Method, String> methodsToAdd = new LinkedHashMap<Method, String>();
		for (Map.Entry<String, Method> entry : methods.entrySet()) {
			String name = methodsToAdd.get(entry.getValue());
			if (name == null || name.length() > entry.getKey().length()) {
				if (entry.getValue() == pkMethod 
						|| entry.getValue().getDeclaringClass() == Object.class
						|| entry.getValue().getName().equals("toString") //$NON-NLS-1$
						|| entry.getValue().getName().equals("hashCode") //$NON-NLS-1$
						|| TypeFacility.getRuntimeType(entry.getValue().getReturnType()) == Object.class) {
					continue;
				}				
				methodsToAdd.put(entry.getValue(), entry.getKey());
			}
		}
		
		for (Map.Entry<Method, String> entry : methodsToAdd.entrySet()) {
			addColumn(mf, entity, entry.getKey().getReturnType(), entry.getValue(), entry.getValue(), SearchType.Unsearchable, table);
		}
		return table;
	}
	
	/**
	 * Call to get the name of table based on the <code>Class</code> entity
	 * @param entity
	 * @return String name of table
	 */
	protected String getTableName(Class<?> entity) {
		return entity.getSimpleName();
	}
	
	/**
	 * @param entity  
	 */
	protected boolean isUpdateable(Class<?> entity) {
		return this.isUpdatable;
	}

	/**
	 * @param entity  
	 * @param columnName 
	 */
	protected boolean isUpdateable(Class<?> entity, String columnName) {
		return this.isUpdatable;
	}

	protected Column addColumn(MetadataFactory mf, Class<?> entity, Class<?> type, String attributeName, String nis, SearchType searchType, Table entityTable) {
		Column c = entityTable.getColumnByName(attributeName);
		if (c != null) {
			//TODO: there should be a log here
			return c;
		}
		c = mf.addColumn(attributeName, TypeFacility.getDataTypeName(TypeFacility.getRuntimeType(type)), entityTable);
		if (nis != null) {
			c.setNameInSource(nis);
		}
		c.setUpdatable(isUpdateable(entity, attributeName));
		c.setSearchType(searchType);
		c.setNativeType(type.getName());
		if (type.isPrimitive()) {
			c.setNullType(NullType.No_Nulls);
		}
		return c;
	}
	
}
