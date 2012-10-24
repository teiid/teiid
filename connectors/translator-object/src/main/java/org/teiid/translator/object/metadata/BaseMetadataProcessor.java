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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;


/**
 * The BaseMetadataProcess is the core logic for providing metadata to the translator.
 */
public abstract class BaseMetadataProcessor {
	public static final String KEY_ASSOSIATED_WITH_FOREIGN_TABLE = "assosiated_with_table";  //$NON-NLS-1$
	public static final String ENTITYCLASS= "entity_class"; //$NON-NLS-1$
	
	public static final String SCHEMA_NAME = "ObjectModel"; //$NON-NLS-1$
	
	public static final String GET = "get"; //$NON-NLS-1$
	public static final String IS = "is"; //$NON-NLS-1$
		
	public static final String VIEWTABLE_SUFFIX = "View"; //$NON-NLS-1$
	public static final String OBJECT_COL_SUFFIX = "Object"; //$NON-NLS-1$


	final static Map<Class<?>, Class<?>> map = new HashMap<Class<?>, Class<?>>();
	static {
	    map.put(boolean.class, Boolean.class);
	    map.put(byte.class, Byte.class);
	    map.put(short.class, Short.class);
	    map.put(char.class, Character.class);
	    map.put(int.class, Integer.class);
	    map.put(long.class, Long.class);
	    map.put(float.class, Float.class);
	    map.put(double.class, Double.class);
	    map.put(byte[].class, BlobType.class);
	    map.put(char[].class, ClobType.class);
	    map.put(Byte[].class, BlobType.class);
	    map.put(Character[].class, ClobType.class);
	    
	    map.put(Boolean.class, Boolean.class);
	    map.put(Byte.class, Byte.class);
	    map.put(Short.class, Short.class);
	    map.put(Character.class, Character.class);
	    map.put(Integer.class, Integer.class);
	    map.put(Long.class, Long.class);
	    map.put(Float.class, Float.class);
	    map.put(Double.class, Double.class);
	    map.put(Calendar.class, java.sql.Timestamp.class);
	}
	
	public void getMetadata(MetadataFactory mf, ObjectConnection conn, ObjectExecutionFactory env) throws TranslatorException {
		
		Map<String, Class<?>> cacheTypes = conn.getMapOfCacheTypes();
		for (String cacheName : cacheTypes.keySet()) {
			Class<?> type = cacheTypes.get(cacheName);
			
			Schema objSource = new Schema();
			objSource.setName(SCHEMA_NAME); //$NON-NLS-1$
			
			mf.setSchema(objSource);
			
			Table sourceTable = createSourceTable(mf, type, cacheName);
			createViewTable(mf, sourceTable, type, cacheName);
		}

	}
	
	private Table createSourceTable(MetadataFactory mf, Class<?> entity, String cacheName) throws TranslatorException {
		String tableName = getTableName(entity);
		Table table = mf.getSchema().getTable(tableName);
		if (table == null) {			
			table = mf.addTable(tableName);
			table.setSupportsUpdate(isUpdateable(entity));
			table.setNameInSource(cacheName);  //$NON-NLS-1$

			table.setProperty(ENTITYCLASS, entity.getName());
			
			String columnName = tableName + OBJECT_COL_SUFFIX;
			Column column = addColumn(mf, entity, columnName, "this", SearchType.Unsearchable, TypeFacility.getDataTypeName(getJavaDataType(entity)), isUpdateable(entity, columnName), table);
			
			column.setNativeType(entity.getName());

		}
		return table;
	}
	
	private void createViewTable(MetadataFactory mf, Table sourceTable, Class<?> entity, String cacheName) throws TranslatorException {		
		String viewName = sourceTable.getName() + VIEWTABLE_SUFFIX;
		Table vtable = mf.getSchema().getTable(viewName);
		if (vtable == null) {			
			vtable = mf.addTable(viewName);
			vtable.setSupportsUpdate(isUpdateable(entity));
			vtable.setTableType(Type.View);
			vtable.setVirtual(true);
			
			String transfomation = createViewTransformation(mf, entity, vtable, sourceTable);
			vtable.setSelectTransformation(transfomation);
		}

	}	
	
	/**
	 * Call to get the name of table based on the <code>Class</code> entity
	 * @param entity
	 * @return String name of table
	 */
	protected abstract String getTableName(Class<?> entity);
	
	/**
	 * Call to determine if entity is updateable 
	 * @param table
	 * @return boolean true if the table is updateable
	 */
	protected abstract boolean isUpdateable(Class<?> entity);

	protected abstract boolean isUpdateable(Class<?> entity, String columnName);
		

	protected abstract String createViewTransformation(MetadataFactory mf, Class<?> entity, Table vtable, Table sourceTable) throws TranslatorException;


	
	protected Column addColumn(MetadataFactory mf, Class<?> entity, String attributeName, String nis, SearchType searchType, String type, boolean updateable, Table entityTable) throws TranslatorException {
		if (!columnExists(attributeName, entityTable)) {
			Column c = mf.addColumn(attributeName, type, entityTable);
			if (nis != null) {
				c.setNameInSource(nis);
			}
			c.setUpdatable(updateable);
			c.setSearchType(searchType);
			return c;
		}
		return entityTable.getColumnByName(attributeName);
	}

	
	protected boolean isSimpleType(Class<?> type) {
		return type.isPrimitive() || type.equals(String.class)
				|| type.equals(BigDecimal.class) || type.equals(Date.class)
				|| type.equals(BigInteger.class)
				|| map.containsKey(type);
	}
	
	protected boolean isSupportedObjectType(Object object) {
		if (object instanceof Blob || object instanceof Clob || object instanceof SQLXML) {
			return true;
		}
		return false;
	}
	
	private boolean columnExists(String name, Table table) {
		return table.getColumnByName(name) != null;
	}
	
	protected Class<?> getJavaDataType(Class<?> type) {
		if (type.equals(Date.class)) {
			return java.sql.Timestamp.class;
		}
		
		if (type.isPrimitive()) {
			return map.get(type);  // usage			
		}
		return type;
	}
	
}
