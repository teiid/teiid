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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.script.ScriptException;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.*;
import org.teiid.metadata.Column.SearchType;
import org.teiid.query.eval.TeiidScriptEngine;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectPlugin;


/**
 * The BaseMetadataProcess is the core logic for providing metadata to the translator.
 */
public class JavaBeanMetadataProcessor implements MetadataProcessor<ObjectConnection>{
    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Entity Class", description="Java Entity Class that represents this table", required=true)
    public static final String ENTITYCLASS= MetadataFactory.JPA_URI+"entity_class"; //$NON-NLS-1$
	
	public static final String GET = "get"; //$NON-NLS-1$
	public static final String IS = "is"; //$NON-NLS-1$
		
	public static final String VIEWTABLE_SUFFIX = "View"; //$NON-NLS-1$
	public static final String OBJECT_COL_SUFFIX = "Object"; //$NON-NLS-1$
	
	protected boolean isUpdatable = false;
	private TeiidScriptEngine engine = new TeiidScriptEngine();

	@Override
	public void process(MetadataFactory mf, ObjectConnection conn) throws TranslatorException {
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
		addColumn(mf, entity, entity, columnName, "this", SearchType.Unsearchable, table, true); //$NON-NLS-1$
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
                    addColumn(mf, entity, pkMethod.getReturnType(), pkField, pkField, SearchType.Searchable, table, true);
                } else {
                	// add a column so the PKey can be created, but make it not selectable
                    addColumn(mf, entity, java.lang.String.class, pkField, pkField, SearchType.Searchable, table, false);

                }
                             
    			String pkName = "PK_" + pkField.toUpperCase(); //$NON-NLS-1$
                ArrayList<String> x = new ArrayList<String>(1) ;
                x.add(pkField);
                mf.addPrimaryKey(pkName, x , table);

         } else {
 			// warn if no pk is defined
 			LogManager.logWarning(LogConstants.CTX_CONNECTOR, ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21000, tableName));				
         }
			
		//we have to filter the duplicate names, isFoo vs. foo
		Map<Method, String> methodsToAdd = new LinkedHashMap<Method, String>();
		for (Map.Entry<String, Method> entry : methods.entrySet()) {
			String name = methodsToAdd.get(entry.getValue());
			if (name == null || name.length() > entry.getKey().length()) {
				// dont create a column for the already created PKkey
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
			addColumn(mf, entity, entry.getKey().getReturnType(), entry.getValue(), entry.getValue(), SearchType.Unsearchable, table, true);			
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
	 * @return boolean
	 */
	protected boolean isUpdateable(Class<?> entity) {
		return this.isUpdatable;
	}

	/**
	 * @param entity  
	 * @param columnName 
	 * @return boolean
	 */
	protected boolean isUpdateable(Class<?> entity, String columnName) {
		return this.isUpdatable;
	}

	protected Column addColumn(MetadataFactory mf, Class<?> entity, Class<?> type, String attributeName, String nis, SearchType searchType, Table entityTable, boolean selectable) {
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
		c.setSelectable(selectable);
		if (type.isPrimitive()) {
			c.setNullType(NullType.No_Nulls);
		}
		return c;
	}
	
}
