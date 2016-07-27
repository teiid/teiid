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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectPlugin;


/**
 * The BaseMetadataProcess is the core logic for providing metadata to the translator.
 */
public class JavaBeanMetadataProcessor implements MetadataProcessor<ObjectConnection>{
	
	private static final String PRIMARY_TABLE="primary_table"; //$NON-NLS-1$

	public static final String OBJECT_URI = "{http://www.teiid.org/translator/object/2016}"; //$NON-NLS-1$
	
    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Primary Table", description="Indicates the primary table that this staging table is used for", required=true)
    public static final String PRIMARY_TABLE_PROPERTY= OBJECT_URI+PRIMARY_TABLE; //$NON-NLS-1$

	public static final String GET = "get"; //$NON-NLS-1$
	public static final String IS = "is"; //$NON-NLS-1$
		
	public static final String VIEWTABLE_SUFFIX = "View"; //$NON-NLS-1$
	public static final String OBJECT_COL_SUFFIX = "Object"; //$NON-NLS-1$
	
	protected boolean isUpdatable = false;
	protected boolean useAnnotations = false;
	protected boolean classObjectAsColumn = false;
	protected Table rootTable = null;
	
	public JavaBeanMetadataProcessor(boolean useAnnotations) {
		this.useAnnotations = useAnnotations;
	}
	
	
	@TranslatorProperty(display="Class Object As Column", category=PropertyType.IMPORT, description="If true, and when the translator provides the metadata, a column of object data type will be created that represents the stored object in the cache", advanced=true)
	public boolean supportClassObjectAsColumn() {
		return classObjectAsColumn;
	}	
	
	public void setSupportsClassObjectAsColumn(boolean classObjectAsColumn) {
		this.classObjectAsColumn = classObjectAsColumn;
	}

	@Override
	public void process(MetadataFactory mf, ObjectConnection conn) throws TranslatorException {
		String cacheName = conn.getCacheName();			

		Class<?> type = conn.getCacheClassType();
		rootTable = createSourceTable(mf, type, cacheName, conn, false);
		
		if (conn.getDDLHandler().getCacheNameProxy().getAliasCacheName() != null) {
			createSourceTable(mf, type, cacheName, conn, true);
		}
	}
	
	private Table createSourceTable(MetadataFactory mf, Class<?> entity, String cacheName, ObjectConnection conn, boolean staging ) {

		ClassRegistry registry = conn.getClassRegistry();
		
		String pkField = conn.getPkField();
		setIsUpdateable(pkField != null ? true : false);
			
		String tableName = getTableName(entity);
		if (staging) {
			tableName = "ST_" + tableName; //$NON-NLS-1$
		}
		Table table = mf.getSchema().getTable(tableName);
		if (table != null) {
			//TODO: probably an error
			return table;
		}
		table = mf.addTable(tableName);
		table.setSupportsUpdate(this.isUpdatable);
		
		if (classObjectAsColumn) {
			String columnName = tableName + OBJECT_COL_SUFFIX;
			addColumn(mf, entity, entity, columnName, "this", SearchType.Unsearchable, table, false, NullType.Nullable, false); //$NON-NLS-1$
		}
		
		Map<String, Method> methods;
		try {
			
			methods = registry.getReadClassMethods(entity.getName());
		} catch (TranslatorException e) {
			throw new MetadataException(e);
		}
		
        Method pkMethod = null;
        if (pkField != null) {
                pkMethod = methods.get(pkField);
                if (pkMethod != null) {
                    addColumn(mf, entity, pkMethod.getReturnType(), pkField, pkField, SearchType.Searchable, table, true, NullType.No_Nulls, this.isUpdatable);
                } else {
                	// add a column so the PKey can be created, but make it not selectable
                    addColumn(mf, entity, java.lang.String.class, pkField, pkField, SearchType.Searchable, table, false, NullType.Unknown, this.isUpdatable);

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
			Method m = entry.getKey();
	
				SearchType st = SearchType.Unsearchable;
				
				NullType nt = NullType.Nullable;

				// default for Hibernate index = true, so setting based on finding false
				if (useAnnotations) {
					if (isMethodSearchable(entity,m)) {
						st = SearchType.Searchable;
						
						if (isValueRequired(m)) {
							nt =NullType.No_Nulls;
						}
					} else {
						// then check if there's an attribute that's indexed
						Field f = findClassField(entity, m);
						if (isFieldSearchable(entity,f)) {
							st = SearchType.Searchable;
							if (isValueRequired(f)) {
								nt =NullType.No_Nulls;			
							}

						} 
					}					
					
				}
				
				addColumn(mf, entity, m.getReturnType(), entry.getValue(), entry.getValue(), st, table, true, nt, this.isUpdatable);			
		}
		
		if (staging) {
			table.setProperty(PRIMARY_TABLE_PROPERTY, rootTable.getFullName());

		}
				
		return table;
	}
		
	protected boolean isMethodSearchable(Class<?> entity, Method m) {
		Object ma = m.getAnnotation(org.hibernate.search.annotations.Field.class);
		if (ma != null) return true;
		
		return false;
		
	}
	
	protected boolean isValueRequired(Method m) {
		return false;
	}
	
	protected boolean isFieldSearchable(Class<?> entity, Field f) {
		
		if (f != null) {
			Object fa = f.getAnnotation(org.hibernate.search.annotations.Field.class);
			if (fa != null) {
				org.hibernate.search.annotations.Field hfa = (org.hibernate.search.annotations.Field) fa;
				if (hfa.index() == org.hibernate.search.annotations.Index.NO) {
					return false;
				}
				return true;
			}
		} 
		return false;
		
	}	
	
	protected boolean isValueRequired(Field f) {
		return false;
	}
	
	private Field findClassField(Class<?> clzz, Method m) {
		final String ml = m.getName().toLowerCase();
		Field[] fields = clzz.getDeclaredFields();
		if (fields != null) {
			for (Field f:fields) {
				
				String fl = f.getName().toLowerCase();
				if (ml.endsWith(fl)) {
					return f;
				}
			}
		}
		return null;
		
	}
	
	/**
	 * Call to get the name of table based on the <code>Class</code> entity
	 * @param entity
	 * @return String name of table
	 */
	protected String getTableName(Class<?> entity) {
		return entity.getSimpleName();
	}
	
	protected void setIsUpdateable(boolean isUpdateable) {
		this.isUpdatable = isUpdateable;
	}
	
	protected Column addColumn(MetadataFactory mf, Class<?> entity, Class<?> type, String attributeName, String nis, SearchType searchType, Table entityTable, boolean selectable, NullType nt, boolean updatable) {
		Column c = entityTable.getColumnByName(attributeName);
		if (c != null) {
			//TODO: there should be a log here
			return c;
		}
		c = mf.addColumn(attributeName, TypeFacility.getDataTypeName(TypeFacility.getRuntimeType(type)), entityTable);
		if (nis != null) {
			c.setNameInSource(nis);
		}
		
		
		if (type.isArray()) {
			c.setNativeType(type.getSimpleName());
		} else if (type.isEnum()) {
			c.setNativeType(Enum.class.getName());
		} else {
			c.setNativeType(type.getName());
		}
		

		c.setUpdatable(updatable);
		c.setSearchType(searchType);
		c.setSelectable(selectable);
		c.setNullType(nt);
		return c;
	}
	
}
