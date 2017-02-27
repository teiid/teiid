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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.MetadataException;
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
	private static final String STAGING_TABLE_PREFIX = "ST_";

	public static final String OBJECT_URI = "{http://www.teiid.org/translator/object/2016}"; //$NON-NLS-1$
	
    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Primary Table", description="Indicates the primary table that this staging table is used for", required=true)
    public static final String PRIMARY_TABLE_PROPERTY= OBJECT_URI+PRIMARY_TABLE; //$NON-NLS-1$

	public static final String GET = "get"; //$NON-NLS-1$
	public static final String SET = "set"; //$NON-NLS-1$
	public static final String IS = "is"; //$NON-NLS-1$
		
	public static final String VIEWTABLE_SUFFIX = "View"; //$NON-NLS-1$
	public static final String OBJECT_COL_SUFFIX = "Object"; //$NON-NLS-1$
	
	protected boolean classObjectColumn = false;
	protected boolean useAnnotations = false;
	protected Table rootTable = null;
	protected Method pkMethod=null;
	protected boolean materializedSource=false;
	private Table stagingTable = null;
	
	@TranslatorProperty(display = "Class Object As Column", category = PropertyType.IMPORT, description = "If true, and when the translator provides the metadata, a column of object data type will be created that represents the stored object in the cache", advanced = true)
	public boolean isClassObjectColumn() {
		return classObjectColumn;
	} 
	
	public void setClassObjectColumn(boolean classObjectAsColumn) {
		this.classObjectColumn = classObjectAsColumn;
	}
	
	public JavaBeanMetadataProcessor(boolean useAnnotations) {
		this.useAnnotations = useAnnotations;
	}

	@Override
	public void process(MetadataFactory mf, ObjectConnection conn) throws TranslatorException {

		this.materializedSource = conn.configuredForMaterialization();
		Class<?> type = conn.getCacheClassType();

		createRootTable(mf, type, conn);
				
		if (materializedSource) {
			stagingTable.setParent(rootTable.getParent());
			stagingTable.setSupportsUpdate(true);
			stagingTable.setProperty(JavaBeanMetadataProcessor.PRIMARY_TABLE_PROPERTY, rootTable.getFullName());
		}
	}
	
	
	private void createRootTable(MetadataFactory mf, Class<?> entity,  ObjectConnection conn ) throws TranslatorException {
				

		Map<String, Method> methods=null;
		Map<String, Method> writeMethods=null;
		try {
			ClassRegistry registry = conn.getClassRegistry();
			// only read methods are used for determining what columns will be defined
			methods = registry.getReadClassMethods(entity.getName());
			
			writeMethods = registry.getWriteClassMethods(entity.getName());
		} catch (TranslatorException e) {
			throw new MetadataException(e);
		}		
		
		String pkField = conn.getPkField();
		boolean updatable = determineUpdatable(conn, pkField, entity.getSimpleName(), methods);

		pkMethod = null;
		if (updatable) {
		    pkMethod = findMethod(entity.getName(), pkField, conn);
		    if (pkMethod == null) {
				throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21017, new Object[] {pkField, conn.getCacheName(), entity.getName()}));		    	
		    }	
		}
		
		rootTable = createTable(mf, entity, updatable, false);

		if (materializedSource) {
			stagingTable = createTable(mf, entity, true, true);
		}

		if (classObjectColumn && !materializedSource) {
			String columnName = rootTable.getName() + OBJECT_COL_SUFFIX;
			addColumn(mf, entity, columnName, "this", SearchType.Unsearchable, rootTable, false, NullType.Nullable, false, conn); //$NON-NLS-1$	
		}
		

 		if (updatable) {
 			// add the primary key field information; column and primary key 
 			addColumn(mf, pkMethod.getReturnType(), pkField, pkField, SearchType.Searchable, this.rootTable, true, NullType.No_Nulls, updatable, conn);
 	        
 			if (materializedSource) {
 	           addColumn(mf, pkMethod.getReturnType(), pkField, pkField, SearchType.Searchable, stagingTable, true, NullType.No_Nulls, true, conn);
 			}
 						
			String colname = getNameFromMethodName(pkMethod.getName());
            addPrimaryKey(mf, colname, rootTable);
            

    		if (materializedSource) {
    	        addPrimaryKey(mf, colname , stagingTable);
    		}
		}
              

		addTableContents(mf, rootTable, entity, pkField, conn, false, updatable, methods, writeMethods);
		if (materializedSource) {
			addTableContents(mf, stagingTable, entity, pkField, conn, true, updatable, methods, writeMethods);
		}

	}
	
	private void addPrimaryKey(MetadataFactory mf, String pkField, Table t ) {
        String pkn = getNameFromMethodName(pkField);
		
		String pkName = "PK_" + pkn.toUpperCase(); //$NON-NLS-1$
		ArrayList<String> x = new ArrayList<String>(1) ;
		x.add(pkn);
		mf.addPrimaryKey(pkName, x , t);

	}
	
	private boolean determineUpdatable(ObjectConnection conn, String pkField, String entityName, Map<String, Method> methods) {
		
	    this.pkMethod = null;
	    if (pkField != null) {
	    	pkMethod = methods.get(pkField);
        } else {
			// warn if no pk is defined
			LogManager.logWarning(LogConstants.CTX_CONNECTOR, ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21000, entityName));				
        }
	    
	    return (materializedSource ? true : (pkField != null ? true : false));
	}
	
	private Table createTable(MetadataFactory mf, Class<?> entity, boolean updatable, boolean staging) {
			
		String tableName = getTableName(entity);
		if (staging) {
			tableName = "ST_" + tableName; //$NON-NLS-1$
		}
			
		Table table = doesTableExist(mf, tableName, staging);
		if (table != null) {
			return table;
		}
		table = mf.addTable(tableName);
		table.setSupportsUpdate(updatable);
	
		return table;
	}
	
	private void addTableContents(MetadataFactory mf, Table table, Class<?> entity, String pkField, ObjectConnection conn, boolean staging, boolean updatable, Map<String, Method> readMethods, Map<String, Method> writeMethods) throws TranslatorException {
        
		List<Class<?>> regClasses = conn.getClassRegistry().getRegisteredClasses();
		
		//we have to filter the duplicate names, isFoo vs. footableName
		Map<Method, String> methodsToAdd = new LinkedHashMap<Method, String>();
		for (Map.Entry<String, Method> entry : readMethods.entrySet()) {
			String name = methodsToAdd.get(entry.getValue());
			if (name == null || name.length() > entry.getKey().length()) {
				// dont create a column for the already created PKkey
				Object o = entry.getValue();
				if (entry.getValue() == pkMethod 
						|| entry.getValue().getDeclaringClass() == Object.class
						|| entry.getValue().getName().equals("toString") //$NONentry.getValue().getReturnType()-NLS-1$
						|| entry.getValue().getName().equals("hashCode") //$NON-NLS-1$
			          ) {
					continue;
					
				} else if (TypeFacility.getRuntimeType(entry.getValue().getReturnType()) == Object.class) {
					// when data source is using materialization, children relationships are not supported
					if (materializedSource) continue;
						
					if (regClasses.contains(entry.getValue().getReturnType())) {
						
						// only if pkfield is defined can children be defined
						if (pkField != null) {
							Table childTable = createChildTable(mf, entry.getValue().getReturnType(), pkField, updatable, conn);
							createRelationShip(mf, childTable, (Method) o, this.rootTable, this.pkMethod, conn);
						}

					} else {
						continue;
					}
				}				
				methodsToAdd.put(entry.getValue(), entry.getKey());
			}
		}
		
		for (Map.Entry<Method, String> entry : methodsToAdd.entrySet()) {
			Method m = entry.getKey();
			
			// only if there's a corresponding set method is the column set to updatable
			boolean columnUpdatable = false;
			if (updatable) {
				if (doesSetMethodExist(writeMethods, m)){
					columnUpdatable = true;
				} 
			}
	
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
			addColumn(mf, m.getReturnType(), entry.getValue(), entry.getValue(), st, table, true, nt, columnUpdatable, conn);			
		}
	}
	
	// this should not be performed when the data source is being used for staging.
	// children tables are not supported for materializaton
	private Table createChildTable(MetadataFactory mf, Class<?> entity,  String pkField, boolean updatable, ObjectConnection conn ) throws TranslatorException {
		Map<String, Method> methods=null;
		Map<String, Method> writeMethods=null;
		try {
			ClassRegistry registry = conn.getClassRegistry();
			// only read methods are used for determining what columns will be defined, boolean staging
			methods = registry.getReadClassMethods(entity.getName());
			
			writeMethods = registry.getWriteClassMethods(entity.getName());
		} catch (TranslatorException e) {
			throw new MetadataException(e);
		}		

		Table childTable = createTable(mf, entity, updatable, false);

		addTableContents(mf, childTable, entity, pkField, conn, false, updatable, methods, writeMethods);
	
		return childTable;

	}
	
	private Table doesTableExist(MetadataFactory mf, String tableName, boolean staging) {
		if (staging) {
			tableName = STAGING_TABLE_PREFIX + tableName; //$NON-NLS-1$
		}
		Table table = mf.getSchema().getTable(tableName);

		if (table != null) {
			//TODO: probably an error
			return table;
		}
		return null;
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
	
	private static boolean doesSetMethodExist(Map<String, Method> writeMethods, Method m) {
		final String setMethod = SET + getNameFromMethodName(m.getName());

		if (writeMethods.containsKey(setMethod)) {
			return true;
		}
	
		return false;		
	}
	
	private static String getNameFromMethodName(String name) {
		String tolower = name.toLowerCase();
		if (tolower.toLowerCase().startsWith(GET) || tolower.startsWith(SET)) {
			return name.substring(3);
		}
		if (tolower.startsWith(IS)) {
			return name.substring(2);
		}
		return name;
	}
	
	/**
	 * Call to get the name of table based on the <code>Class</code> entity
	 * @param entity
	 * @return String name of table
	 */
	protected String getTableName(Class<?> entity) {
		return entity.getSimpleName();
	}
	
	protected Column addColumn(MetadataFactory mf,  Class<?> type, String attributeName, String nis, SearchType searchType, Table entityTable, boolean selectable, NullType nt, boolean updatable,  ObjectConnection conn) {
		Column c = entityTable.getColumnByName(attributeName);
		if (c != null) {
			//TODO: there should be a log here
			return c;
		}
		c = mf.addColumn(attributeName, conn.getClassRegistry().getObjectDataTypeManager().getDataTypeName(type), entityTable);
		if (nis != null) {
			c.setNameInSource(nis);
		}
		
		c.setNativeType(conn.getClassRegistry().getObjectDataTypeManager().getNativeTypeName(type));

		c.setUpdatable(updatable);
		c.setSearchType(searchType);
		c.setSelectable(selectable);
		c.setNullType(nt);
		return c;
	}
	
	private void createRelationShip(MetadataFactory mf, Table childTable, Method parentChildMethod,  Table rootTable, Method pkMethod, ObjectConnection conn)  {
		
		if (pkMethod != null) {
			// use the same parent table primary ke column name in the foreign key tables
			String fkName = "FK_" + rootTable.getName().toUpperCase();
			
			String methodName = rootTable.getPrimaryKey().getColumns().get(0).getName();
			List<String> keyColumns = new ArrayList<String>();
			keyColumns.add(methodName);
			List<String> referencedKeyColumns = new ArrayList<String>();
			referencedKeyColumns.add(methodName);			
			
    		addColumn(mf, pkMethod.getReturnType(), methodName, methodName, SearchType.Searchable, childTable,  false, NullType.No_Nulls, true, conn);
    		addPrimaryKey(mf, pkMethod.getName(), childTable);
    		
    		ForeignKey fk = mf.addForiegnKey(fkName, keyColumns, referencedKeyColumns, rootTable.getName(), childTable);
			
			fk.setNameInSource(getNameFromMethodName (parentChildMethod.getName()) );

		}
	}	
	
    private static Method findMethod(String className, String methodName, ObjectConnection conn) throws TranslatorException {
        Map<String, Method> mapMethods = conn.getClassRegistry().getReadClassMethods(className);

        
        Method m = ClassRegistry.findMethod(mapMethods, methodName, className);

        if (m != null) return m;
         
        // because the class 'methods' contains 2 different references
        //  get'Name'  and 'Name', this will look for the 'Name' version
        for (Iterator<String> it=mapMethods.keySet().iterator(); it.hasNext();) {
        	String mName = it.next();
        	if (mName.toLowerCase().startsWith(methodName.toLowerCase()) ) {
        		m = mapMethods.get(mName);
        		return m;
        	} 
        }
		throw new TranslatorException("Program Error: unable to find method " + methodName + " on class " + className);

    }


}
