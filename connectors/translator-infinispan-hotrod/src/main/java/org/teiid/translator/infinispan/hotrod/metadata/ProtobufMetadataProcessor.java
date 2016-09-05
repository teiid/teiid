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

package org.teiid.translator.infinispan.hotrod.metadata;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.infinispan.hotrod.InfinispanHotRodConnection;
import org.teiid.translator.infinispan.hotrod.InfinispanPlugin;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.metadata.JavaBeanMetadataProcessor;

import protostream.com.google.protobuf.Descriptors;


/**
 * The ProtobufMetadataProcess is the logic for providing metadata to the translator based on
 * the google protobuf Descriptors and the defined class types.
 * 
 * <p>
 * Here are the rules that are being followed.
 * </p>
 * <li>The Cache defined by the resource adapter</li>
 * The cache defined by the connection will be processed to create 1 or more tables.
 * <li>Table</li>
 * Each class defined in the protobuf is mapped to a table.
 * </br>
 * <li>Attributes</li>
 * Get/Is methods will be become table columns.
 * </br>
 * <li>One-to-one association</li>
 * The classes on both sides will be merged into a one table.  The nameInSource for the attributes in the class
 * contained within the root class will be assigned based on this format:  "get{Method}" name + "." + attribute
 * </br>
 * Example:   Person contains Contact (getContact)  and Contact has getter method getPhoneNumber  
 *     so the nameInSource for the column would become Contact.PhoneNumber
 * </br>
 * <li>One-to-many association</li>
 * The classes on both sides are mapped to two tables.  The root class will have primary key created.  The
 * table for the "many" side will have a foreign created. 
 * </br>
 * When an attribute is included from the many side (children), the total number of rows will be multiplied by
 * by the number of children objects.
 * <p>
 * <b>Decisions</b>
 * <li>User will need to decide what will be the primary key.  Will the key to the cache or a real attribute on the root class
 * will be used as the primary key.</li>
 * 
 */
public class ProtobufMetadataProcessor implements MetadataProcessor<ObjectConnection>{
				
	public static final String VIEWTABLE_SUFFIX = "View"; //$NON-NLS-1$
	public static final String OBJECT_COL_SUFFIX = "Object"; //$NON-NLS-1$

	private Table rootTable = null;
	private Method pkMethod = null;
	private Map<String, String> repeatableMap = new HashMap<String, String>();
	protected boolean classObjectColumn = false;
	
	@TranslatorProperty(display = "Class Object As Column", category = PropertyType.IMPORT, description = "If true, and when the translator provides the metadata, a column of object data type will be created that represents the stored object in the cache", advanced = true)
	public boolean isClassObjectColumn() {
		return classObjectColumn;
	} 
	
	public void setClassObjectColumn(boolean classObjectAsColumn) {
		this.classObjectColumn = classObjectAsColumn;
	}

	@Override
	public void process(MetadataFactory metadataFactory, ObjectConnection conn) throws TranslatorException {
			
			String cacheName = conn.getCacheName();			

			Class<?> type = conn.getCacheClassType();
			createRootTable(metadataFactory, type, ((InfinispanHotRodConnection) conn).getDescriptor(), cacheName,conn);

			boolean materializied = conn.getDDLHandler().getCacheNameProxy().getAliasCacheName() != null;
			if (materializied) {
											
				Table stageTable = addTable(metadataFactory, type, true, true);
							
				stageTable.setColumns(rootTable.getColumns());
				stageTable.setForiegnKeys(rootTable.getForeignKeys());
				stageTable.setFunctionBasedIndexes(rootTable.getFunctionBasedIndexes());
				stageTable.setIndexes(rootTable.getIndexes());
				stageTable.setParent(rootTable.getParent());
				stageTable.setSupportsUpdate(true);
				stageTable.setUniqueKeys(rootTable.getUniqueKeys());
				stageTable.setPrimaryKey(rootTable.getPrimaryKey());
				stageTable.setProperty(JavaBeanMetadataProcessor.PRIMARY_TABLE_PROPERTY, rootTable.getFullName());	
				
			} else {
				// only define inner classes when the data source is not using materialization.  
				// do not support materializing to inner classes
				addInnerClasses(metadataFactory, ((InfinispanHotRodConnection) conn).getDescriptor(), conn );
			}
	}


	private void createRootTable(MetadataFactory mf, Class<?> entity, Descriptor descriptor, String cacheName, ObjectConnection conn) throws TranslatorException {
			
		String pkField = conn.getPkField();
		boolean updatable = (pkField != null ? true : false);
		
		rootTable = addTable(mf, entity, updatable, false);
		
		if (classObjectColumn) {
	    // add column for cache Object, set to non-selectable by default so that select * queries don't fail by default
			addRootColumn(mf, Object.class, entity, null, null, SearchType.Unsearchable, rootTable.getName(), rootTable, false, false, NullType.Nullable); //$NON-NLS-1$	
		}
		
		pkMethod = null;
		if (updatable) {
		    pkMethod = conn.getClassRegistry().getReadClassMethods(entity.getName()).get(pkField);
		    if (pkMethod == null) {
				throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25008, new Object[] {pkField, cacheName, entity.getName()}));
		    	
		    }	    
		}
		
		boolean addKey = false;
		boolean repeats = false;
		// the descriptor is needed to determine which fields are defined as searchable.
		for (FieldDescriptor fd:descriptor.getFields()) {	
			if (fd.isRepeated() ) {
				repeatableMap.put(fd.getMessageType().getName(), fd.getName());
			} else {
				NullType nt = NullType.Nullable;
				SearchType st = isSearchable(fd);
				Class<?> returnType = getJavaType( fd,entity, conn);
//				String j = fd.getJavaType().name();
				if (fd.getName().equalsIgnoreCase(pkField)) {
					addKey = true;
					st = SearchType.Searchable;
					nt = NullType.No_Nulls;
				} else if (fd.isRequired()) {
					nt = NullType.No_Nulls;
				}
				// dont make primary key updatable, the object must be deleted and readded in order to change the key
				addRootColumn(mf, returnType, getProtobufNativeType(fd), fd.getFullName(), fd.getName(), st, rootTable.getName(), rootTable, true, true, nt);	
			}
		}	
		
		if (!addKey) {
				addRootColumn(mf, pkMethod.getReturnType(), pkMethod.getReturnType(), pkField, pkField, SearchType.Searchable, rootTable.getName(), rootTable, true, true, NullType.No_Nulls);
		}
		
		if (updatable) {
			// add primary key
			String pkName = "PK_" + pkField.toUpperCase(); //$NON-NLS-1$
	        ArrayList<String> x = new ArrayList<String>(1) ;
	        x.add(pkField);
	        mf.addPrimaryKey(pkName, x , rootTable);		    
		}			
	}
			
	private void addInnerClasses(MetadataFactory mf, Descriptor descriptor, ObjectConnection conn) throws TranslatorException {
		for (Descriptor des: descriptor.getNestedTypes()) {
			createInnerTable(mf, des, rootTable, pkMethod, conn);		
		}	

	}
	
	
	private Table addTable(MetadataFactory mf, Class<?> entity, boolean updatable, boolean staging) {
		String tName = entity.getSimpleName();
		if (staging) {
			tName = "ST_" + tName; //$NON-NLS-1$
		}
		Table t = mf.getSchema().getTable(tName);
		if (t != null) {
			//already loaded
			return t;
		}
		t = mf.addTable(tName);
		t.setSupportsUpdate(updatable);

		return t;
		
	}
	
    private static Method findMethod(String className, String methodName, ObjectConnection conn) throws TranslatorException {
        Map<String, Method> mapMethods = conn.getClassRegistry().getReadClassMethods(className);

        Method m = mapMethods.get(methodName);
        if (m!= null) return m;
        
        m = mapMethods.get(methodName.toLowerCase());
        if (m!= null) return m;
        
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
    private void createInnerTable(MetadataFactory mf, Descriptor desc, Table rootTable, Method pkMethod, ObjectConnection conn) throws TranslatorException  {

    	//	Descriptor d = desc.getContainingType();
		Descriptor parent = desc.getContainingType();
		// Need to find the method name that corresponds to the repeating attribute
		// so that the actual method name can be used in defining the NIS
		// which will provide the correct method to use when retrieving the data at execution time

		Class<?> pc = getRegisteredClass(parent.getName(), conn);
		Class<?> c = getRegisteredClass(desc.getName(), conn);

//		throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25060, new Object[] { colName }));

		Table t = addTable(mf, c, true, false);
		t.setSupportsUpdate(rootTable.supportsUpdate());

		String fd_Name = repeatableMap.get(desc.getName());
		List<FieldDescriptor> fields = desc.getFields();
		for (FieldDescriptor f:fields) {

			
			final SearchType st = isSearchable(f);
			
			if (fd_Name == null) {
				fd_Name = "";
			}
			// need to use the repeated descriptor, fd, as the prefix to the NIS in order to perform query
			addSubColumn(mf, getJavaType(f, c, conn),  getProtobufNativeType(f), f, st, fd_Name, t, true, true, (f.isRequired() ? NullType.No_Nulls  : NullType.Nullable) );	
		}
		
		if (pkMethod != null) {
			// if not repeatable, then use the attribute name
			String mName =  findMethodName(pc.getName(), fd_Name, conn);
			
			if (mName == null) {		
				mName = desc.getName();
			}

			
			// use the same parent table primary ke column name in the foreign key tables
			String methodName = rootTable.getPrimaryKey().getColumns().get(0).getName();
			List<String> keyColumns = new ArrayList<String>();
			keyColumns.add(methodName);
			List<String> referencedKeyColumns = new ArrayList<String>();
			referencedKeyColumns.add(methodName);
			String fkName = "FK_" + rootTable.getName().toUpperCase();
    		addRootColumn(mf, pkMethod.getReturnType(), pkMethod.getReturnType(), methodName, methodName, SearchType.Searchable, t.getName(), t, true, true, NullType.No_Nulls);
			ForeignKey fk = mf.addForiegnKey(fkName, keyColumns, referencedKeyColumns, rootTable.getName(), t);
			
			fk.setNameInSource(mName);

		}		

    }
    
	private void processRepeatedType(MetadataFactory mf, FieldDescriptor fd, Table rootTable, Method pkMethod, ObjectConnection conn) throws TranslatorException  {
		
		Descriptor d = fd.getMessageType();
		Descriptor parent = d.getContainingType();
		// Need to find the method name that corresponds to the repeating attribute
		// so that the actual method name can be used in defining the NIS
		// which will provide the correct method to use when retrieving the data at execution time

		Class<?> pc = getRegisteredClass(parent.getName(), conn);
		Class<?> c = getRegisteredClass(fd.getMessageType().getName(), conn);

		String mName =  findMethodName(pc.getName(), fd.getName(), conn);
		
		if (mName == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25060, new Object[] { fd.getName() }));
		}
		
		Table t = addTable(mf, c, true, false);
		
		// do not use getSourceName, the NameInSource has to be defined as the cache name
		t.setNameInSource(rootTable.getNameInSource()); 
			
		List<FieldDescriptor> fields = fd.getMessageType().getFields();
		for (FieldDescriptor f:fields) {

			final SearchType st = isSearchable(f);
			// need to use the repeated descriptor, fd, as the prefix to the NIS in order to perform query
			addSubColumn(mf, getJavaType(f, c, conn),  getProtobufNativeType(f), f, st, fd.getName(), t, true, true, NullType.Nullable);	
		}
		
		if (pkMethod != null) {
			// use the same parent table primary ke column name in the foreign key tables
			String methodName = rootTable.getPrimaryKey().getColumns().get(0).getName();
			List<String> keyColumns = new ArrayList<String>();
			keyColumns.add(methodName);
			List<String> referencedKeyColumns = new ArrayList<String>();
			referencedKeyColumns.add(methodName);
			String fkName = "FK_" + rootTable.getName().toUpperCase();
    		addRootColumn(mf, pkMethod.getReturnType(), pkMethod.getReturnType(), methodName, methodName, SearchType.Searchable, t.getName(), t, false, false, NullType.No_Nulls);
			ForeignKey fk = mf.addForiegnKey(fkName, keyColumns, referencedKeyColumns, rootTable.getName(), t);
			
			fk.setNameInSource(mName);

		}
	}	
	
    private  String findMethodName(String className, String methodName, ObjectConnection conn) throws TranslatorException {
        if (methodName == null || methodName.length() == 0) {
            return null;
        }
               
        Map<String, Method> mapMethods = conn.getClassRegistry().getReadClassMethods(className);
        if (mapMethods.get(methodName.toLowerCase()) != null) {
        	Method m = mapMethods.get(methodName.toLowerCase());
    		if (Collection.class.isAssignableFrom(m.getReturnType()) || m.getReturnType().isArray()) {
    			return methodName;
    		}
    		return null;
    		
        } else   if (mapMethods.get(methodName) != null) {
        	Method m = mapMethods.get(methodName);
    		if (Collection.class.isAssignableFrom(m.getReturnType()) || m.getReturnType().isArray()) {
    			return methodName;
    		}
    		return null;
    		
        }
        
        // because the class 'methods' contains 2 different references
        //  get'Name'  and 'Name', this will look for the 'Name' version
        for (Iterator it=mapMethods.keySet().iterator(); it.hasNext();) {
        	String mName = (String) it.next();
        	if (mName.toLowerCase().startsWith(methodName.toLowerCase()) ) {
        		Method m = mapMethods.get(mName);
         		if (Collection.class.isAssignableFrom(m.getReturnType()) || m.getReturnType().isArray()) {
        			return mName;
        		}
        	} 
        }
        return null;
    }	
	
	private Class<?> getRegisteredClass(String name, ObjectConnection conn) throws TranslatorException {
		Class<?> c = conn.getClassRegistry().getRegisteredClassUsingTableName(name);
		if (c != null) return c;
		
		throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25003, new Object[] { name }));
	}	

	private Column addRootColumn(MetadataFactory mf, Class<?> type, Class<?> nativeType, String columnFullName, String columnName,
			 SearchType searchType, String entityName, Table rootTable, boolean selectable, boolean updateable, NullType nt) {
		String attributeName;
		String nis;
		
		// Null fd indicates this is for the object
		if (columnFullName != null) {
			int idx;
			
			// the following logic is to set the NameInSource to the attribute name, for 
			// the root object.   For attributes that are in subclasses, then the
			// NameInSource will start with the method to access the subclass values.
			// QuickStart example:  
			//     root:    quickstart.Person.name   becomes  name
			// subclass:    quickstart.Person.PhoneNumber.number  becomes PhoneNumber.number
			//
			// The reflection logic, when multiple node names are used, will traverse the root
			// object by calling getPhoneNumber() to get its value, then will call getNumber()
			// to arrive at the value to return in the result set.
			if (rootTable.getName().equalsIgnoreCase(entityName)) {
				nis = columnName;
			} else {
				idx = columnFullName.indexOf(entityName);
				nis = columnFullName.substring(idx);
			}

			attributeName = columnName;
		} else {
			attributeName = entityName + OBJECT_COL_SUFFIX;
			nis = "this";
		}
	
		return addColumn(mf, type, nativeType, attributeName, nis, searchType, rootTable, selectable, updateable, nt);

	}
	
	private Column addSubColumn(MetadataFactory mf, Class<?> type, Class<?> nativeType, FieldDescriptor fd,
			 SearchType searchType, String nisPrefix, Table rootTable, boolean selectable, boolean updateable, NullType nt) {
		String attributeName = fd.getName();
		
		// create the combined name to use for a 1-to-many relationship when searching
		String nis = nisPrefix + "." + fd.getName();

		return addColumn(mf, type, nativeType, attributeName, nis, searchType, rootTable, selectable, updateable, nt);

	}	
	
	private Column addColumn(MetadataFactory mf, Class<?> type, Class<?> nativeType,  String attributeName, String nis, SearchType searchType, Table rootTable, boolean selectable, boolean updateable, NullType nt) {
		if (rootTable.getColumnByName(attributeName) != null) return rootTable.getColumnByName(attributeName);
		
		boolean isEnum = false;
		Class<?> datatype = type;
		if (type.isEnum()) {
			datatype = String.class;
			isEnum=true;
			
		} else {
			datatype = TypeFacility.getRuntimeType(type);
		}
		
		Column c = mf.addColumn(attributeName, TypeFacility.getDataTypeName(datatype), rootTable);
		
		if (nis != null) {
			c.setNameInSource(nis);
		}
		
		c.setUpdatable(updateable);
		c.setSearchType(searchType);
		
		if (isEnum) {
			c.setNativeType(Enum.class.getName());
		} else if (nativeType.isArray()) {
			c.setNativeType(type.getSimpleName());
		} else {
			c.setNativeType(nativeType.getName());
		}
		
		
		c.setSelectable(selectable);

		// this is needed due to an infinispan issue with updates/inserts that requires a workaround for boolean attributes:
		// either:
		//  - have a default set in the protobuf def file
		//  - pass a value on the sql statement
		if (nativeType.isAssignableFrom(boolean.class)) {
			c.setNullType(NullType.No_Nulls);
		} else {
			c.setNullType(nt);
		}
		
//		else if (type.isArray()) {
//			c.setNativeType(type.getSimpleName());
//		} else if (type.isEnum()) {
//			c.setNativeType(type.getName());
//		} else {
//			c.setNativeType(type.getName());
//		}

		return c; 
	}	
	
	private static SearchType isSearchable(FieldDescriptor fd) {
		if (fd.getDocumentation() != null) {
			String doc = fd.getDocumentation();
				// if its an indexfield, but not set to false, then its indexed and searchable
			if (doc.indexOf("@IndexedField")  > -1) {
				if (doc.indexOf("index=false") == -1) {
					return SearchType.Searchable;
				}
			}
		}
		return SearchType.Unsearchable;

	}
	
	private static Class<?> getJavaType(FieldDescriptor fd, Class<?> c, ObjectConnection conn) throws TranslatorException {

			Method m = findMethod(c.getName(), fd.getName(), conn);
			return m.getReturnType();
	}
	/*
	 * See https://developers.google.com/protocol-buffers/docs/proto#scalar
	 */
	private static Class<?> getProtobufNativeType(FieldDescriptor fd) throws TranslatorException {

		String n = fd.getJavaType().name();
		
		if (Descriptors.FieldDescriptor.JavaType.STRING.name().equals(n)) {
			return String.class;
		}
		
		if (Descriptors.FieldDescriptor.JavaType.BOOLEAN.name().equals(n)) {
			return boolean.class;
		}
		
		if (Descriptors.FieldDescriptor.JavaType.LONG.name().equals(n)) {
			return long.class;
		}
		
		if (Descriptors.FieldDescriptor.JavaType.INT.name().equals(n)) {
			return int.class;
		}
		if (Descriptors.FieldDescriptor.JavaType.DOUBLE.name().equals(n)) {
			return double.class;
		}
		
		if (Descriptors.FieldDescriptor.JavaType.FLOAT.name().equals(n)) {
			return float.class;
		}
		
		if (Descriptors.FieldDescriptor.JavaType.BYTE_STRING.name().equals(n)) {
			return byte[].class;
		}

//		if (Descriptors.FieldDescriptor.JavaType.ENUM.equals(fd.getJavaType())) {
//			return Enum.class;
//		}	
//		if (Descriptors.FieldDescriptor.JavaType.MESSAGE.equals(fd.getJavaType())) {
//			return Byte.class;
//		}	
		
		return String.class;
	}
}
