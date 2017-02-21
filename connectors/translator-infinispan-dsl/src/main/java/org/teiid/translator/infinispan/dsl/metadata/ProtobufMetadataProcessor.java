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

package org.teiid.translator.infinispan.dsl.metadata;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.infinispan.dsl.InfinispanDSLConnection;
import org.teiid.translator.infinispan.dsl.InfinispanPlugin;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.metadata.JavaBeanMetadataProcessor;
import org.teiid.translator.object.util.ObjectUtil;


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
 * <li>Relationship Support</li>
 * 1-to-1 associations
 * 1-to-many associations (i.e, Collections or Array)
 * 
 */
public class ProtobufMetadataProcessor implements MetadataProcessor<ObjectConnection>{
				
	public static final String VIEWTABLE_SUFFIX = "View"; //$NON-NLS-1$
	public static final String OBJECT_COL_SUFFIX = "Object"; //$NON-NLS-1$

	private Table rootTable = null;
	private Method pkMethod = null;
	private Map<String, FieldDescriptor> descriptorMap = new HashMap<String, FieldDescriptor>();
	protected boolean classObjectColumn = false;
	private boolean materialized = false;
	private Table stagingTable = null;
	
	@TranslatorProperty(display = "Class Object As Column", category = PropertyType.IMPORT, description = "If true, and when the translator provides the metadata, a column of object data type will be created that represents the stored object in the cache", advanced = true)
	public boolean isClassObjectColumn() {
		return classObjectColumn;
	} 
	
	public void setClassObjectColumn(boolean classObjectAsColumn) {
		this.classObjectColumn = classObjectAsColumn;
	}

	@Override
	public void process(MetadataFactory metadataFactory, ObjectConnection connection) throws TranslatorException {
		InfinispanDSLConnection conn = (InfinispanDSLConnection) connection;		

		materialized = conn.configuredForMaterialization();
		createRootTable(metadataFactory, conn.getCacheClassType(), conn.getDescriptor(), conn.getCacheName() ,conn);

		if (materialized) {
				stagingTable.setParent(rootTable.getParent());
				stagingTable.setSupportsUpdate(true);
				stagingTable.setProperty(JavaBeanMetadataProcessor.PRIMARY_TABLE_PROPERTY, rootTable.getFullName());	
		}

	}


	private void createRootTable(MetadataFactory mf, Class<?> entity, Descriptor descriptor, String cacheName, InfinispanDSLConnection conn) throws TranslatorException {
			
		String pkField = conn.getPkField();
		boolean updatable = (materialized ? true : (pkField != null ? true : false));
		
		rootTable = addTable(mf, entity, updatable, false);
		if (materialized) {
			stagingTable = addTable(mf, entity, true, true);
		}
		
		if (classObjectColumn && !materialized) {
	    // add column for cache Object, set to non-selectable by default so that select * queries don't fail by default
			addRootColumn(mf, Object.class, entity, null, null, SearchType.Unsearchable, rootTable.getName(), rootTable, false, false, NullType.Nullable); //$NON-NLS-1$	
		}
		
		pkMethod = null;
		if (updatable) {
		    pkMethod = findMethod(entity.getName(), pkField, conn);
		    if (pkMethod == null) {
				throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25008, new Object[] {pkField, cacheName, entity.getName()}));
		    	
		    }	
		}
		
		boolean addKey = false;
		boolean registeredClass = false;
		// the descriptor is needed to determine which fields are defined as searchable.
		for (FieldDescriptor fd:descriptor.getFields()) {	
			if (fd.isRepeated() ) {
				descriptorMap.put(fd.getName(), fd);				
				continue;

			} 
				NullType nt = NullType.Nullable;
				SearchType st = isSearchable(fd);
				Class<?> returnType = getJavaType( fd,entity, conn);
//				String j = fd.getJavaType().name();
				if (fd.getName().equalsIgnoreCase(pkField)) {
					addKey = true;
					st = SearchType.Searchable;
					nt = NullType.No_Nulls;
				} else {
					registeredClass = (conn.getClassRegistry().getRegisteredClass(returnType.getName()) != null ? true : false);
					if (registeredClass) {		
						descriptorMap.put(fd.getName(), fd);						
						continue;
					}

					if (fd.isRequired()) {
						nt = NullType.No_Nulls;
					}
				}
				// dont make primary key updatable, the object must be deleted and readded in order to change the key
				addRootColumn(mf, returnType, getProtobufNativeType(fd), fd.getFullName(), fd.getName(), st, rootTable.getName(), rootTable, true, true, nt);	
				if (materialized) {
					addRootColumn(mf, returnType, getProtobufNativeType(fd), fd.getFullName(), fd.getName(), st, stagingTable.getName(), stagingTable, true, true, nt);	
					
				}
		}
			
		if (pkMethod != null) {
			@SuppressWarnings("null")
			String pkName = "PK_" + pkField.toUpperCase(); //$NON-NLS-1$
	        ArrayList<String> x = new ArrayList<String>(1) ;
	        x.add(pkField);
	        mf.addPrimaryKey(pkName, x , rootTable);	
			if (materialized) {
		        mf.addPrimaryKey(pkName, x , stagingTable);
			}

		}
	
		if (!addKey) {
				addRootColumn(mf, pkMethod.getReturnType(), pkMethod.getReturnType(), pkField, pkField, SearchType.Searchable, rootTable.getName(), rootTable, true, true, NullType.No_Nulls);
		}
		
		for (String key : descriptorMap.keySet()) {
			FieldDescriptor fd = descriptorMap.get(key);
			Descriptor d = fd.getMessageType();
			createInnerTable(mf, d, key, rootTable, pkMethod, conn);

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
	
	private  boolean doesStagingTableExist(MetadataFactory mf, Class<?> entity) {
		String tName = entity.getSimpleName();
		tName = "ST_" + tName; //$NON-NLS-1$
		Table t = mf.getSchema().getTable(tName);
		if (t != null) {
			//already loaded
			return true;
		}
		return false;

	}
	
    private static Method findMethod(String className, String methodName, InfinispanDSLConnection conn) throws TranslatorException {
        Map<String, Method> mapMethods = conn.getClassRegistry().getReadClassMethods(className);

        
        Method m = ClassRegistry.findMethod(mapMethods, methodName, className);
        		//mapMethods.get(methodName);
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
    private void createInnerTable(MetadataFactory mf, Descriptor desc, String parentColumnRef, Table rootTable, Method pkMethod, InfinispanDSLConnection conn) throws TranslatorException  {
    	Class<?> c = getRegisteredClass(desc.getName(), conn);
 
    	// get the root class
		Class<?> pc = conn.getCacheClassType();
		
		String fd_Name = parentColumnRef;

		Table t = addTable(mf, c, rootTable.supportsUpdate(), false);

		List<FieldDescriptor> fields = desc.getFields();
		for (FieldDescriptor f:fields) {
			
			final SearchType st = isSearchable(f);

			// need to use the repeated descriptor, fd, as the prefix to the NIS in order to perform query
			addSubColumn(mf, getJavaType(f, c, conn),  getProtobufNativeType(f), f, st, (fd_Name == null ? f.getContainingMessage().getName() : fd_Name), t, true, rootTable.supportsUpdate(), (f.isRequired() ? NullType.No_Nulls  : NullType.Nullable) );	
		}
		
		if (pkMethod != null) {
					
			// if not repeatable, then use the attribute name
			String mName =  findMethodNameForReturnType(pc.getName(), c, fd_Name, conn);
			
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
			
    		addRootColumn(mf, pkMethod.getReturnType(), pkMethod.getReturnType(), methodName, methodName, SearchType.Searchable, t.getName(), t, false, true, NullType.No_Nulls);
			ForeignKey fk = mf.addForiegnKey(fkName, keyColumns, referencedKeyColumns, rootTable.getName(), t);
			
			fk.setNameInSource(mName);

		}		

    }
	
    /*
     * This is used to find the method name to call on the parent object to obtain the child reference object(s)
     */
    private  String findMethodNameForReturnType(String parentClassName, Class<?> childClzz, String methodName, InfinispanDSLConnection conn) throws TranslatorException {
        if (methodName == null || methodName.length() == 0) {
            return null;
        }
        
        Map<String, Method> mapMethods = conn.getClassRegistry().
        		getReadClassMethods(parentClassName);
        
        Method m = ClassRegistry.findMethod(mapMethods, methodName, parentClassName);
        
        if (m != null) return ObjectUtil.getNameFromMethodName(m.getName().toLowerCase()); 

        String methodKey = ObjectUtil.getNameFromMethodName(methodName.toLowerCase());        

        for (String k:mapMethods.keySet()) {
			// because parent methods can have plural names (i.e., add 's')
				
			String nk = ObjectUtil.getNameFromMethodName(k.toLowerCase());
			if (nk.startsWith(methodKey)) {
				return nk;
			}
        }
        
        // try to match the return generic type (if defined) to the child class
		for (Iterator it = mapMethods.keySet().iterator(); it.hasNext();) {
			String k = (String) it.next();
			m = mapMethods.get(k);
			
			if (Collection.class.isAssignableFrom(m.getReturnType()) || m.getReturnType().isArray()) {
		        Type returnType = m.getGenericReturnType();

		        if(returnType instanceof ParameterizedType){
		            ParameterizedType type = (ParameterizedType) returnType;
		            Type[] typeArguments = type.getActualTypeArguments();
		            for(Type typeArgument : typeArguments){
		                Class typeArgClass = (Class) typeArgument;
		                if (typeArgument.equals(childClzz)) {
		                	return ObjectUtil.getNameFromMethodName(m.getName()).toLowerCase();

		                }
		                
		                
		            }
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
	
	private static Class<?> getJavaType(FieldDescriptor fd, Class<?> c, InfinispanDSLConnection conn) throws TranslatorException {

			Method m = findMethod(c.getName(), fd.getName(), conn);
			return m.getReturnType();
	}
	/*
	 * See https://developers.google.com/protocol-buffers/docs/proto#scalar
	 */
	private static Class<?> getProtobufNativeType(FieldDescriptor fd) {

		String n = fd.getJavaType().name();

		
		
		if (JavaType.STRING.name().equals(n)) {
			return String.class;
		}
		
		if (JavaType.BOOLEAN.name().equals(n)) {
			return boolean.class;
		}
		
		if (JavaType.LONG.name().equals(n)) {
			return long.class;
		}
		
		if (JavaType.INT.name().equals(n)) {
			return int.class;
		}
		if (JavaType.DOUBLE.name().equals(n)) {
			return double.class;
		}
		
		if (JavaType.FLOAT.name().equals(n)) {
			return float.class;
		}
		
		if (JavaType.BYTE_STRING.name().equals(n)) {
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