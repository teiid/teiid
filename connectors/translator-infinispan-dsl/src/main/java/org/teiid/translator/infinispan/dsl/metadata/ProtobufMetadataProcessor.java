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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.infinispan.dsl.InfinispanConnection;
import org.teiid.translator.infinispan.dsl.InfinispanPlugin;

import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;



/**
 * The ProtobufMetadataProcess is the logic for providing metadata to the translator based on
 * the google protobuf Descriptors and the defined class types.
 * 
 * <p>
 * Here are the rules that are being followed.
 * </p>
 * <li>Cache</li>
 * Each cache defined by the connection will be processed to create 1 or more tables.
 * <li>Table</li>
 * Each class is mapped to a table.
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
public class ProtobufMetadataProcessor implements MetadataProcessor<InfinispanConnection>{
		
	public static final String GET = "get"; //$NON-NLS-1$
	public static final String IS = "is"; //$NON-NLS-1$
		
	public static final String VIEWTABLE_SUFFIX = "View"; //$NON-NLS-1$
	public static final String OBJECT_COL_SUFFIX = "Object"; //$NON-NLS-1$

	
	@Override
	public void process(MetadataFactory metadataFactory, InfinispanConnection conn) throws TranslatorException {
			
		Map<String, Class<?>> cacheTypes = conn.getCacheNameClassTypeMapping();
		for (String cacheName : cacheTypes.keySet()) {

			Class<?> type = cacheTypes.get(cacheName);
			String pkField = conn.getPkField(cacheName);
			if (pkField == null || pkField.trim().length() == 0) {
					throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25007, new Object[] {cacheName}));
			}
			createRootTable(metadataFactory, type, conn.getDescriptor(cacheName), cacheName, pkField, conn);
		}		
	}


	private Table createRootTable(MetadataFactory mf, Class<?> entity, Descriptor descriptor, String cacheName, String pkField, InfinispanConnection conn) throws TranslatorException {
			
		Table rootTable = addTable(mf, entity, descriptor);
		rootTable.setNameInSource(cacheName); 
		
	    Method pkMethod = conn.getClassRegistry().getReadClassMethods(entity.getName()).get(pkField);
	    if (pkMethod == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25008, new Object[] {pkField, cacheName, entity.getName()}));
	    	
	    }
	    	    
	    // add column for cache Object
		addRootColumn(mf, Object.class, null, null, SearchType.Unsearchable, rootTable.getName(), rootTable,true, false); //$NON-NLS-1$	

		// add a column for primary key
		addRootColumn(mf, pkMethod.getReturnType(), pkField, pkField, SearchType.Searchable, rootTable.getName(), rootTable, true, true);
		
		boolean repeats = false;
		for (FieldDescriptor fd:descriptor.getFields()) {	
			if (fd.isRepeated() ) {
				repeats = true;
			} else {
				SearchType st = isSearchable(fd);

				addRootColumn(mf, getJavaType(fd), fd, st, rootTable.getName(), rootTable, true, true);	
			}
		}	
		
		// add primary key
		String pkName = "PK_" + pkField.toUpperCase(); //$NON-NLS-1$
        ArrayList<String> x = new ArrayList<String>(1) ;
        x.add(pkField);
        mf.addPrimaryKey(pkName, x , rootTable);		    
		
		if (repeats) {
			for (FieldDescriptor fd:descriptor.getFields()) {	
				if (fd.isRepeated() ) {
					processRepeatedType(mf,fd, rootTable, pkMethod, conn);	
				} 
			}	
		}

			
		return rootTable;
	}
	
	private Table addTable(MetadataFactory mf, Class<?> entity, Descriptor descriptor) {
		String tName = entity.getSimpleName();
				//getTableName(descriptor);
		Table t = mf.getSchema().getTable(tName);
		if (t != null) {
			//already loaded
			return t;
		}
		t = mf.addTable(tName);
		t.setSupportsUpdate(true);

		return t;
		
	}
	
    private  String findRepeatedMethodName(String className, String methodName, InfinispanConnection conn) throws TranslatorException {
        if (methodName == null || methodName.length() == 0) {
            return null;
        }
        
        Map<String, Method> mapMethods = conn.getClassRegistry().getReadClassMethods(className);
        
        // because the class 'methods' contains 2 different references
        //  get'Name'  and 'Name', this will look for the 'Name' version
        for (Iterator it=mapMethods.keySet().iterator(); it.hasNext();) {
        	String mName = (String) it.next();
        	if (mName.toLowerCase().startsWith(methodName.toLowerCase()) ) {
        		Method m = mapMethods.get(mName);
        		Class<?> c = m.getReturnType();
        		if (Collection.class.isAssignableFrom(m.getReturnType()) || m.getReturnType().isArray()) {
        			return mName;
        		}
        	} 
        }
        return null;
    }	
	
	private void processRepeatedType(MetadataFactory mf, FieldDescriptor fd, Table rootTable, Method pkMethod, InfinispanConnection conn) throws TranslatorException  {
		
		Descriptor d = fd.getMessageType();
		Descriptor parent = d.getContainingType();
		// Need to find the method name that corresponds to the repeating attribute
		// so that the actual method name can be used in defining the NIS
		// which will provide the correct method to use when retrieving the data at execution time

		Class<?> pc = getRegisteredClass(parent.getName(), conn);
		Class<?> c = getRegisteredClass(fd.getMessageType().getName(), conn);

		String mName = findRepeatedMethodName(pc.getName(), fd.getName(), conn);
		
		if (mName == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25060, new Object[] { fd.getName() }));
		}
		
		Table t = addTable(mf, c, d);
		
		// do not use getSourceName, the NameInSource has to be defined as the cache name
		t.setNameInSource(rootTable.getNameInSource()); 
			
		List<FieldDescriptor> fields = fd.getMessageType().getFields();

		for (FieldDescriptor f:fields) {
			final SearchType st = isSearchable(f);
			// need to use the repeated descriptor, fd, as the prefix to the NIS in order to perform query
			addSubColumn(mf, getJavaType(f), f, st, fd.getName(), t, true, true);	
		}
		
		if (pkMethod != null) {
			// use the same parent table primary ke column name in the foreign key tables
			String methodName = rootTable.getPrimaryKey().getColumns().get(0).getName();
			List<String> keyColumns = new ArrayList<String>();
			keyColumns.add(methodName);
			List<String> referencedKeyColumns = new ArrayList<String>();
			referencedKeyColumns.add(methodName);
			String fkName = "FK_" + rootTable.getName().toUpperCase();
    		addRootColumn(mf, pkMethod.getReturnType(), methodName, methodName, SearchType.Searchable, t.getName(), t, false, false);
			ForeignKey fk = mf.addForiegnKey(fkName, keyColumns, referencedKeyColumns, rootTable.getName(), t);
			fk.setNameInSource(mName);

		}
	}	
	
	private Class<?> getRegisteredClass(String name, InfinispanConnection conn) throws TranslatorException {
		List<Class<?>> registeredClasses = conn.getClassRegistry().getRegisteredClasses();
		for (Class<?> c:registeredClasses) {
			if (c.getName().endsWith(name)) {
				return c;
			}
		}
		
		throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25003, new Object[] { name }));
	}	

	private Column addRootColumn(MetadataFactory mf, Class<?> type, FieldDescriptor fd,
			 SearchType searchType, String entityName, Table rootTable, boolean selectable, boolean updateable) {
		
		return addRootColumn(mf, type, fd.getFullName(), fd.getName(), searchType, entityName, rootTable, selectable, updateable);

	}
	private Column addRootColumn(MetadataFactory mf, Class<?> type, String columnFullName, String columnName,
			 SearchType searchType, String entityName, Table rootTable, boolean selectable, boolean updateable) {
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
	
		return addColumn(mf, type, attributeName, nis, searchType, rootTable, selectable, updateable);

	}
	
	private Column addSubColumn(MetadataFactory mf, Class<?> type, FieldDescriptor fd,
			 SearchType searchType, String nisPrefix, Table rootTable, boolean selectable, boolean updateable) {
		String attributeName = fd.getName();
		String nis = nisPrefix + "." + fd.getName();

		return addColumn(mf, type, attributeName, nis, searchType, rootTable, selectable, updateable);

	}	
	
	private Column addColumn(MetadataFactory mf, Class<?> type, String attributeName, String nis, SearchType searchType, Table rootTable, boolean selectable, boolean updateable) {
		if (rootTable.getColumnByName(attributeName) != null) return rootTable.getColumnByName(attributeName);
		
		Column c = mf.addColumn(attributeName, TypeFacility.getDataTypeName(TypeFacility.getRuntimeType(type)), rootTable);
		
		if (nis != null) {
			c.setNameInSource(nis);
		}
		
		c.setUpdatable(updateable);
		c.setSearchType(searchType);
		c.setNativeType(type.getName());
		c.setSelectable(selectable);


		if (type.isPrimitive()) {
			c.setNullType(NullType.No_Nulls);
		}
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
	
	private static Class<?> getJavaType(FieldDescriptor fd) {
		
		   switch (fd.getJavaType()) {
		      case INT:         return Integer.class   ; 
		      case LONG:        return Long.class      ; 
		      case FLOAT:       return Float.class     ; 
		      case DOUBLE:      return Double.class    ; 
		      case BOOLEAN:     return Boolean.class   ; 
		      case STRING:      return String.class    ; 
		      case BYTE_STRING: return byte[].class    ; 
		      case ENUM:  		return String.class 	;
		      case MESSAGE:  	return String.class 	;
		      default:
		      	
		        return String.class;
		   }
	}
	
}
