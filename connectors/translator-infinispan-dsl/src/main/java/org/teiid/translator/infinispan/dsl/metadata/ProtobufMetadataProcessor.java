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

import java.util.List;
import java.util.Map;

import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.infinispan.dsl.InfinispanConnection;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;


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
 * contained within the root class will be assigned based on this format:  "getMethod" name + "." + attribute
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
	public static final String KEY_ASSOSIATED_WITH_FOREIGN_TABLE = "assosiated_with_table";  //$NON-NLS-1$
	public static final String ENTITYCLASS= "entity_class"; //$NON-NLS-1$
	
	public static final String GET = "get"; //$NON-NLS-1$
	public static final String IS = "is"; //$NON-NLS-1$
		
	public static final String VIEWTABLE_SUFFIX = "View"; //$NON-NLS-1$
	public static final String OBJECT_COL_SUFFIX = "Object"; //$NON-NLS-1$
	
	protected boolean isUpdatable = false;
	
	@Override
	public void process(MetadataFactory metadataFactory, InfinispanConnection conn) throws TranslatorException {
		
    		Map<String, Class<?>> cacheTypes = conn.getCacheNameClassTypeMapping();
    		for (String cacheName : cacheTypes.keySet()) {

    			Class<?> type = cacheTypes.get(cacheName);
    			String pkField = conn.getPkField(cacheName);
    			createSourceTable(metadataFactory, type, conn.getDescriptor(cacheName), cacheName, pkField);
    		}
	}

	private Table createSourceTable(MetadataFactory mf, Class<?> entity, Descriptor descriptor, String cacheName, String pkField) {
		
		String rootTableName = getTableName(descriptor);
		Table rootTable = mf.getSchema().getTable(rootTableName);
		if (rootTable != null) {
			//already loaded
			return rootTable;
		}
		rootTable = mf.addTable(rootTableName);
		rootTable.setSupportsUpdate(isUpdateable());
		rootTable.setNameInSource(cacheName); 

		rootTable.setProperty(ENTITYCLASS, descriptor.getFullName());	

		addColumn(mf, Object.class, null, SearchType.Unsearchable, rootTableName, rootTable,true); //$NON-NLS-1$
		
		List<FieldDescriptor> fields = descriptor.getFields();
		for (FieldDescriptor fd:fields) {			
			addColumn(mf, getJavaType(fd), fd, SearchType.Searchable, rootTableName, rootTable, true);	
		}

		processDescriptor(mf, descriptor, rootTable);
				
		return rootTable;
	}
	
	private void processDescriptor(MetadataFactory mf, Descriptor descriptor, Table parentTable) {
		List<Descriptor>descriptors =descriptor.getNestedTypes();
		if (descriptors == null || descriptors.isEmpty()) return;
		
		for (Descriptor d:descriptors) {
			addDescriptor(mf, d, parentTable);
			
			processDescriptor(mf, d, parentTable);
		}
	}
	
	private void addDescriptor(MetadataFactory mf, Descriptor descriptor, Table parentTable) {
		
		List<FieldDescriptor> fields = descriptor.getFields();
		for (FieldDescriptor fd:fields) {
			addColumn(mf, getJavaType(fd), fd, SearchType.Searchable, getTableName(descriptor), parentTable, true);	
		}

	}
	
	/**
	 * Call to get the name of table based on the <code>Class</code> entity
	 * @param entity
	 * @return String name of table
	 */
	protected String getTableName(Class<?> entity) {
		return entity.getSimpleName();
	}
	
	protected String getTableName(Descriptor descriptor) {
		return descriptor.getName();
	}
	
	/**
	 * @return boolean
	 */
	protected boolean isUpdateable() {
		return this.isUpdatable;
	}

	protected Column addColumn(MetadataFactory mf, Class<?> type, FieldDescriptor fd,
			 SearchType searchType, String entityName, Table parentTable, boolean selectable) {
		String attributeName;
		String nis;
		
		// Null fd indicates this is for the object
		if (fd != null) {
			String fn = fd.getFullName();
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
			if (parentTable.getName().equalsIgnoreCase(entityName)) {
				nis = fd.getName();
			} else {
				idx = fn.indexOf(entityName);
				nis = fd.getFullName().substring(idx);
			}

			attributeName = fd.getName();
		} else {
			attributeName = entityName + OBJECT_COL_SUFFIX;
			nis = "this";
		}
	

		Column c = parentTable.getColumnByName(attributeName);
		if (c != null) {
			return c;
		}
		c = mf.addColumn(attributeName, TypeFacility.getDataTypeName(TypeFacility.getRuntimeType(type)), parentTable);
		
		if (nis != null) {
			c.setNameInSource(nis);
		}
		
		c.setUpdatable(isUpdateable());
		c.setSearchType(searchType);
		c.setNativeType(type.getName());
		c.setSelectable(selectable);


		if (type.isPrimitive()) {
			c.setNullType(NullType.No_Nulls);
		}
		return c;
	}
	
	protected Class<?> getJavaType(FieldDescriptor fd) {
		
		   switch (fd.getJavaType()) {
		      case INT:         return Integer.class   ; 
		      case LONG:        return Long.class      ; 
		      case FLOAT:       return Float.class     ; 
		      case DOUBLE:      return Double.class    ; 
		      case BOOLEAN:     return Boolean.class   ; 
		      case STRING:      return String.class    ; 
		      case BYTE_STRING: return String.class    ; 
		      case ENUM:  		return String.class 	;
		      case MESSAGE:  	return String.class 	;
		      default:
		      	
		        return String.class;
		   }
	}
	
}
