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
package org.teiid.translator.jpa;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import javax.persistence.EntityManager;
import javax.persistence.metamodel.*;
import javax.persistence.metamodel.Type.PersistenceType;

import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.*;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

/**
 * TODO: 
 *  - support of abstract entities is an issue, should we represent base and extended types, just extended types?
 * 
 */
@SuppressWarnings("nls")
public class JPAMetadataProcessor implements MetadataProcessor<EntityManager> {
    
    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Foriegn Table Name", description="Applicable on Forign Key columns")
	public static final String KEY_ASSOSIATED_WITH_FOREIGN_TABLE = MetadataFactory.JPA_URI+"assosiated_with_table";

    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Entity Class", description="Java Entity Class that represents this table", required=true)
	public static final String ENTITYCLASS= MetadataFactory.JPA_URI+"entity_class";
	
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

	public void process(MetadataFactory mf, EntityManager entityManager) throws TranslatorException {
		Metamodel model = entityManager.getMetamodel();
		
		Set<EntityType<?>> entities = model.getEntities();
		for (EntityType<?> entity:entities) {
			addEntity(mf, model, entity);
		}
		
		// take a second swipe and add Foreign Keys
		for (EntityType<?> entity:entities) {
			Table t = mf.getSchema().getTable(entity.getName());
			addForeignKeys(mf, model, entity, t);
		}		
	}

	private Table addEntity(MetadataFactory mf, Metamodel model, EntityType<?> entity) throws TranslatorException {
		Table table = mf.getSchema().getTable(entity.getName());
		if (table == null) {			
			table = mf.addTable(entity.getName());
			table.setSupportsUpdate(true);
			table.setProperty(ENTITYCLASS, entity.getJavaType().getCanonicalName());
			addPrimaryKey(mf, model, entity, table);
			addSingularAttributes(mf, model, entity, table);
		}
		return table;
	}
	
	private boolean columnExists(String name, Table table) {
		return table.getColumnByName(name) != null;
	}
	
	private Column addColumn(MetadataFactory mf, String name, String type, Table entityTable) throws TranslatorException {
		if (!columnExists(name, entityTable)) {
			Column c = mf.addColumn(name, type, entityTable);
			c.setUpdatable(true);
			return c;
		}
		return entityTable.getColumnByName(name);
	}
	
	private void addForiegnKey(MetadataFactory mf, String name, List<String> columnNames, String referenceTable, Table table) throws TranslatorException {
		ForeignKey fk = mf.addForiegnKey("FK_"+name, columnNames, referenceTable, table);
		for (String column:columnNames) {
			Column c = table.getColumnByName(column);
			c.setProperty(KEY_ASSOSIATED_WITH_FOREIGN_TABLE, mf.getName()+Tokens.DOT+referenceTable);
		}
		fk.setNameInSource(name);
	}

	private void addSingularAttributes(MetadataFactory mf, Metamodel model, ManagedType<?> entity, Table entityTable) throws TranslatorException {
		for (Attribute<?, ?> attr:entity.getAttributes()) {
			if (!attr.isCollection()) {
				boolean simpleType = isSimpleType(attr.getJavaType());
				if (simpleType) {
					Column column = addColumn(mf, attr.getName(), TypeFacility.getDataTypeName(getJavaDataType(attr.getJavaType())), entityTable);
					if (((SingularAttribute)attr).isOptional()) {
						column.setDefaultValue(null);
					}
				} 
				else {
					boolean classFound = false;
					// If this attribute is a @Embeddable then add its columns as
					// this tables columns
					for (EmbeddableType<?> embeddable:model.getEmbeddables()) {
						if (embeddable.getJavaType().equals(attr.getJavaType())) {
							addSingularAttributes(mf, model, embeddable, entityTable);
							classFound = true;
							break;
						}
					}
					
					if (!classFound) {
						// if the attribute is @Entity then add entity's PK as column on this
						// table, then add that column as FK 
						for (EntityType et:model.getEntities()) {
							if (et.getJavaType().equals(attr.getJavaType())) {
								Table attributeTable = addEntity(mf, model, et);
								KeyRecord pk = attributeTable.getPrimaryKey();
								if (pk != null) { // TODO: entities must have PK, so this check is not needed.
									ArrayList<String> keys = new ArrayList<String>();
									for (Column column:pk.getColumns()) {
										addColumn(mf, column.getName(), column.getDatatype().getRuntimeTypeName(), entityTable);
										keys.add(column.getName());
									}
									if (!foreignKeyExists(keys, entityTable)) {
										addForiegnKey(mf, attr.getName(), keys, attributeTable.getName(), entityTable);
									}
								}
								else {
									throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14001, attributeTable.getName()));
								}
								classFound = true;
								break;
							}
						}
					}
					
					if (!classFound) {
						throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14002, attr.getName()));
					}
				}
			}
		}
	}
	
	private boolean isSimpleType(Class type) {
		return type.isPrimitive() || type.equals(String.class)
				|| type.equals(BigDecimal.class) || type.equals(Date.class)
				|| type.equals(BigInteger.class)
				|| map.containsKey(type);
	}
	
	private void addForeignKeys(MetadataFactory mf, Metamodel model, ManagedType<?> entity, Table entityTable) throws TranslatorException {
		for (Attribute<?, ?> attr:entity.getAttributes()) {
			if (attr.isCollection()) {
				
				PluralAttribute pa = (PluralAttribute)attr;
				Table forignTable = null;
				
				for (EntityType et:model.getEntities()) {
					if (et.getJavaType().equals(pa.getElementType().getJavaType())) {
						forignTable = mf.getSchema().getTable(et.getName());
						break;
					}
				}
				
				if (forignTable == null) {
					continue;
				}
				
				// add foreign keys as columns in table first; check if they exist first
				ArrayList<String> keys = new ArrayList<String>();
				KeyRecord pk = entityTable.getPrimaryKey();
				for (Column entityColumn:pk.getColumns()) {
					addColumn(mf, entityColumn.getName(), entityColumn.getDatatype().getRuntimeTypeName(), forignTable);
					keys.add(entityColumn.getName());
				}

				if (!foreignKeyExists(keys, forignTable)) {
					addForiegnKey(mf, attr.getName(), keys, entityTable.getName(), forignTable);
				}
			}
		}
	}
	
	private boolean foreignKeyExists(List<String> keys, Table forignTable) {
		boolean fkExists = false;
		for (ForeignKey fk:forignTable.getForeignKeys()) {
			boolean allKeysFound = true;
			for (String key:keys) {
				boolean keyfound = false;
				for (Column col:fk.getColumns()) {
					if (col.getName().equals(key)) {
						keyfound = true;
						break;
					}
				}
				if (!keyfound) {
					allKeysFound = false;
					break;
				}
			}
			
			if (allKeysFound) {
				fkExists = true;
				break;
			}
		}
		return fkExists;
	}	

	private void addPrimaryKey(MetadataFactory mf, Metamodel model, EntityType<?> entity, Table entityTable) throws TranslatorException {
		// figure out the PK
		if (entity.hasSingleIdAttribute()) {
			if (entity.getIdType().getPersistenceType().equals(PersistenceType.BASIC)) {
				SingularAttribute<?, ?> pkattr = entity.getId(entity.getIdType().getJavaType());
				addColumn(mf, pkattr.getName(), TypeFacility.getDataTypeName(getJavaDataType(pkattr.getJavaType())), entityTable);
				mf.addPrimaryKey("PK_"+entity.getName(), Arrays.asList(pkattr.getName()), entityTable); //$NON-NLS-1$
			}
			else if (entity.getIdType().getPersistenceType().equals(PersistenceType.EMBEDDABLE)) {
				SingularAttribute<?, ?> pkattr = entity.getId(entity.getIdType().getJavaType());
				for (EmbeddableType<?> embeddable:model.getEmbeddables()) {
					if (embeddable.getJavaType().equals(pkattr.getJavaType())) {
						addSingularAttributes(mf, model, embeddable, entityTable);
						ArrayList<String> keys = new ArrayList<String>();
						for (Attribute<?, ?> attr:embeddable.getAttributes()) {
							if (isSimpleType(attr.getJavaType())) {
								keys.add(attr.getName());
							}
							else {
								throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14003, entityTable.getName()));
							}
						}
						mf.addPrimaryKey("PK_"+pkattr.getName(), keys, entityTable);
						break;
					}
				}			
			}
		}
		else {
			// Composite PK. If the PK is specified with @IdClass then read its attributes,
			// if those attributes are not found, add them as columns then as composite PK
			ArrayList<String> keys = new ArrayList<String>();
			for (Object obj:entity.getIdClassAttributes()) {
				SingularAttribute<?, ?> attr = (SingularAttribute)obj;
				addColumn(mf, attr.getName(), TypeFacility.getDataTypeName(getJavaDataType(attr.getJavaType())), entityTable);
				keys.add(attr.getName());
			}
			mf.addPrimaryKey("PK_"+entity.getName(), keys, entityTable);
		}
	}
	
	private Class getJavaDataType(Class type) {
		if (type.equals(Date.class)) {
			return java.sql.Timestamp.class;
		}
		
		if (type.isPrimitive()) {
			return map.get(type);  // usage			
		}
		return type;
	}
	
}
