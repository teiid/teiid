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
package org.teiid.olingo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.Target;
import org.apache.olingo.server.api.edm.provider.Action;
import org.apache.olingo.server.api.edm.provider.ActionImport;
import org.apache.olingo.server.api.edm.provider.ComplexType;
import org.apache.olingo.server.api.edm.provider.EntityContainer;
import org.apache.olingo.server.api.edm.provider.EntitySet;
import org.apache.olingo.server.api.edm.provider.EntityType;
import org.apache.olingo.server.api.edm.provider.NavigationProperty;
import org.apache.olingo.server.api.edm.provider.NavigationPropertyBinding;
import org.apache.olingo.server.api.edm.provider.Parameter;
import org.apache.olingo.server.api.edm.provider.Property;
import org.apache.olingo.server.api.edm.provider.PropertyRef;
import org.apache.olingo.server.api.edm.provider.ReferentialConstraint;
import org.apache.olingo.server.api.edm.provider.ReturnType;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;

public class OData4EntitySchemaBuilder {
	
	public static org.apache.olingo.server.api.edm.provider.Schema buildMetadata(org.teiid.metadata.Schema teiidSchema) {
		try {
		    org.apache.olingo.server.api.edm.provider.Schema edmSchema = new org.apache.olingo.server.api.edm.provider.Schema();

		    buildEntityTypes(teiidSchema, edmSchema);
			buildActions(teiidSchema, edmSchema);	

			return edmSchema;
		} catch (Exception e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	static EntitySet findEntitySet(org.apache.olingo.server.api.edm.provider.Schema edmSchema, String enitityName) {
		EntityContainer entityContainter = edmSchema.getEntityContainer();
		for (EntitySet entitySet:entityContainter.getEntitySets()) {
			if (entitySet.getName().equalsIgnoreCase(enitityName)) {
				return entitySet;
			}
		}
		return null;
	}
	
	static org.apache.olingo.server.api.edm.provider.Schema findSchema(Map<String, org.apache.olingo.server.api.edm.provider.Schema> edmSchemas, String schemaName) {
		return edmSchemas.get(schemaName);
	}	
	
	static EntityType findEntityType(Map<String, org.apache.olingo.server.api.edm.provider.Schema> edmSchemas, String schemaName, String enitityName) {
		org.apache.olingo.server.api.edm.provider.Schema schema = findSchema(edmSchemas, schemaName);
		if (schema != null) {
			for (EntityType type:schema.getEntityTypes()) {
				if (type.getName().equalsIgnoreCase(enitityName)) {
					return type;
				}
			}
		}
		return null;
	}	
	
	static EntityContainer findEntityContainer(Map<String, org.apache.olingo.server.api.edm.provider.Schema> edmSchemas, String schemaName) {
		org.apache.olingo.server.api.edm.provider.Schema schema = edmSchemas.get(schemaName);
		return schema.getEntityContainer();
	}	

	public static void buildEntityTypes(Schema schema, org.apache.olingo.server.api.edm.provider.Schema edmSchema) {
		List<EntitySet> entitySets = new ArrayList<EntitySet>();
		List<EntityType> entityTypes = new ArrayList<EntityType>();
	    
		for (Table table: schema.getTables().values()) {
			
			// skip if the table does not have the PK or unique
			KeyRecord primaryKey = table.getPrimaryKey();
			List<KeyRecord> uniques = table.getUniqueKeys();
			if (primaryKey == null && uniques.isEmpty()) {
				LogManager.logDetail(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16017, table.getFullName()));
				continue;
			}

			String entityTypeName = table.getName();
	    	EntityType entityType = new EntityType()
	    		.setName(entityTypeName);
			
			// adding properties
	    	List<Property> properties = new ArrayList<Property>();
			for (Column c : table.getColumns()) {
				properties.add(buildProperty(c));
			}
			entityType.setProperties(properties);
			if (hasStream(properties)) {
				entityType.setHasStream(true);
			}
			
			// set keys
			ArrayList<PropertyRef> keyProps = new ArrayList<PropertyRef>();
	    	if (primaryKey != null) {
				for (Column c : primaryKey.getColumns()) {					
					keyProps.add(new PropertyRef().setPropertyName(c.getName()));
				}
	    	}
	    	else {
				for (Column c : uniques.get(0).getColumns()) {
					keyProps.add(new PropertyRef().setPropertyName(c.getName()));
				}
	    	}	
	    	entityType.setKey(keyProps);
			
							
			// entity set one for one entity type
			EntitySet entitySet = new EntitySet()
					.setName(table.getName())
					.setType(new FullQualifiedName(schema.getName(), table.getName()))
					.setIncludeInServiceDocument(true);

			
			buildNavigationProperties(table, entityType, entitySet);
			
			// add entity types for entity schema
			entityTypes.add(entityType);
			entitySets.add(entitySet);
	    }
		
		// entity container is holder entity sets, association sets, function imports
		EntityContainer entityContainer = new EntityContainer()
				.setName(schema.getName())
				.setEntitySets(entitySets);
		
		// build entity schema
		edmSchema.setNamespace(schema.getName())
				.setAlias(schema.getName())
				.setEntityTypes(entityTypes)
				.setEntityContainer(entityContainer);
	}

	private static boolean hasStream(List<Property> properties) {
		for (Property p:properties) {
			if (p.getType().equals(EdmPrimitiveTypeKind.Binary.getFullQualifiedName())) {
				return true;
			}
		}
		return false;
	}

	private static Property buildProperty(Column c) {
		Property property = new Property()
				.setName(c.getName())
				.setType(ODataTypeManager.odataType(c.getRuntimeType()).getFullQualifiedName())
				.setNullable(c.getNullType() == NullType.Nullable);
		
		if (DataTypeManager.isArrayType(c.getRuntimeType())) {
			property.setCollection(true);
		}
		
		if (c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.STRING)) {
			property.setMaxLength(c.getLength())
				.setUnicode(true);
		}
		else if(c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.DOUBLE)||c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.FLOAT)||c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.BIG_DECIMAL)) {
			property.setPrecision(c.getPrecision());
			property.setScale(c.getScale());
		}
		else {
			if (c.getDefaultValue() != null) {
				property.setDefaultValue(c.getDefaultValue());
			}			
		}
		return property;
	}	
	
	private static void buildNavigationProperties(Table table, EntityType entityType, EntitySet entitySet) {
		// skip if the table does not have the PK or unique
		KeyRecord primaryKey = table.getPrimaryKey();
		List<KeyRecord> uniques = table.getUniqueKeys();				
		if (primaryKey == null && uniques.isEmpty()) {
			return;
		}
		
		ArrayList<NavigationProperty> navigationProperties = new ArrayList<NavigationProperty>();
		ArrayList<NavigationPropertyBinding> navigationBindingProperties = new ArrayList<NavigationPropertyBinding>();
		
		// build Associations
		for (ForeignKey fk:table.getForeignKeys()) {
			String refSchemaName = fk.getReferenceKey().getParent().getParent().getName();
			
			// check to see if fk is part of this table's pk, then it is 1 to 1 relation
			boolean onetoone = sameColumnSet(table.getPrimaryKey(), fk);
			
			NavigationProperty navigaton = new NavigationProperty();
			navigaton.setName(fk.getName())
				.setType(new FullQualifiedName(refSchemaName, fk.getReferenceTableName()));
			
			if (!onetoone) {
				navigaton.setCollection(true);
			}
			else {
				navigaton.setNullable(false);
			}
			
			NavigationPropertyBinding navigationBinding = new NavigationPropertyBinding();
			navigationBinding.setPath(fk.getName());
			navigationBinding.setTarget(new Target().setTargetName(fk.getReferenceTableName()));
			
			ArrayList<ReferentialConstraint> constrainsts = new ArrayList<ReferentialConstraint>();
			for (int i = 0; i < fk.getColumns().size(); i++) {
				Column c = fk.getColumns().get(i);
				String refColumn = fk.getReferenceColumns().get(i);
				ReferentialConstraint constraint = new ReferentialConstraint();
				constraint.setProperty(c.getName());
				constraint.setReferencedProperty(refColumn);
			}
			navigaton.setReferentialConstraints(constrainsts);
			navigationProperties.add(navigaton);
			navigationBindingProperties.add(navigationBinding);
		}
		entityType.setNavigationProperties(navigationProperties);
		entitySet.setNavigationPropertyBindings(navigationBindingProperties);
	}	

	public static void buildActions(Schema schema, org.apache.olingo.server.api.edm.provider.Schema edmSchema) {
		// procedures
		ArrayList<ComplexType> complexTypes = new ArrayList<ComplexType>();
		ArrayList<Action> actions = new ArrayList<Action>();
		ArrayList<ActionImport> actionImports = new ArrayList<ActionImport>();
		
		for(Procedure proc:schema.getProcedures().values()) {
			
			Action edmAction = new Action();
			edmAction.setName(proc.getName());
			edmAction.setBound(false);
			
			ArrayList<Parameter> params = new ArrayList<Parameter>();
			for (ProcedureParameter pp:proc.getParameters()) {
				if (pp.getName().equals("return")) { //$NON-NLS-1$
					edmAction.setReturnType(new ReturnType().setType(ODataTypeManager.odataType(pp.getRuntimeType()).getFullQualifiedName()));
					continue;
				}
				
				Parameter param = new Parameter();
				param.setName(pp.getName());
				param.setType(ODataTypeManager.odataType(pp.getRuntimeType()).getFullQualifiedName());
				
				if (DataTypeManager.isArrayType(pp.getRuntimeType())) {
					param.setCollection(true);
				}
				param.setNullable(pp.getNullType() == NullType.Nullable);
				params.add(param);
			}
			edmAction.setParameters(params);
			
			// add a complex type for return resultset.
			ColumnSet<Procedure> returnColumns = proc.getResultSet();
			if (returnColumns != null) {
				ComplexType complexType = new ComplexType();
				String entityTypeName = proc.getName()+"_"+returnColumns.getName(); //$NON-NLS-1$
				complexType.setName(entityTypeName); 
				
				ArrayList<Property> props = new ArrayList<Property>();
				for (Column c:returnColumns.getColumns()) {
					props.add(buildProperty(c));
				}
				complexType.setProperties(props);
				
				complexTypes.add(complexType);
				edmAction.setReturnType((new ReturnType().setType(new FullQualifiedName(schema.getName(), complexType.getName())).setCollection(true)));
			}
			
			ActionImport actionImport = new ActionImport();
			actionImport.setName(proc.getName())
				.setAction(new FullQualifiedName(schema.getName(), proc.getName()));
			
			actions.add(edmAction);
			actionImports.add(actionImport);
		}
		edmSchema.setComplexTypes(complexTypes);
		edmSchema.setActions(actions);
		edmSchema.getEntityContainer().setActionImports(actionImports);
	}	
	
	static List<String> getColumnNames(List<Column> columns){
		ArrayList<String> names = new ArrayList<String>();
		for (Column c: columns) {
			names.add(c.getName());
		}
		return names;
	}
	
	static boolean sameColumnSet(KeyRecord recordOne, KeyRecord recordTwo) {
		
		if (recordOne == null || recordTwo == null) {
			return false;
		}
		
		List<Column> setOne = recordOne.getColumns();
		List<Column> setTwo = recordTwo.getColumns();
		
		if (setOne.size() != setTwo.size()) {
			return false;
		}
		for (int i = 0; i < setOne.size(); i++) {
			Column one = setOne.get(i);
			Column two = setTwo.get(i);
			if (!one.getName().equals(two.getName())) {
				return false;
			}
		}
		return true;
	}
}
