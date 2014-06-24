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
package org.teiid.translator.odata;

import java.util.ArrayList;
import java.util.List;

import org.odata4j.edm.*;
import org.odata4j.edm.EdmFunctionParameter.Mode;
import org.odata4j.edm.EdmProperty.CollectionKind;
import org.odata4j.edm.EdmSchema.Builder;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.*;

public class ODataEntitySchemaBuilder {
	
	public static EdmDataServices buildMetadata(MetadataStore metadataStore) {
		try {
			List<EdmSchema.Builder> edmSchemas = new ArrayList<EdmSchema.Builder>();
			for (Schema schema:metadataStore.getSchemaList()) {
				buildEntityTypes(schema, edmSchemas);
			}
			for (Schema schema:metadataStore.getSchemaList()) {
				buildFunctionImports(schema, edmSchemas);
			}
			for (Schema schema:metadataStore.getSchemaList()) {
				buildAssosiationSets(schema, edmSchemas);
			}
			return EdmDataServices.newBuilder().addSchemas(edmSchemas).build();
		} catch (Exception e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	public static EdmDataServices buildMetadata(Schema schema) {
		try {
			List<EdmSchema.Builder> edmSchemas = new ArrayList<EdmSchema.Builder>();
			buildEntityTypes(schema, edmSchemas);
			buildFunctionImports(schema, edmSchemas);
			buildAssosiationSets(schema, edmSchemas);
			return EdmDataServices.newBuilder().addSchemas(edmSchemas).build();
		} catch (Exception e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	static org.odata4j.edm.EdmEntitySet.Builder findEntitySet(List<Builder> edmSchemas, String schemaName, String enitityName) {
		for (EdmSchema.Builder modelSchema:edmSchemas) {
			if (modelSchema.getNamespace().equals(schemaName)) {
				for (EdmEntityContainer.Builder entityContainter:modelSchema.getEntityContainers()) {
					for(EdmEntitySet.Builder entitySet:entityContainter.getEntitySets()) {
						if (entitySet.getName().equals(enitityName)) {
							return entitySet;
						}
					}
				}
			}
		}
		return null;
	}
	
	static EdmSchema.Builder findSchema(List<Builder> edmSchemas, String schemaName) {
		for (EdmSchema.Builder modelSchema:edmSchemas) {
			if (modelSchema.getNamespace().equals(schemaName)) {
				return modelSchema;
			}
		}
		return null;
	}	
	
	static org.odata4j.edm.EdmEntityType.Builder findEntityType(List<Builder> edmSchemas, String schemaName, String enitityName) {
		for (EdmSchema.Builder modelSchema:edmSchemas) {
			if (modelSchema.getNamespace().equals(schemaName)) {
				for (EdmEntityType.Builder type:modelSchema.getEntityTypes()) {
					if (type.getName().equals(enitityName)) {
						return type;
					}
				}
			}
		}
		return null;
	}	
	
	static EdmEntityContainer.Builder findEntityContainer(List<Builder> edmSchemas, String schemaName) {
		for (EdmSchema.Builder modelSchema:edmSchemas) {
			if (modelSchema.getNamespace().equals(schemaName)) {
				for (EdmEntityContainer.Builder entityContainter:modelSchema.getEntityContainers()) {
					return entityContainter;
				}
			}
		}
		return null;
	}	
	
	public static void buildEntityTypes(Schema schema, List<EdmSchema.Builder> edmSchemas) {
		List<EdmEntitySet.Builder> entitySets = new ArrayList<EdmEntitySet.Builder>();
		List<EdmEntityType.Builder> entityTypes = new ArrayList<EdmEntityType.Builder>();
	    
		for (Table table: schema.getTables().values()) {
			
			// skip if the table does not have the PK or unique
			KeyRecord primaryKey = table.getPrimaryKey();
			List<KeyRecord> uniques = table.getUniqueKeys();
			if (primaryKey == null && uniques.isEmpty()) {
				LogManager.logDetail(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17017, table.getFullName()));
				continue;
			}

			String entityTypeName = table.getName();
			if (table.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false) != null) {
				entityTypeName = table.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
			}
	    	EdmEntityType.Builder entityType = EdmEntityType
					.newBuilder().setName(entityTypeName)
					.setNamespace(schema.getName());
			
	    	// adding key
	    	if (primaryKey != null) {
				for (Column c : primaryKey.getColumns()) {
					entityType.addKeys(c.getName());
				}
	    	}
	    	else {
				for (Column c : uniques.get(0).getColumns()) {
					entityType.addKeys(c.getName());
				}
	    	}
			
			// adding properties
			for (Column c : table.getColumns()) {
				EdmProperty.Builder property = EdmProperty.newBuilder(c.getName())
						.setType(ODataTypeManager.odataType(c.getRuntimeType()))
						.setNullable(c.getNullType() == NullType.Nullable);
				if (c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.STRING)) {
					property.setFixedLength(c.isFixedLength())
						.setMaxLength(c.getLength())
						.setUnicode(true);
				}
				entityType.addProperties(property);
			}
							
			// entity set one for one entity type
			EdmEntitySet.Builder entitySet = EdmEntitySet.newBuilder()
					.setName(table.getName())
					.setEntityType(entityType);
			
			entityType.setNamespace(schema.getName());
			entitySets.add(entitySet);

			// add enitity types for entity schema
			entityTypes.add(entityType);
	    }
		
		// entity container is holder entity sets, association sets, function imports
		EdmEntityContainer.Builder entityContainer = EdmEntityContainer
				.newBuilder().setName(schema.getName())
				.setIsDefault(false)
				.addEntitySets(entitySets);
		
		// build entity schema
		EdmSchema.Builder modelSchema = EdmSchema.newBuilder()
				.setNamespace(schema.getName())
				.addEntityTypes(entityTypes)
				.addEntityContainers(entityContainer);
		
		edmSchemas.add(modelSchema);
	}	
	
	private static String getEntityTypeName(Table t) {
		String entityTypeName = t.getName();
		if (t.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false) != null) {
			entityTypeName = t.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
		}
		return entityTypeName;
	}
	
	public static void buildAssosiationSets(Schema schema, List<Builder> edmSchemas) {
		EdmSchema.Builder odataSchema = findSchema(edmSchemas, schema.getName());
		EdmEntityContainer.Builder entityContainer = findEntityContainer(edmSchemas, schema.getName());
		List<EdmAssociationSet.Builder> assosiationSets = new ArrayList<EdmAssociationSet.Builder>();
		List<EdmAssociation.Builder> assosiations = new ArrayList<EdmAssociation.Builder>();
		
		for (Table table: schema.getTables().values()) {
			
			// skip if the table does not have the PK or unique
			KeyRecord primaryKey = table.getPrimaryKey();
			List<KeyRecord> uniques = table.getUniqueKeys();				
			if (primaryKey == null && uniques.isEmpty()) {
				continue;
			}
			
			// build Associations
			for (ForeignKey fk:table.getForeignKeys()) {
				
				EdmEntitySet.Builder entitySet = findEntitySet(edmSchemas,schema.getName(), table.getName());
				EdmEntitySet.Builder refEntitySet = findEntitySet(edmSchemas, schema.getName(),fk.getReferenceTableName());
				EdmEntityType.Builder entityType = findEntityType(edmSchemas, schema.getName(), getEntityTypeName(table));				
				EdmEntityType.Builder refEntityType = findEntityType(edmSchemas, schema.getName(), getEntityTypeName(fk.getPrimaryKey().getParent()));
				
				// check to see if fk is part of this table's pk, then it is 1 to 1 relation
				boolean onetoone = sameColumnSet(table.getPrimaryKey(), fk);
				
				// Build Association Ends				
				EdmAssociationEnd.Builder endSelf = EdmAssociationEnd.newBuilder()
						.setRole(table.getName())
						.setType(entityType)
						.setMultiplicity(onetoone?EdmMultiplicity.ZERO_TO_ONE:EdmMultiplicity.MANY);
				
				EdmAssociationEnd.Builder endRef = EdmAssociationEnd.newBuilder()
						.setRole(fk.getReferenceTableName())
						.setType(refEntityType)
						.setMultiplicity(EdmMultiplicity.ZERO_TO_ONE);
				

				// Build Association
				EdmAssociation.Builder association = EdmAssociation.newBuilder();
				association.setName(table.getName()+"_"+fk.getName()); //$NON-NLS-1$
				association.setEnds(endSelf, endRef);
				association.setNamespace(refEntityType.getFullyQualifiedTypeName().substring(0, refEntityType.getFullyQualifiedTypeName().indexOf('.')));
				assosiations.add(association);
				
				// Build ReferentialConstraint
				if (fk.getReferenceColumns() != null) {
					EdmReferentialConstraint.Builder erc = EdmReferentialConstraint.newBuilder();
					erc.setPrincipalRole(fk.getReferenceTableName());
					erc.addPrincipalReferences(fk.getReferenceColumns());
					erc.setDependentRole(table.getName());
					erc.addDependentReferences(getColumnNames(fk.getColumns()));
					association.setRefConstraint(erc);
				}					
				
				if (!fk.getReferenceTableName().equalsIgnoreCase(table.getName())) {
				    // Add EdmNavigationProperty to entity type
    				EdmNavigationProperty.Builder nav = EdmNavigationProperty.newBuilder(fk.getReferenceTableName());
    				nav.setRelationshipName(fk.getName());
    				nav.setFromToName(table.getName(), fk.getReferenceTableName());
    				nav.setRelationship(association);
    				nav.setFromTo(endSelf, endRef);
    				entityType.addNavigationProperties(nav);
				}
				
				// Add EdmNavigationProperty to Reference entity type
				EdmNavigationProperty.Builder refNav = EdmNavigationProperty.newBuilder(table.getName());
				refNav.setRelationshipName(fk.getName());
				refNav.setFromToName(fk.getReferenceTableName(), table.getName());
				refNav.setRelationship(association);
				refNav.setFromTo(endRef, endSelf);
				refEntityType.addNavigationProperties(refNav);					
				
				// build AssosiationSet
				EdmAssociationSet.Builder assosiationSet = EdmAssociationSet.newBuilder()
						.setName(table.getName()+"_"+fk.getName()) //$NON-NLS-1$
						.setAssociationName(fk.getName());	
				
				// Build AssosiationSet Ends
				EdmAssociationSetEnd.Builder endOne = EdmAssociationSetEnd.newBuilder()
						.setEntitySet(entitySet)
						.setRoleName(table.getName())
						.setRole(EdmAssociationEnd.newBuilder().setType(entityType).setRole(entityType.getName()));
				
				EdmAssociationSetEnd.Builder endTwo = EdmAssociationSetEnd.newBuilder()
						.setEntitySet(refEntitySet)
						.setRoleName(fk.getReferenceTableName())
						.setRole(EdmAssociationEnd.newBuilder().setType(refEntityType).setRole(refEntityType.getName()));					
				assosiationSet.setEnds(endOne, endTwo);
				
				assosiationSet.setAssociation(association);
				assosiationSets.add(assosiationSet);
			}
		}
		entityContainer.addAssociationSets(assosiationSets);
		odataSchema.addAssociations(assosiations);
	}	
	
	public static void buildFunctionImports(Schema schema, List<Builder> edmSchemas) {
		EdmSchema.Builder odataSchema = findSchema(edmSchemas, schema.getName());
		EdmEntityContainer.Builder entityContainer = findEntityContainer(edmSchemas, schema.getName());
		
		// procedures
		for(Procedure proc:schema.getProcedures().values()) {
			EdmFunctionImport.Builder edmProcedure = EdmFunctionImport.newBuilder();
			edmProcedure.setName(proc.getName());
			String httpMethod = "POST"; //$NON-NLS-1$
			
			for (ProcedureParameter pp:proc.getParameters()) {
				if (pp.getName().equals("return")) { //$NON-NLS-1$
					httpMethod = "GET"; //$NON-NLS-1$
					edmProcedure.setReturnType(ODataTypeManager.odataType(pp.getRuntimeType()));						
					continue;
				}
				
				EdmFunctionParameter.Builder param = EdmFunctionParameter.newBuilder();
				param.setName(pp.getName());
				param.setType(ODataTypeManager.odataType(pp.getRuntimeType()));

				if (pp.getType() == ProcedureParameter.Type.In) {
					param.setMode(Mode.In);
				}
				else if (pp.getType() == ProcedureParameter.Type.InOut) {
					param.setMode(Mode.InOut);
				}
				else if (pp.getType() == ProcedureParameter.Type.Out) {
					param.setMode(Mode.Out);
				}
				
				param.setNullable(pp.getNullType() == NullType.Nullable);
				edmProcedure.addParameters(param);
			}
			
			// add a complex type for return resultset.
			ColumnSet<Procedure> returnColumns = proc.getResultSet();
			if (returnColumns != null) {
				httpMethod = "GET"; //$NON-NLS-1$
				EdmComplexType.Builder complexType = EdmComplexType.newBuilder();
				String entityTypeName = proc.getName()+"_"+returnColumns.getName(); //$NON-NLS-1$
				if (proc.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false) != null) {
					entityTypeName = proc.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
				}				
				complexType.setName(entityTypeName); 
				complexType.setNamespace(schema.getName());
				for (Column c:returnColumns.getColumns()) {
					EdmProperty.Builder property = EdmProperty.newBuilder(c.getName())
							.setType(ODataTypeManager.odataType(c.getRuntimeType()))
							.setNullable(c.getNullType() == NullType.Nullable);
					if (c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.STRING)) {
						property.setFixedLength(c.isFixedLength())
							.setMaxLength(c.getLength())
							.setUnicode(true);
					}
					complexType.addProperties(property);
				}
				odataSchema.addComplexTypes(complexType);
				edmProcedure.setIsCollection(true);
				edmProcedure.setReturnType(EdmCollectionType.newBuilder().setCollectionType(complexType).setKind(CollectionKind.Collection));
			}
			edmProcedure.setHttpMethod(httpMethod);
			entityContainer.addFunctionImports(edmProcedure);
		}
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
