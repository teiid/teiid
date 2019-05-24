/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.odata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

import org.core4j.Enumerable;
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

    static EdmDataServices buildMetadata(MetadataStore metadataStore) {
        try {
            List<EdmSchema.Builder> edmSchemas = new ArrayList<EdmSchema.Builder>();
            for (Schema schema:metadataStore.getSchemaList()) {
                buildEntityTypes(schema, edmSchemas, true);
                buildFunctionImports(schema, edmSchemas, true);
                buildAssosiationSets(schema, edmSchemas, true);
            }
            return EdmDataServices.newBuilder().addSchemas(edmSchemas).build();
        } catch (Exception e) {
            throw new TeiidRuntimeException(e);
        }
    }

    static EdmDataServices buildMetadata(Schema schema) {
        try {
            List<EdmSchema.Builder> edmSchemas = new ArrayList<EdmSchema.Builder>();
            buildEntityTypes(schema, edmSchemas, true);
            buildFunctionImports(schema, edmSchemas, true);
            buildAssosiationSets(schema, edmSchemas, true);
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

    public static void buildEntityTypes(Schema schema, List<EdmSchema.Builder> edmSchemas, boolean preserveEntityTypeName) {
        List<EdmEntitySet.Builder> entitySets = new ArrayList<EdmEntitySet.Builder>();
        List<EdmEntityType.Builder> entityTypes = new ArrayList<EdmEntityType.Builder>();
        LinkedHashMap<String, EdmComplexType.Builder> complexTypes = new LinkedHashMap<String, EdmComplexType.Builder>();

        if (preserveEntityTypeName) {
            //first pass, build complex types
            for (Table table: schema.getTables().values()) {
                // skip if the table does not have the PK or unique
                KeyRecord primaryKey = table.getPrimaryKey();
                List<KeyRecord> uniques = table.getUniqueKeys();
                if (primaryKey == null && uniques.isEmpty()) {
                    LogManager.logDetail(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17017, table.getFullName()));
                    continue;
                }

                for (Column c : table.getColumns()) {
                    String columnGroup = c.getProperty(ODataMetadataProcessor.COLUMN_GROUP, false);
                    String name = c.getName();
                    if (columnGroup != null) {
                        //use the real name not the group_column name
                        name = c.getSourceName();
                    }
                    String complexType = c.getProperty(ODataMetadataProcessor.COMPLEX_TYPE, false);
                    if (complexType == null) {
                        continue;
                    }
                    EdmComplexType.Builder complexTypeBuilder = complexTypes.get(complexType);
                    if (complexTypeBuilder == null) {
                        complexTypeBuilder = EdmComplexType.newBuilder();
                        complexTypes.put(complexType, complexTypeBuilder);
                        complexTypeBuilder.setName(complexType);
                        complexTypeBuilder.setNamespace(schema.getName());
                    } else if (complexTypeBuilder.findProperty(name) != null) {
                        continue; //already added
                    }
                    EdmProperty.Builder property = EdmProperty.newBuilder(name)
                            .setType(ODataTypeManager.odataType(c.getRuntimeType()))
                            .setNullable(isPartOfPrimaryKey(table, c.getName())?false:c.getNullType() == NullType.Nullable);
                    if (c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.STRING)) {
                        property.setFixedLength(c.isFixedLength())
                            .setMaxLength(c.getLength())
                            .setUnicode(true);
                    }
                    complexTypeBuilder.addProperties(property);
                }
            }
        }

        //second pass, add all columns
        for (Table table: schema.getTables().values()) {

            // skip if the table does not have the PK or unique
            KeyRecord primaryKey = table.getPrimaryKey();
            List<KeyRecord> uniques = table.getUniqueKeys();
            if (primaryKey == null && uniques.isEmpty()) {
                LogManager.logDetail(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17017, table.getFullName()));
                continue;
            }

            String entityTypeName = table.getName();
            if (preserveEntityTypeName && table.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false) != null) {
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

            HashSet<String> columnGroups = new HashSet<String>();
            // adding properties
            for (Column c : table.getColumns()) {
                String complexType = c.getProperty(ODataMetadataProcessor.COMPLEX_TYPE, false);
                if (complexType != null && preserveEntityTypeName) {
                    String columnGroup = c.getProperty(ODataMetadataProcessor.COLUMN_GROUP, false);
                    if (!columnGroups.add(columnGroup)) {
                        continue;
                    }
                    EdmProperty.Builder property = EdmProperty.newBuilder(columnGroup)
                            .setType(complexTypes.get(complexType).build());
                    entityType.addProperties(property);
                    continue;
                }
                EdmProperty.Builder property = EdmProperty.newBuilder(c.getName())
                        .setType(ODataTypeManager.odataType(c.getRuntimeType()))
                        .setNullable(isPartOfPrimaryKey(table, c.getName())?false:c.getNullType() == NullType.Nullable);
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
                .addEntityContainers(entityContainer)
                .addComplexTypes(complexTypes.values());

        edmSchemas.add(modelSchema);
    }

    static boolean isPartOfPrimaryKey(Table table, String columnName) {
        KeyRecord pk = table.getPrimaryKey();
        if (hasColumn(pk, columnName)) {
            return true;
        }
        for (KeyRecord key:table.getUniqueKeys()) {
            if (hasColumn(key, columnName)) {
                return true;
            }
        }
        return false;
    }

    static boolean hasColumn(KeyRecord pk, String columnName) {
        if (pk != null) {
            return pk.getColumnByName(columnName) != null;
        }
        return false;
    }

    private static String getEntityTypeName(Table t, boolean preserveEntityTypeName) {
        String entityTypeName = t.getName();
        if (preserveEntityTypeName && t.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false) != null) {
            entityTypeName = t.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
        }
        return entityTypeName;
    }

    public static void buildAssosiationSets(Schema schema, List<Builder> edmSchemas, boolean preserveEntityTypeName) {
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
                EdmEntityType.Builder entityType = findEntityType(edmSchemas, schema.getName(), getEntityTypeName(table, preserveEntityTypeName));
                EdmEntityType.Builder refEntityType = findEntityType(edmSchemas, schema.getName(), getEntityTypeName(fk.getPrimaryKey().getParent(), preserveEntityTypeName));

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
                    String navigationName = getNavigationName(entityType, fk.getReferenceTableName());
                    EdmNavigationProperty.Builder nav = EdmNavigationProperty.newBuilder(navigationName);
                    nav.setRelationshipName(fk.getName());
                    nav.setFromToName(table.getName(), fk.getReferenceTableName());
                    nav.setRelationship(association);
                    nav.setFromTo(endSelf, endRef);
                    entityType.addNavigationProperties(nav);
                }

                // Add EdmNavigationProperty to Reference entity type
                String navigationName = getNavigationName(refEntityType, table.getName());
                EdmNavigationProperty.Builder refNav = EdmNavigationProperty.newBuilder(navigationName);
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

    private static String getNavigationName(
            org.odata4j.edm.EdmEntityType.Builder entityType,
            String tableName) {

        String navigationName = tableName;
        int i = 1;
        while(true) {
            for (EdmNavigationProperty.Builder b:entityType.getNavigationProperties()) {
                if (b.getName().equals(navigationName)) {
                    navigationName = null;
                    break;
                }
            }
            if (navigationName != null) {
                return navigationName;
            }
            navigationName = tableName+(i++);
        }
    }

    public static void buildFunctionImports(Schema schema, List<Builder> edmSchemas, boolean preserveEntityTypeName) {
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
                if (preserveEntityTypeName && proc.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false) != null) {
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

    static EdmEntitySet removeModelName(EdmEntitySet src) {
        EdmEntityType srcType = src.getType();
        String schemaName = srcType.getName().substring(0, srcType.getName().indexOf('.'));
        String name = srcType.getName().substring(srcType.getName().indexOf('.')+1);

        EdmEntityType.Builder targetType = EdmEntityType
                .newBuilder().setName(name)
                .setNamespace(schemaName);

        targetType.addKeys(srcType.getKeys());

        Enumerable<EdmProperty> properties = srcType.getProperties();
        for (EdmProperty srcProperty:properties.toList()) {

            EdmProperty.Builder tgtProperty = EdmProperty.newBuilder(srcProperty.getName())
                    .setType(srcProperty.getType())
                    .setNullable(srcProperty.isNullable())
                    .setFixedLength(srcProperty.getFixedLength())
                    .setMaxLength(srcProperty.getMaxLength())
                    .setUnicode(true);
            targetType.addProperties(tgtProperty);
        }

        EdmEntitySet.Builder target = EdmEntitySet.newBuilder()
                .setName(src.getName())
                .setEntityType(targetType);
        return target.build();
    }
}
