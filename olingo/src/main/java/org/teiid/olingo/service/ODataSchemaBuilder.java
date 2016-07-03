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
package org.teiid.olingo.service;

import static org.teiid.language.visitor.SQLStringVisitor.getRecordName;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.*;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlCollection;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression.ConstantExpressionType;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Table;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.common.ODataTypeManager;

public class ODataSchemaBuilder {

    public static CsdlSchema buildMetadata(String namespace, org.teiid.metadata.Schema teiidSchema) {
        try {
            CsdlSchema csdlSchema = new CsdlSchema();
            String fullSchemaName = namespace+"."+teiidSchema.getName();
            csdlSchema.setNamespace(fullSchemaName).setAlias(teiidSchema.getName());
            buildEntityTypes(namespace, teiidSchema, csdlSchema);
            buildProcedures(teiidSchema, csdlSchema);
            return csdlSchema;
        } catch (Exception e) {
            throw new TeiidRuntimeException(e);
        }
    }

    static CsdlEntitySet findEntitySet(CsdlSchema edmSchema, String enitityName) {
        CsdlEntityContainer entityContainter = edmSchema.getEntityContainer();
        for (CsdlEntitySet entitySet : entityContainter.getEntitySets()) {
            if (entitySet.getName().equalsIgnoreCase(enitityName)) {
                return entitySet;
            }
        }
        return null;
    }

    static CsdlSchema findSchema(Map<String, CsdlSchema> edmSchemas, String schemaName) {
        return edmSchemas.get(schemaName);
    }

    static CsdlEntityType findEntityType(Map<String, CsdlSchema> edmSchemas,
            String schemaName, String enitityName) {
        CsdlSchema schema = findSchema(edmSchemas, schemaName);
        if (schema != null) {
            for (CsdlEntityType type : schema.getEntityTypes()) {
                if (type.getName().equalsIgnoreCase(enitityName)) {
                    return type;
                }
            }
        }
        return null;
    }

    static CsdlEntityContainer findEntityContainer(Map<String, CsdlSchema> edmSchemas, String schemaName) {
        CsdlSchema schema = edmSchemas.get(schemaName);
        return schema.getEntityContainer();
    }

    static void buildEntityTypes(String namespace, org.teiid.metadata.Schema schema, CsdlSchema csdlSchema) {
        Map<String, CsdlEntitySet> entitySets = new LinkedHashMap<String, CsdlEntitySet>();
        Map<String, CsdlEntityType> entityTypes = new LinkedHashMap<String, CsdlEntityType>();
        
        String fullSchemaName = namespace+"."+schema.getName();

        for (Table table : schema.getTables().values()) {

            // skip if the table does not have the PK or unique
            KeyRecord primaryKey = getIdentifier(table);
            if ( primaryKey == null) {
                LogManager.logDetail(LogConstants.CTX_ODATA,
                        ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16017,table.getFullName()));
                continue;
            }

            String entityTypeName = table.getName();
            CsdlEntityType entityType = new CsdlEntityType().setName(entityTypeName);

            // adding properties
            List<CsdlProperty> properties = new ArrayList<CsdlProperty>();
            for (Column c : table.getColumns()) {
                boolean nullable = c.getNullType() == NullType.Nullable;
                CsdlProperty property = buildProperty(c, isPartOfPrimaryKey(table, c.getName())?false:nullable);
                addColumnAnnotations(c, property, csdlSchema);
                properties.add(property);
            }
            entityType.setProperties(properties);
            if (hasStream(properties)) {
                entityType.setHasStream(true);
            }

            // set keys
            ArrayList<CsdlPropertyRef> keyProps = new ArrayList<CsdlPropertyRef>();
            for (Column c : primaryKey.getColumns()) {
                keyProps.add(new CsdlPropertyRef().setName(c.getName()));
            }

            entityType.setKey(keyProps);
            addTableAnnotations(table, entityType, csdlSchema);

            // entity set one for one entity type
            CsdlEntitySet entitySet = new CsdlEntitySet()
                    .setName(table.getName())
                    .setType(new FullQualifiedName(fullSchemaName, table.getName()))
                    .setIncludeInServiceDocument(true);
           
            // add entity types for entity schema
            entityTypes.put(entityTypeName, entityType);
            entitySets.put(entityTypeName, entitySet);
        }
        

        buildNavigationProperties(schema, entityTypes, entitySets);


        // entity container is holder entity sets, association sets, function
        // imports
        CsdlEntityContainer entityContainer = new CsdlEntityContainer().setName(
                schema.getName()).setEntitySets(
                new ArrayList<CsdlEntitySet>(entitySets.values()));

        // build entity schema
        csdlSchema.setEntityTypes(new ArrayList<CsdlEntityType>(entityTypes.values()))
                .setEntityContainer(entityContainer);
    }

    private static boolean hasStream(List<CsdlProperty> properties) {
        for (CsdlProperty p : properties) {
            if (p.getType().equals(EdmPrimitiveTypeKind.Binary.getFullQualifiedName())) {
                return true;
            }
        }
        return false;
    }

    private static CsdlProperty buildProperty(Column c, boolean nullable) {
        CsdlProperty property = new CsdlProperty()
                .setName(c.getName())
                .setType(ODataTypeManager.odataType(c.getRuntimeType()).getFullQualifiedName())
                .setNullable(nullable);

        if (DataTypeManager.isArrayType(c.getRuntimeType())) {
            property.setCollection(true);
        }

        if (c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.STRING)) {
            property.setMaxLength(c.getLength()).setUnicode(true);
        } else if (c.getRuntimeType().equals(
                DataTypeManager.DefaultDataTypes.DOUBLE)
                || c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.FLOAT)
                || c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.BIG_DECIMAL)) {
            property.setPrecision(c.getPrecision());
            property.setScale(c.getScale());
        } else if (c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.TIMESTAMP)
                || c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.TIME)) {
            property.setPrecision(c.getPrecision() == 0?new Integer(4):c.getPrecision());
        }
        else {
            if (c.getDefaultValue() != null) {
                property.setDefaultValue(c.getDefaultValue());
            }
        }
        return property;
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
            for (Column column : pk.getColumns()) {
                if (getRecordName(column).equals(columnName)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private static KeyRecord getIdentifier(Table table) {
        if (table.getPrimaryKey() != null) {
            return table.getPrimaryKey();
        }
        
        for (KeyRecord key:table.getUniqueKeys()) {
            return key;
        }
        return null;
    }
    
    private static void buildNavigationProperties(org.teiid.metadata.Schema schema, 
            Map<String, CsdlEntityType> entityTypes, Map<String, CsdlEntitySet> entitySets) {
    
        for (Table table : schema.getTables().values()) {

            // skip if the table does not have the PK or unique
            if (getIdentifier(table) == null) {
                continue;
            }
                       
            // build Associations
            for (ForeignKey fk : table.getForeignKeys()) {
    
                // check to see if fk is part of this table's pk, then it is 1 to 1 relation
                boolean onetoone = sameColumnSet(getIdentifier(table), fk);

                addForwardNavigation(entityTypes, entitySets, table, fk, onetoone);
                addReverseNavigation(entityTypes, entitySets, table, fk, onetoone);
            }
        }
    }

    private static void addForwardNavigation(Map<String, CsdlEntityType> entityTypes,
            Map<String, CsdlEntitySet> entitySets, Table table, ForeignKey fk, boolean onetoone) {
        CsdlNavigationProperty navigaton = null;
        CsdlNavigationPropertyBinding navigationBinding = null;
        String entityTypeName = null;
        if (onetoone) {
            entityTypeName = table.getName();
            navigaton = buildNavigation(fk);                
            navigationBinding = buildNavigationBinding(fk);                    
            navigaton.setNullable(false);
        } else {
            entityTypeName = fk.getReferenceTableName();
            navigaton = buildReverseNavigation(table, fk);                
            navigationBinding = buildReverseNavigationBinding(table,fk);                                        
            navigaton.setCollection(true);
        }                
   
        
        CsdlEntityType entityType = entityTypes.get(entityTypeName);
        entityType.getNavigationProperties().add(navigaton);
        
        CsdlEntitySet entitySet = entitySets.get(entityTypeName);
        entitySet.getNavigationPropertyBindings().add(navigationBinding);
    }

    private static void addReverseNavigation(Map<String, CsdlEntityType> entityTypes,
            Map<String, CsdlEntitySet> entitySets, Table table, ForeignKey fk, boolean onetoone) {
        CsdlNavigationProperty navigaton = null;
        CsdlNavigationPropertyBinding navigationBinding = null;
        String entityTypeName = null;
        
        if (onetoone) {
            entityTypeName = fk.getReferenceTableName();
            navigaton = buildReverseNavigation(table, fk);                
            navigationBinding = buildReverseNavigationBinding(table,fk);                                        
            navigaton.setNullable(false);
        } else {
            entityTypeName = table.getName();
            navigaton = buildNavigation(fk);                
            navigationBinding = buildNavigationBinding(fk);
            navigaton.setCollection(true);
        }                
        
        CsdlEntityType entityType = entityTypes.get(entityTypeName);
        entityType.getNavigationProperties().add(navigaton);
        
        CsdlEntitySet entitySet = entitySets.get(entityTypeName);
        entitySet.getNavigationPropertyBindings().add(navigationBinding);
    }
    
    private static CsdlNavigationPropertyBinding buildNavigationBinding(ForeignKey fk) {
        CsdlNavigationPropertyBinding navigationBinding = new CsdlNavigationPropertyBinding();
        navigationBinding.setPath(fk.getName());
        navigationBinding.setTarget(fk.getReferenceTableName());
        return navigationBinding;
    }
    
    private static CsdlNavigationPropertyBinding buildReverseNavigationBinding(Table table, ForeignKey fk) {
        CsdlNavigationPropertyBinding navigationBinding = new CsdlNavigationPropertyBinding();
        navigationBinding.setPath(table.getName()+"_"+fk.getName());
        navigationBinding.setTarget(table.getName());
        return navigationBinding;
    }

    private static CsdlNavigationProperty buildNavigation(ForeignKey fk) {
        String refSchemaName = fk.getReferenceKey().getParent().getParent().getName();
        CsdlNavigationProperty navigaton = new CsdlNavigationProperty();
        navigaton.setName(fk.getName()).setType(new FullQualifiedName(refSchemaName, fk.getReferenceTableName()));
        
        ArrayList<CsdlReferentialConstraint> constrainsts = new ArrayList<CsdlReferentialConstraint>();
        for (int i = 0; i < fk.getColumns().size(); i++) {
            Column c = fk.getColumns().get(i);
            String refColumn = fk.getReferenceColumns().get(i);
            CsdlReferentialConstraint constraint = new CsdlReferentialConstraint();
            constraint.setProperty(c.getName());
            constraint.setReferencedProperty(refColumn);
        }
        navigaton.setReferentialConstraints(constrainsts);
        return navigaton;
    }
    
    private static CsdlNavigationProperty buildReverseNavigation(Table table, ForeignKey fk) {
        String refSchemaName = table.getParent().getName();
        
        CsdlNavigationProperty navigaton = new CsdlNavigationProperty();
        navigaton.setName(table.getName() + "_" + fk.getName()).setType(
                new FullQualifiedName(refSchemaName, table.getName()));
        
        ArrayList<CsdlReferentialConstraint> constrainsts = new ArrayList<CsdlReferentialConstraint>();
        for (int i = 0; i < fk.getColumns().size(); i++) {
            Column c = fk.getColumns().get(i);
            String refColumn = fk.getReferenceColumns().get(i);
            CsdlReferentialConstraint constraint = new CsdlReferentialConstraint();
            constraint.setProperty(refColumn);
            constraint.setReferencedProperty(c.getName());
        }
        navigaton.setReferentialConstraints(constrainsts);
        return navigaton;
    }    

    static void buildProcedures(org.teiid.metadata.Schema schema, CsdlSchema csdlSchema) {
        // procedures
        ArrayList<CsdlComplexType> complexTypes = new ArrayList<CsdlComplexType>();
        ArrayList<CsdlFunction> functions = new ArrayList<CsdlFunction>();
        ArrayList<CsdlFunctionImport> functionImports = new ArrayList<CsdlFunctionImport>();
        ArrayList<CsdlAction> actions = new ArrayList<CsdlAction>();
        ArrayList<CsdlActionImport> actionImports = new ArrayList<CsdlActionImport>();

        for (Procedure proc : schema.getProcedures().values()) {
            if (!allowedProcedure(proc)){
                LogManager.logDetail(LogConstants.CTX_ODATA, 
                        ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16032, proc.getFullName()));
                continue;                
            }
            
            if (isFuntion(proc)) {
                buildFunction(schema.getName(), proc, complexTypes, functions, functionImports, csdlSchema);
            }
            else {
                buildAction(schema.getName(), proc, complexTypes, actions, actionImports, csdlSchema);
            }
        }
        csdlSchema.setComplexTypes(complexTypes);
        csdlSchema.setFunctions(functions);
        csdlSchema.setActions(actions);
        csdlSchema.getEntityContainer().setFunctionImports(functionImports);
        csdlSchema.getEntityContainer().setActionImports(actionImports);
    }

    private static boolean doesProcedureReturn(Procedure proc) {        
        for (ProcedureParameter pp : proc.getParameters()) {
            if (pp.getType().equals(ProcedureParameter.Type.ReturnValue)) {
                return true;
            }
        }
        return (proc.getResultSet() != null);
    }

    private static boolean allowedProcedure(Procedure proc) {
        // any number of in, but can have only one LOB if lob is present
        // only *one* result, or resultset allowed
        int inouts = 0;
        int lobs = 0;
        int outs = 0;
        for (ProcedureParameter pp : proc.getParameters()) {
            if (pp.getType().equals(ProcedureParameter.Type.Out)) { 
                continue;
            }
        	
            if (pp.getType().equals(ProcedureParameter.Type.In) 
            		|| pp.getType().equals(ProcedureParameter.Type.InOut)) {
                inouts++;
                if (DataTypeManager.isLOB(pp.getRuntimeType())) {
                    lobs++;
                }                
            } else if (pp.getType().equals(ProcedureParameter.Type.ReturnValue)) { 
                outs++;
            }                        
        }
        
        if (proc.getResultSet() != null) {
        	for (Column c : proc.getResultSet().getColumns()) {
        		if (DataTypeManager.isLOB(c.getRuntimeType())) {
        			return false;
        		}
        	}
            outs++;
        }
        
        if (outs > 1) {
            return false;
        }
        
        if (inouts > 1 && lobs >= 1) {
            return false;
        }
        
        return true;
    }
    
    private static boolean isInputParameterLob(Procedure proc) {        
        for (ProcedureParameter pp : proc.getParameters()) {
            if (!pp.getType().equals(ProcedureParameter.Type.ReturnValue)
                    && DataTypeManager.isLOB(pp.getRuntimeType())) {
                return true;
            }
        }
        return false;
    }
    
    private static boolean isFuntion(Procedure proc) {
        if (doesProcedureReturn(proc) && proc.getUpdateCount() == 0
                && !isInputParameterLob(proc)) {
            return true;
        }
        return false;
    }    

    static void buildFunction(String schemaName, Procedure proc,
            ArrayList<CsdlComplexType> complexTypes, ArrayList<CsdlFunction> functions,
            ArrayList<CsdlFunctionImport> functionImports, CsdlSchema csdlSchema) {

        CsdlFunction edmFunction = new CsdlFunction();
        edmFunction.setName(proc.getName());
        edmFunction.setBound(false);

        ArrayList<CsdlParameter> params = new ArrayList<CsdlParameter>();
        for (ProcedureParameter pp : proc.getParameters()) {
            EdmPrimitiveTypeKind odataType = ODataTypeManager.odataType(pp.getRuntimeType());            
            if (pp.getType().equals(ProcedureParameter.Type.ReturnValue)) {
                edmFunction.setReturnType(new CsdlReturnType().setType(odataType.getFullQualifiedName()).setCollection(DataTypeManager.isArrayType(pp.getRuntimeType())));
                continue;
            } 
            
            if (pp.getType().equals(ProcedureParameter.Type.In)
                    || pp.getType().equals(ProcedureParameter.Type.InOut)) {
                CsdlParameter parameter = buildParameter(pp, odataType);
                addOperationParameterAnnotations(pp, parameter, csdlSchema);
                params.add(parameter);
            }           
        }
        edmFunction.setParameters(params);

        // add a complex type for return resultset.
        ColumnSet<Procedure> returnColumns = proc.getResultSet();
        if (returnColumns != null) {
            CsdlComplexType complexType = buildComplexType(proc, returnColumns, csdlSchema);
            complexTypes.add(complexType);
            FullQualifiedName odataType = new FullQualifiedName(schemaName, complexType.getName());
            edmFunction.setReturnType((new CsdlReturnType().setType(odataType).setCollection(true)));
        }

        CsdlFunctionImport functionImport = new CsdlFunctionImport();
        functionImport.setName(proc.getName()).setFunction(new FullQualifiedName(schemaName, proc.getName()));
        addOperationAnnotations(proc, edmFunction, csdlSchema);
        functions.add(edmFunction);
        functionImports.add(functionImport);
    }

    private static CsdlParameter buildParameter(ProcedureParameter pp,
            EdmPrimitiveTypeKind odatatype) {
        CsdlParameter param = new CsdlParameter();
        param.setName(pp.getName());
        param.setType(odatatype.getFullQualifiedName());

        if (DataTypeManager.isArrayType(pp.getRuntimeType())) {
            param.setCollection(true);
        }
        param.setNullable(pp.getNullType() == NullType.Nullable);
        
        if (pp.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.STRING)) {
            param.setMaxLength(pp.getLength());
        } else if (pp.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.DOUBLE)
                || pp.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.FLOAT)
                || pp.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.BIG_DECIMAL)) {
            param.setPrecision(pp.getPrecision());
            param.setScale(pp.getScale());
        } else {
            if (pp.getDefaultValue() != null) {
                //param.setDefaultValue(pp.getDefaultValue());
            }
        }
        return param;
    }

    static void buildAction(String schemaName, Procedure proc,
            ArrayList<CsdlComplexType> complexTypes,
            ArrayList<CsdlAction> actions,
            ArrayList<CsdlActionImport> actionImports, CsdlSchema csdlSchema) {
        CsdlAction edmAction = new CsdlAction();
        edmAction.setName(proc.getName());
        edmAction.setBound(false);

        ArrayList<CsdlParameter> params = new ArrayList<CsdlParameter>();        
        for (ProcedureParameter pp : proc.getParameters()) {
            EdmPrimitiveTypeKind odatatype = ODataTypeManager.odataType(pp.getRuntimeType());
            if (pp.getType().equals(ProcedureParameter.Type.ReturnValue)) {                 
                edmAction.setReturnType(new CsdlReturnType().setType(odatatype.getFullQualifiedName()).setCollection(DataTypeManager.isArrayType(pp.getRuntimeType())));
                continue;
            }

            if (pp.getType().equals(ProcedureParameter.Type.In)
                    || pp.getType().equals(ProcedureParameter.Type.InOut)) {
                CsdlParameter parameter = buildParameter(pp, odatatype);
                addOperationParameterAnnotations(pp, parameter, csdlSchema);
                params.add(parameter);
            }
        }
        edmAction.setParameters(params);

        // add a complex type for return resultset.
        ColumnSet<Procedure> returnColumns = proc.getResultSet();
        if (returnColumns != null) {
            CsdlComplexType complexType = buildComplexType(proc, returnColumns, csdlSchema);
            complexTypes.add(complexType);
            edmAction.setReturnType((new CsdlReturnType()
                    .setType(new FullQualifiedName(schemaName, complexType
                    .getName())).setCollection(true)));
        }

        CsdlActionImport actionImport = new CsdlActionImport();
        actionImport.setName(proc.getName()).setAction(new FullQualifiedName(schemaName, proc.getName()));
        addOperationAnnotations(proc, edmAction, csdlSchema);
        actions.add(edmAction);
        actionImports.add(actionImport);
    }

    private static CsdlComplexType buildComplexType(Procedure proc,
            ColumnSet<Procedure> returnColumns, CsdlSchema csdlSchema) {
        CsdlComplexType complexType = new CsdlComplexType();
        String entityTypeName = proc.getName() + "_" + returnColumns.getName(); //$NON-NLS-1$
        complexType.setName(entityTypeName);

        ArrayList<CsdlProperty> props = new ArrayList<CsdlProperty>();
        for (Column c : returnColumns.getColumns()) {
            CsdlProperty property = buildProperty(c, (c.getNullType() == NullType.Nullable));
            props.add(property);
            addColumnAnnotations(c, property, csdlSchema);
        }
        complexType.setProperties(props);
        return complexType;
    }

    static List<String> getColumnNames(List<Column> columns) {
        ArrayList<String> names = new ArrayList<String>();
        for (Column c : columns) {
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
    
    private static void addTableAnnotations(Table table, CsdlEntityType entityType, CsdlSchema csdlSchema) {
        if (table.getAnnotation() != null) {
            addStringAnnotation(entityType, "core.Description", table.getAnnotation());
        }
        
        if (table.getCardinality() != -1) {
            addIntAnnotation(entityType, "teiid.CARDINALITY", table.getCardinality());
        }
        
        if (table.isMaterialized()) {
        
            if (table.getMaterializedTable() != null) {
                addStringAnnotation(entityType, "teiid.MATERIALIZED_TABLE", table.getMaterializedTable().getFullName());
            }
            
            if (table.getMaterializedStageTable() != null) {
                addStringAnnotation(entityType, "teiid.MATERIALIZED_STAGE_TABLE", table.getMaterializedStageTable().getFullName());
            }
        }
        
        if (table.getNameInSource() != null) {
            addStringAnnotation(entityType, "teiid.NAMEINSOURCE", table.getNameInSource());
        }
        
        if (table.getAccessPatterns() != null && !table.getAccessPatterns().isEmpty()) {
            ArrayList<String> values = new ArrayList<String>();
            for (KeyRecord record:table.getAccessPatterns()) {
                StringBuilder sb = new StringBuilder();
                for (Column c:record.getColumns()) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(c.getName());
                }
                values.add(sb.toString());
            }
            addStringCollectionAnnotation(entityType, "teiid.ACCESS_PATTERNS", values);
        }
        
        if (table.supportsUpdate()) {
            addBooleanAnnotation(entityType, "teiid.UPDATABLE", table.supportsUpdate());
        }
        
        // add all custom properties
        for (String property:table.getProperties().keySet()) {
            addTerm(normalizeTermName(property), "EntityType", csdlSchema);
            addStringAnnotation(entityType, csdlSchema.getAlias()+"."+normalizeTermName(property), table.getProperties().get(property));
        }
    }

    private static void addColumnAnnotations(Column column,
            CsdlProperty property, CsdlSchema csdlSchema) {
        if (column.getAnnotation() != null) {
            addStringAnnotation(property, "core.Description", column.getAnnotation());
        }
        
        if (column.getNameInSource() != null) {
            addStringAnnotation(property, "teiid.NAMEINSOURCE", column.getNameInSource());
        }
        
        if (!column.isSelectable()) {
            addBooleanAnnotation(property, "teiid.SELECTABLE", column.isSelectable());
        }
        
        if(column.isUpdatable()) {
            addBooleanAnnotation(property, "teiid.UPDATABLE", column.isUpdatable());
        }
        
        if (column.isCurrency()) {
            addBooleanAnnotation(property, "teiid.CURRENCY", column.isCurrency());
        }
        
        if (column.isCaseSensitive()) {
            addBooleanAnnotation(property, "teiid.CASE_SENSITIVE", column.isCaseSensitive());
        }
        
        if (column.isFixedLength()) {
            addBooleanAnnotation(property, "teiid.FIXED_LENGTH", column.isFixedLength());
        }
        
        if (!column.isSigned()) {
            addBooleanAnnotation(property, "teiid.SIGNED", column.isSigned());
        }
        
        if (column.getDistinctValues() != -1) {
            addIntAnnotation(property, "teiid.DISTINCT_VALUES", column.getDistinctValues());
        }
        
        if (column.getNullValues() != -1) {
            addIntAnnotation(property, "teiid.NULL_VALUE_COUNT", column.getNullValues());
        }
        
        // add all custom properties
        for (String str:column.getProperties().keySet()) {
            addTerm(normalizeTermName(str), "Property", csdlSchema);
            addStringAnnotation(property, csdlSchema.getAlias()+"."+normalizeTermName(str), column.getProperties().get(str));
        }        
    }
    
    private static void addOperationAnnotations(Procedure proc,
            CsdlOperation operation, CsdlSchema csdlSchema) {
        if (proc.getAnnotation() != null) {
            addStringAnnotation(operation, "core.Description", proc.getAnnotation());
        }
        
        if (proc.getNameInSource() != null) {
            addStringAnnotation(operation, "teiid.NAMEINSOURCE", proc.getNameInSource());
        }
        if (proc.getUpdateCount() != 1) {
            addIntAnnotation(operation, "teiid.UPDATECOUNT", proc.getUpdateCount());
        }   
        // add all custom properties
        for (String str:proc.getProperties().keySet()) {
            addTerm(normalizeTermName(str), "Action Function", csdlSchema);
            addStringAnnotation(operation, csdlSchema.getAlias()+"."+normalizeTermName(str), proc.getProperties().get(str));
        }         
    }    
    

    private static void addOperationParameterAnnotations(
            ProcedureParameter procedure, CsdlParameter parameter,
            CsdlSchema csdlSchema) {
        if (procedure.getAnnotation() != null) {
            addStringAnnotation(parameter, "core.Description", procedure.getAnnotation());
        }
        
        if (procedure.getNameInSource() != null) {
            addStringAnnotation(parameter, "teiid.NAMEINSOURCE", procedure.getNameInSource());
        }
        
        // add all custom properties
        for (String str:procedure.getProperties().keySet()) {
            addTerm(normalizeTermName(str), "Parameter", csdlSchema);
            addStringAnnotation(parameter, csdlSchema.getAlias()+"."+normalizeTermName(str), procedure.getProperties().get(str));
        } 
    }    
    private static void addStringAnnotation(CsdlAnnotatable recipent, String term, String value) {
        CsdlAnnotation annotation = new CsdlAnnotation();
        annotation.setTerm(term);
        annotation.setExpression(new CsdlConstantExpression(ConstantExpressionType.String, value));
        recipent.getAnnotations().add(annotation);  
    }
    
    private static void addIntAnnotation(CsdlAnnotatable recipent, String term, int value) {
        CsdlAnnotation annotation = new CsdlAnnotation();
        annotation.setTerm(term);
        annotation.setExpression(new CsdlConstantExpression(ConstantExpressionType.Int, String.valueOf(value)));
        recipent.getAnnotations().add(annotation);  
    }  
    
    private static void addBooleanAnnotation(CsdlAnnotatable recipent, String term, boolean value) {
        CsdlAnnotation annotation = new CsdlAnnotation();
        annotation.setTerm(term);
        annotation.setExpression(new CsdlConstantExpression(ConstantExpressionType.Bool, String.valueOf(value)));
        recipent.getAnnotations().add(annotation);  
    }
    
    private static void addStringCollectionAnnotation(CsdlAnnotatable recipent, String term, List<String> values) {
        CsdlAnnotation annotation = new CsdlAnnotation();
        annotation.setTerm(term);
        CsdlCollection collection = new CsdlCollection();
        for (String value:values) {
            collection.getItems().add(new CsdlConstantExpression(ConstantExpressionType.String, value));
        }
        annotation.setExpression(collection);
        recipent.getAnnotations().add(annotation);  
    }  
    
    private static void addTerm(String property, String appliesTo, CsdlSchema schema) {
        CsdlTerm term = schema.getTerm(property);
        if ( term == null) {
            term = new CsdlTerm();
            term.setName(property);
            term.setType("Edm.String");
            schema.getTerms().add(term);            
        }
        if (term.getAppliesTo().isEmpty() || !term.getAppliesTo().contains(appliesTo)) {
            term.getAppliesTo().add(appliesTo);
        }
    }     
    
    private static String normalizeTermName(String name) {
        if (name.startsWith("{")) {
            int end = name.indexOf("}");
            if (end != -1) {
                String modified = null;
                String namespace = name.substring(1, end);
                for (Map.Entry<String, String> entry:MetadataFactory.BUILTIN_NAMESPACES.entrySet()) {
                    if (entry.getValue().equals(namespace)) {
                        modified = entry.getKey();
                        break;
                    }
                }
                return modified + ":" + name.substring(end+1);
            }
        }
        return name;
    }
}
