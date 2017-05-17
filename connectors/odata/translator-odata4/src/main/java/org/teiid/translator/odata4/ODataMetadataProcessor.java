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
package org.teiid.translator.odata4;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.olingo.client.api.edm.xml.XMLMetadata;
import org.apache.olingo.commons.api.edm.provider.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column.SearchType;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.WSConnection;

public class ODataMetadataProcessor implements MetadataProcessor<WSConnection> {
    private static final String EDM_GEOMETRY = "Edm.Geometry"; //$NON-NLS-1$

	public enum ODataType {
        COMPLEX, 
        NAVIGATION, 
        ENTITY, 
        ENTITY_COLLECTION, 
        ACTION, 
        FUNCTION, 
        COMPLEX_COLLECTION, 
        NAVIGATION_COLLECTION
    };
    
    // local planning properties
    private static final String PARENT_TABLE = "PARENT_TABLE"; //$NON-NLS-1$
    private static final String CONSTRAINT_PROPERTY = "CONSTRAINT_PROPERTY"; //$NON-NLS-1$
    private static final String CONSTRAINT_REF_PROPERTY = "CONSTRAINT_REF_PROPERTY"; //$NON-NLS-1$
    private static final String FK_NAME = "FK_NAME"; //$NON-NLS-1$
    private static final String NAME_SEPARATOR = "_"; //$NON-NLS-1$
    
    @ExtensionMetadataProperty(applicable = { Table.class, Procedure.class }, 
            datatype = String.class, 
            display = "Name in OData Schema", 
            description = "Name in OData Schema", 
            required = true)    
    public static final String NAME_IN_SCHEMA = MetadataFactory.ODATA_URI+"NameInSchema"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable = { Table.class, Procedure.class }, 
            datatype = String.class, 
            display = "OData Type", 
            description = "Type of OData Schema Item",
            allowed = "COMPLEX, NAVIGATION, ENTITY, ENTITY_COLLECTION, ACTION, FUNCTION, COMPLEX_COLLECTION, NAVIGATION_COLLECTION",
            required=true)
    public static final String ODATA_TYPE = MetadataFactory.ODATA_URI+"Type"; //$NON-NLS-1$
    
    @ExtensionMetadataProperty(applicable=Table.class, 
            datatype=String.class, 
            display="Merge Into Table", 
            description="Declare the name of table that this table needs to be merged into.")
    public static final String MERGE = MetadataFactory.ODATA_URI+"MERGE"; //$NON-NLS-1$
    
    @ExtensionMetadataProperty(applicable=Column.class, 
            datatype=String.class, 
            display="Pseudo Column", 
            description="Pseudo column for join purposes")
    public static final String PSEUDO = MetadataFactory.ODATA_URI+"PSEUDO"; //$NON-NLS-1$    
    
    private String schemaNamespace;
    private ODataExecutionFactory ef;

    void setExecutionfactory(ODataExecutionFactory ef) {
        this.ef = ef;
    }
    
    public void process(MetadataFactory mf, WSConnection conn)
            throws TranslatorException {
        XMLMetadata serviceMetadata = getSchema(conn);    
        getMetadata(mf, serviceMetadata);
    }

    protected XMLMetadata getSchema(WSConnection conn) throws TranslatorException {
        if (this.ef != null) {
            return this.ef.getSchema(conn);
        }
        return null;
    }
    
    void getMetadata(MetadataFactory mf, XMLMetadata metadata)
            throws TranslatorException {
        CsdlSchema csdlSchema = getDefaultSchema(metadata);
        CsdlEntityContainer container = csdlSchema.getEntityContainer();

        // add entity sets as tables
        for (CsdlEntitySet entitySet : container.getEntitySets()) {
            addTable(mf, entitySet.getName(), entitySet.getType(), ODataType.ENTITY_COLLECTION, metadata);
        }

        // add singletons sets as tables
        for (CsdlSingleton singleton : container.getSingletons()) {
            addTable(mf, singleton.getName(), singleton.getType(), ODataType.ENTITY_COLLECTION, metadata);
        }

        // build relationships among tables
        for (CsdlEntitySet entitySet : container.getEntitySets()) {
            addNavigationProperties(mf, entitySet.getName(), entitySet,
                    metadata);
        }
        
        for (CsdlSingleton singleton : container.getSingletons()) {
            addNavigationProperties(mf, singleton.getName(), singleton,
                    metadata);
        }        
        
        // add PK colums for complex-types
        for (Table table : mf.getSchema().getTables().values()) {
            String parentTable = table.getProperty(PARENT_TABLE, false);
            if (parentTable != null) {
                addPrimaryKeyToComplexTables(mf, table, mf.getSchema().getTable(parentTable));
            }
        }        

        // build relations between tables
        for (Table table : mf.getSchema().getTables().values()) {
            String parentTable = table.getProperty(PARENT_TABLE, false);
            if (parentTable != null) {
                table.setProperty(PARENT_TABLE, null);
                addForeignKey(mf, table, mf.getSchema().getTable(parentTable));
            }
        }
        
        // add functions
        for (CsdlFunctionImport function : container.getFunctionImports()) {
            addFunctionImportAsProcedure(mf, function, ODataType.FUNCTION, metadata);
        }

        // add actions
        for (CsdlActionImport action : container.getActionImports()) {
            addActionImportAsProcedure(mf, action, ODataType.ACTION, metadata);
        }
    }



    private CsdlSchema getDefaultSchema(XMLMetadata metadata) throws TranslatorException {
        CsdlSchema csdlSchema = null;
        if (this.schemaNamespace != null) {
            csdlSchema = metadata.getSchema(this.schemaNamespace);
        } 
        else {
            if (!metadata.getSchemas().isEmpty()) {
                csdlSchema = metadata.getSchemas().get(0);
            }
        }
        if(csdlSchema == null) {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17019));
        }
        return csdlSchema;
    }

    private Table buildTable(MetadataFactory mf, String name) {
        Table table = mf.addTable(name);
        table.setSupportsUpdate(true);
        return table;
    }

    private boolean isSimple(String type) {
        return type.startsWith("Edm");
    }
    
    private boolean isEnum(XMLMetadata metadata, String type)
            throws TranslatorException {
        return getEnumType(metadata, type) != null;
    }

    private boolean isComplexType(XMLMetadata metadata, String type)
            throws TranslatorException {
        return getComplexType(metadata, type) != null;
    }

    private boolean isEntityType(XMLMetadata metadata, String type) throws TranslatorException {
        return getEntityType(metadata, type) != null;
    }

    private Table addTable(MetadataFactory mf, String tableName,
            String entityType, ODataType odataType, XMLMetadata metadata) 
            throws TranslatorException {
        Table table = buildTable(mf, tableName);
        table.setProperty(ODATA_TYPE, odataType.name());
        table.setProperty(NAME_IN_SCHEMA, entityType);
        
        CsdlEntityType type = getEntityType(metadata, entityType);
        addEntityTypeProperties(mf, metadata, table, type);
        return table;
    }    
    
    private void addEntityTypeProperties(MetadataFactory mf,
            XMLMetadata metadata, Table table, CsdlEntityType entityType)
            throws TranslatorException {
        // add columns; add complex types as child tables with 1-1 or 1-many
        // relation
        for (CsdlProperty property : entityType.getProperties()) {
            addProperty(mf, metadata, table, property);
        }

        // add properties from base type; if any to flatten the model
        String baseType = entityType.getBaseType();
        while (baseType != null) {
            CsdlEntityType baseEntityType = getEntityType(metadata, baseType);
            for (CsdlProperty property : baseEntityType.getProperties()) {
                addProperty(mf, metadata, table, property);
            }
            baseType = baseEntityType.getBaseType();
        }

        // add PK
        addPrimaryKey(mf, table, entityType.getKey()); //$NON-NLS-1$
    }

    private void addProperty(MetadataFactory mf, XMLMetadata metadata,
            Table table, CsdlProperty property) throws TranslatorException {
        if (isSimple(property.getType()) || isEnum(metadata, property.getType())) {
            addPropertyAsColumn(mf, table, property);
        }
        else {
            CsdlComplexType childType = (CsdlComplexType)getComplexType(metadata, property.getType());            
            addComplexPropertyAsTable(mf, property, childType, metadata, table);
        }
    }
    
    static String getPseudo(Column column) {
        return column.getProperty(ODataMetadataProcessor.PSEUDO, false);
    }
    
    static String getMerge(Table table) {
        return table.getProperty(ODataMetadataProcessor.MERGE, false);
    }    
    
    static boolean isComplexType(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.COMPLEX || type == ODataType.COMPLEX_COLLECTION;
    }
    
    static boolean isNavigationType(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.NAVIGATION || type == ODataType.NAVIGATION_COLLECTION;
    }    
    
    static boolean isCollection(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.ENTITY_COLLECTION
                || type == ODataType.COMPLEX_COLLECTION
                || type == ODataType.NAVIGATION_COLLECTION;
    }    
    
    static boolean isEntitySet(Table table) {
        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.ENTITY_COLLECTION;
    }    
    
    static String getNativeType(Column column) {
        String nativeType = column.getNativeType();
        if (nativeType == null) {
            nativeType = "Edm.String";
        }
        return nativeType;
    }
    
    private void addComplexPropertyAsTable(MetadataFactory mf, CsdlProperty parentProperty, 
            CsdlComplexType complexType, XMLMetadata metadata, Table parentTable) 
            throws TranslatorException {
        
        String tableName = parentTable.getName()+NAME_SEPARATOR+parentProperty.getName();
        Table childTable = buildTable(mf, tableName);
        childTable.setProperty(NAME_IN_SCHEMA, parentProperty.getType()); // complex type
        childTable.setProperty(ODATA_TYPE, 
                parentProperty.isCollection() ? 
                ODataType.COMPLEX_COLLECTION.name() : ODataType.COMPLEX.name()); // complex type
        childTable.setProperty(PARENT_TABLE, parentTable.getName());
        childTable.setProperty(MERGE, parentTable.getFullName());
        if (isComplexType(parentTable)) {
            childTable.setNameInSource(parentTable.getNameInSource()+"/"+parentProperty.getName());
        } else {
            childTable.setNameInSource(parentProperty.getName());
        }
        
        for (CsdlProperty property:complexType.getProperties()) {
            addProperty(mf, metadata, childTable, property);
        }
        
        // add properties from base type; if any to flatten the model
        String baseType = complexType.getBaseType();
        while(baseType != null) {
            CsdlComplexType baseComplexType = getComplexType(metadata, baseType);
            for (CsdlProperty property:baseComplexType.getProperties()) {
                addProperty(mf, metadata, childTable, property);
            }
            baseType = baseComplexType.getBaseType();
        }
    }

    void addPrimaryKey(MetadataFactory mf, Table table,
            List<CsdlPropertyRef> keys) throws TranslatorException {
        List<String> pkNames = new ArrayList<String>();
        for (CsdlPropertyRef ref : keys) {
            pkNames.add(ref.getName());
            if (ref.getAlias() != null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17018, 
                        table.getName(),ref.getName()));
            }
        }
        mf.addPrimaryKey("PK", pkNames, table);
    }

    private CsdlNavigationPropertyBinding getNavigationPropertyBinding(CsdlBindingTarget entitySet, String name) {
        List<CsdlNavigationPropertyBinding> bindings = entitySet.getNavigationPropertyBindings();
        for (CsdlNavigationPropertyBinding binding:bindings) {
            String path = binding.getPath();
            int index = path.lastIndexOf('/');
            if (index != -1) {
                path = path.substring(index+1);
            }
            if (path.equals(name)) {
                return binding;
            }
        }
        return null;
    }
    
    private CsdlEntityType getEntityType(XMLMetadata metadata, String name) throws TranslatorException {
        if(name == null) {
            return null;
        }
        
        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            CsdlSchema schema = metadata.getSchema(name.substring(0, idx));
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, name));
            }
            return schema.getEntityType(name.substring(idx+1));
        }
        return getDefaultSchema(metadata).getEntityType(name);
    }
    
    private List<CsdlFunction> getFunctions(XMLMetadata metadata, String name) throws TranslatorException {
        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            CsdlSchema schema = metadata.getSchema(name.substring(0, idx));
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, name));
            }
            return schema.getFunctions(name.substring(idx+1));
        }
        return getDefaultSchema(metadata).getFunctions(name);
    }    

    private List<CsdlAction> getActions (XMLMetadata metadata, String name) throws TranslatorException {
        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            CsdlSchema schema = metadata.getSchema(name.substring(0, idx));
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, name));
            }
            return schema.getActions(name.substring(idx+1));
        }
        return getDefaultSchema(metadata).getActions(name);
    }    
    
    private CsdlComplexType getComplexType(XMLMetadata metadata, String name) throws TranslatorException {
        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            CsdlSchema schema = metadata.getSchema(name.substring(0, idx));
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, name));
            }
            return schema.getComplexType(name.substring(idx+1));
        }
        return getDefaultSchema(metadata).getComplexType(name);
    }
    
    private CsdlEnumType getEnumType(XMLMetadata metadata, String name) throws TranslatorException {
        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            CsdlSchema schema = metadata.getSchema(name.substring(0, idx));
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, name));
            }
            return schema.getEnumType(name.substring(idx+1));
        }
        return getDefaultSchema(metadata).getEnumType(name);
    }    

    void addNavigationProperties(MetadataFactory mf, String tableName,
            CsdlBindingTarget entitySet, XMLMetadata metadata)
            throws TranslatorException {

        Table fromTable = mf.getSchema().getTable(tableName);
        CsdlEntityType fromEntityType = getEntityType(metadata,entitySet.getType());

        Table toTable = null;
        for (CsdlNavigationProperty property : fromEntityType.getNavigationProperties()) {
            CsdlNavigationPropertyBinding binding = getNavigationPropertyBinding(
                    entitySet, property.getName());

            if (binding != null) {
                String target = binding.getTarget();
                int index = target.lastIndexOf('/');
                if (index != -1) {
                    target = target.substring(index+1);
                }
                toTable = mf.getSchema().getTable(target);
                if(index != -1) {
                    toTable.setNameInSource(binding.getTarget());
                }
            } else {
                // this means there is no EntitySet defined for this EntityType,
                // or even if it is defined the set of rows are specific to this EntitySet
                StringBuilder name = new StringBuilder()
                        .append(fromTable.getName()).append(NAME_SEPARATOR)
                        .append(property.getName());
                toTable = addTable(mf, name.toString(), property.getType(), 
                        property.isCollection()?ODataType.NAVIGATION_COLLECTION:ODataType.NAVIGATION, 
                        metadata);
                toTable.setNameInSource(property.getName());                
            }

            // support for self-joins
            if (same(fromTable, toTable)) {
                StringBuilder name = new StringBuilder()
                        .append(fromTable.getName()).append(NAME_SEPARATOR)
                        .append(property.getName());
                toTable = addTable(mf, name.toString(), toTable.getProperty(NAME_IN_SCHEMA, false), 
                        property.isCollection()?ODataType.NAVIGATION_COLLECTION:ODataType.NAVIGATION, 
                        metadata);
                toTable.setNameInSource(property.getName());
            }
            toTable.setProperty(PARENT_TABLE, fromTable.getName());
            toTable.setProperty(MERGE, fromTable.getFullName());
            toTable.setProperty(FK_NAME, property.getName());
            
            int i = 0;
            for (CsdlReferentialConstraint constraint : property.getReferentialConstraints()) {
                toTable.setProperty(CONSTRAINT_PROPERTY + i, constraint.getReferencedProperty());
                toTable.setProperty(CONSTRAINT_REF_PROPERTY + i, constraint.getProperty());
                i++;
            }
        }
    }
    
    private KeyRecord getPK(MetadataFactory mf, Table table) throws TranslatorException {
        KeyRecord record = table.getPrimaryKey();
        if (record == null) {
            String parentTable = table.getProperty(PARENT_TABLE, false);
            if (parentTable == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17024, 
                        table.getName()));
            }
            return getPK(mf, mf.getSchema().getTable(parentTable));
        }
        return record;
    }
    
    private void addPrimaryKeyToComplexTables(MetadataFactory mf, Table childTable, Table parentTable) 
            throws TranslatorException {
        KeyRecord record = null;
        
        if (isComplexType(childTable)) {
            // these are complex type based tables.
            record = getPK(mf, parentTable);
        } else {
            // this is entity types now.
            record = parentTable.getPrimaryKey();
        }
        
        if (record == null) {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17025, 
                    parentTable.getName()));
        }

        int i = 0;
        List<String> columnNames = new ArrayList<String>();
        for (Column column:record.getColumns()) {
            String targetColumnName = isCollection(childTable) ? 
                    parentTable.getName() + "_" + column.getName() : column.getName();
            if (isNavigationType(childTable)) {
                Column c = mf.getSchema().getTable(childTable.getName()).getColumnByName(targetColumnName);
                if (c == null) {
                    c = addColumn(mf, childTable, column, targetColumnName);
                    c.setProperty(PSEUDO, column.getName());
                } else {
                    targetColumnName = column.getName();
                }
                
            } else {
                Column c = mf.getSchema().getTable(childTable.getName()).getColumnByName(column.getName());
                if (c == null) {
                    c = addColumn(mf, childTable, column, targetColumnName);
                    c.setProperty(PSEUDO, column.getName());
                } else {
                    targetColumnName = column.getName();
                }
            }
            columnNames.add(targetColumnName);
            
            // if there are no constraints are available then, define some implicit ones 
            if (childTable.getProperty(CONSTRAINT_PROPERTY + i, false) == null) {
                childTable.setProperty(CONSTRAINT_PROPERTY + i, targetColumnName);
                childTable.setProperty(CONSTRAINT_REF_PROPERTY + i, column.getName());
            }
            i++;
        }
        
        if (isComplexType(childTable)) {
            mf.addPrimaryKey("PK0", columnNames, childTable);
        }
    }

    private void addForeignKey(MetadataFactory mf, Table childTable, Table parentTable) 
            throws TranslatorException {
        String fkName = childTable.getProperty(FK_NAME, false);
        childTable.setProperty(FK_NAME, null);
        if (fkName == null) {
            fkName = "FK0";
        }
        
        int i = 0;
        ArrayList<String> keyColumns = new ArrayList<String>();
        ArrayList<String> refColumns = new ArrayList<String>();
        while(true) {
            if (childTable.getProperty(CONSTRAINT_PROPERTY + i, false) == null) {
                break;
            }
            keyColumns.add(childTable.getProperty(CONSTRAINT_PROPERTY + i, false));
            refColumns.add(childTable.getProperty(CONSTRAINT_REF_PROPERTY + i, false));
            
            childTable.setProperty(CONSTRAINT_PROPERTY + i, null);
            childTable.setProperty(CONSTRAINT_REF_PROPERTY + i, null);
            
            i++;
        }
        mf.addForiegnKey(fkName, keyColumns, refColumns, parentTable.getName(), childTable); //$NON-NLS-1$
    }    

    boolean same(Table x, Table y) {
        return (x.getFullName().equalsIgnoreCase(y.getFullName()));
    }

    boolean keyMatches(List<String> names, KeyRecord record) {
        if (names.size() != record.getColumns().size()) {
            return false;
        }
        Set<String> keyNames = new TreeSet<String>(
                String.CASE_INSENSITIVE_ORDER);
        for (Column c : record.getColumns()) {
            keyNames.add(c.getName());
        }
        for (int i = 0; i < names.size(); i++) {
            if (!keyNames.contains(names.get(i))) {
                return false;
            }
        }
        return true;
    }

    private Column addPropertyAsColumn(MetadataFactory mf, Table table,
            CsdlProperty property) {

        Column c = buildColumn(mf, table, property);

        c.setNullType(property.isNullable() ? NullType.Nullable
                : NullType.No_Nulls);

        if (property.getMaxLength() != null) {
            c.setLength(property.getMaxLength());
        }
        if (property.getPrecision() != null) {
            c.setPrecision(property.getPrecision());
        }
        if (property.getScale() != null) {
            c.setScale(property.getScale());
        }
        if (property.getDefaultValue() != null) {
            c.setDefaultValue(property.getDefaultValue());
        }

        if (property.getMimeType() != null) {
            c.setProperty("MIME-TYPE", property.getMimeType());
        }

        if (property.getSrid() != null) {
            c.setProperty(BaseColumn.SPATIAL_SRID, property.getSrid().toString());
        }
        if (!property.getType().equals("Edm.String")) {
            if (property.isCollection()) {
                c.setNativeType("Collection("+property.getType()+")");
            } else {
                c.setNativeType(property.getType());
            }
        }
        if (property.getType().equals("Edm.String") && property.isCollection()) {
            c.setNativeType("Collection("+property.getType()+")");
        }        
        return c;
    }
    
    private Column addColumn(MetadataFactory mf, Table table, Column property, String newName) {
        Column c = mf.addColumn(newName, property.getRuntimeType(),table);
        c.setUpdatable(true);
        c.setNullType(property.getNullType());
        c.setLength(property.getLength());
        c.setPrecision(property.getPrecision());
        c.setScale(property.getScale());
        c.setDefaultValue(property.getDefaultValue());
        c.setProperty("MIME-TYPE", c.getProperty("MIME-TYPE", false));
        c.setProperty(BaseColumn.SPATIAL_SRID, property.getProperty(BaseColumn.SPATIAL_SRID, false));
        c.setProperty("NATIVE_TYPE", property.getProperty("NATIVE_TYPE", false));
        return c;
    }

    private ProcedureParameter addParameterAsColumn(MetadataFactory mf,
            Procedure procedure, CsdlParameter parameter) {
        ProcedureParameter p = mf.addProcedureParameter(
                parameter.getName(),
                ODataTypeManager.teiidType(parameter.getType(),parameter.isCollection()), 
                ProcedureParameter.Type.In,
                procedure);
        
        p.setNullType(parameter.isNullable()?NullType.Nullable:NullType.No_Nulls);
        
        if (parameter.getMaxLength() != null) {
            p.setLength(parameter.getMaxLength());
        }
        if (parameter.getPrecision() != null) {
            p.setPrecision(parameter.getPrecision());
        }
        if (parameter.getScale() != null) {
            p.setScale(parameter.getScale());
        }
        if (parameter.getSrid() != null) {
            p.setProperty(BaseColumn.SPATIAL_SRID, parameter.getSrid().toString());
        }
        return p;
    }    

    private Column buildColumn(MetadataFactory mf, Table table, CsdlProperty property) {
        String columnName = property.getName();
        Column c = mf.addColumn(columnName, ODataTypeManager.teiidType(property.getType(), 
                property.isCollection()), table);
        if (TypeFacility.RUNTIME_NAMES.GEOMETRY.equals(c.getDatatype().getName())) {
        	String type = property.getType();
        	if (type.startsWith(EDM_GEOMETRY) && type.length() > EDM_GEOMETRY.length()) {
        		c.setProperty(BaseColumn.SPATIAL_TYPE, type.substring(EDM_GEOMETRY.length()).toUpperCase());
        	}
        }
        if(property.isCollection()) {
            c.setSearchType(SearchType.Unsearchable);
        }
        c.setUpdatable(true);
        return c;
    }

    private void addParameter(MetadataFactory mf, XMLMetadata metadata,
            Procedure procedure, CsdlParameter parameter) throws TranslatorException {
        if (isSimple(parameter.getType())) {
            addParameterAsColumn(mf, procedure, parameter);
        }
        else {
            throw new TranslatorException(ODataPlugin.Util.gs(
                    ODataPlugin.Event.TEIID17022, parameter.getName()));
        }
    }
    
    void addFunctionImportAsProcedure(MetadataFactory mf,
            CsdlFunctionImport functionImport, ODataType odataType, XMLMetadata metadata)
            throws TranslatorException {
        List<CsdlFunction> functions = getFunctions(metadata,
                functionImport.getFunction());
        for (CsdlFunction function : functions) {
            Procedure procedure = mf.addProcedure(function.getName());
            addOperation(mf, metadata, odataType, function, procedure);
        }
    }

    private void addProcedureTableReturn(MetadataFactory mf, XMLMetadata metadata, Procedure procedure, 
            CsdlComplexType type, String namePrefix) throws TranslatorException {
        for (CsdlProperty property:type.getProperties()) {
            if (isSimple(property.getType()) || isEnum(metadata, property.getType())) {                 
                mf.addProcedureResultSetColumn(
                        namePrefix == null? property.getName():namePrefix+"_"+property.getName(),
                        ODataTypeManager.teiidType(property.getType(), property.isCollection()), procedure);
            }
            else if (isComplexType(metadata, property.getType())) {
                CsdlComplexType childType = (CsdlComplexType)getComplexType(metadata, property.getType());
                addProcedureTableReturn(mf, metadata, procedure, childType, property.getName());
            }
            else {
                throw new TranslatorException(ODataPlugin.Util.gs(
                        ODataPlugin.Event.TEIID17023, procedure.getName()));
            }
        }
    }
    
    private void addProcedureTableReturn(MetadataFactory mf, XMLMetadata metadata, Procedure procedure, 
            CsdlEntityType type, String namePrefix) throws TranslatorException {
        for (CsdlProperty property:type.getProperties()) {
            if (isSimple(property.getType()) || isEnum(metadata, property.getType())) {
                mf.addProcedureResultSetColumn(
                        namePrefix == null? property.getName():namePrefix+"_"+property.getName(),
                        ODataTypeManager.teiidType(property.getType(), property.isCollection()), procedure);
            }
            else if (isComplexType(metadata, property.getType())) {
                CsdlComplexType childType = (CsdlComplexType)getComplexType(metadata, property.getType());
                addProcedureTableReturn(mf, metadata, procedure, childType, property.getName());
            }            
            else {
                throw new TranslatorException(ODataPlugin.Util.gs(
                        ODataPlugin.Event.TEIID17023, procedure.getName()));
            }
        }
    }
    
    private void addActionImportAsProcedure(MetadataFactory mf, CsdlActionImport actionImport, 
            ODataType odataType, XMLMetadata metadata) throws TranslatorException {
        List<CsdlAction> actions = getActions(metadata, actionImport.getAction());
        
        for (CsdlAction action : actions) {
            if (!hasComplexParameters(action.getParameters())) { 
                Procedure procedure = mf.addProcedure(action.getName());
                addOperation(mf, metadata, odataType, action, procedure);
            } else {
                LogManager.logInfo(LogConstants.CTX_ODATA, 
                        ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17033, action.getName()));
            }
        }
        
    }

    private boolean hasComplexParameters(List<CsdlParameter> parameters) {
        for (CsdlParameter parameter:parameters) {
            if (!isSimple(parameter.getType())) {
                return false;
            }
        }
        return false;
    }

    private void addOperation(MetadataFactory mf, XMLMetadata metadata, ODataType odataType,
            CsdlOperation function, Procedure procedure)
            throws TranslatorException {
        
        procedure.setProperty(ODATA_TYPE, odataType.name());
        
        for (CsdlParameter parameter : function.getParameters()) {
            addParameter(mf, metadata, procedure, parameter);
        }
        
        CsdlReturnType returnType = function.getReturnType();
        if (returnType != null) {
            if (isSimple(returnType.getType())) {
                mf.addProcedureParameter("return", ODataTypeManager.teiidType(returnType.getType(), 
                        returnType.isCollection()), ProcedureParameter.Type.ReturnValue, procedure); 
            } 
            else if (isComplexType(metadata, returnType.getType())) {
                addProcedureTableReturn(mf, metadata, procedure,
                        getComplexType(metadata, returnType.getType()), null);
                procedure.getResultSet().setProperty(ODATA_TYPE, 
                        returnType.isCollection()?ODataType.COMPLEX_COLLECTION.name():ODataType.COMPLEX.name());
            }
            else if (isEntityType(metadata, returnType.getType())){
                addProcedureTableReturn(mf, metadata, procedure,
                        getEntityType(metadata, returnType.getType()), null);
                procedure.getResultSet().setProperty(ODATA_TYPE, 
                        returnType.isCollection()?ODataType.ENTITY_COLLECTION.name():ODataType.ENTITY.name());
            }
            else {
                throw new TranslatorException(ODataPlugin.Util.gs(
                        ODataPlugin.Event.TEIID17005, function.getName(),
                        returnType.getType()));
            }
        }
    }

    public void setSchemaNamespace(String namespace) {
        this.schemaNamespace = namespace;
    }

    List<String> getColumnNames(List<Column> columns) {
        ArrayList<String> names = new ArrayList<String>();
        for (Column c : columns) {
            names.add(c.getName());
        }
        return names;
    }

    @TranslatorProperty(display="Schema Namespace", category=PropertyType.IMPORT, description="Namespace of the schema to import")
    public String getSchemaNamespace() {
        return schemaNamespace;
    }
}
