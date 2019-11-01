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

import org.apache.olingo.client.api.edm.xml.XMLMetadata;
import org.apache.olingo.commons.api.edm.geo.SRID;
import org.apache.olingo.commons.api.edm.provider.*;
import org.teiid.core.types.DataTypeManager;
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
import org.teiid.translator.ws.WSConnection;
import org.teiid.translator.TypeFacility;
import org.teiid.util.FullyQualifiedName;

public class ODataMetadataProcessor implements MetadataProcessor<WSConnection> {
    private static final String EDM_GEOMETRY = "Edm.Geometry"; //$NON-NLS-1$
    private static final String EDM_GEOGRAPHY = "Edm.Geography"; //$NON-NLS-1$

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

    private static final String NAME_SEPARATOR = "_"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable = { Table.class, Procedure.class },
            datatype = String.class,
            display = "Name in OData Schema",
            description = "Name in OData Schema",
            required = true)
    public static final String NAME_IN_SCHEMA = MetadataFactory.ODATA_PREFIX+"NameInSchema"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable = { Table.class, Procedure.class },
            datatype = String.class,
            display = "OData Type",
            description = "Type of OData Schema Item",
            allowed = "COMPLEX, NAVIGATION, ENTITY, ENTITY_COLLECTION, ACTION, FUNCTION, COMPLEX_COLLECTION, NAVIGATION_COLLECTION",
            required=true)
    public static final String ODATA_TYPE = MetadataFactory.ODATA_PREFIX+"Type"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class,
            datatype=String.class,
            display="Pseudo Column",
            description="Pseudo column for join purposes")
    public static final String PSEUDO = MetadataFactory.ODATA_PREFIX+"PSEUDO"; //$NON-NLS-1$

    private String schemaNamespace;
    private ODataExecutionFactory ef;

    void setExecutionfactory(ODataExecutionFactory ef) {
        this.ef = ef;
    }

    @Override
    public void process(MetadataFactory mf, WSConnection conn)
            throws TranslatorException {
        XMLMetadata serviceMetadata = getSchema(conn);
        try {
            getMetadata(mf, serviceMetadata);
        } catch (NullPointerException e) {
            throw new TranslatorException(e, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17034));
        }
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

        if (container == null) {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17035, csdlSchema.getNamespace()));
        }

        // add entity sets as tables
        for (CsdlEntitySet entitySet : container.getEntitySets()) {
            Table t = addTable(mf, entitySet.getName(), entitySet.getType(), ODataType.ENTITY_COLLECTION, metadata);
            FullyQualifiedName fqn = new FullyQualifiedName("entity container", container.getName()==null?"default":container.getName()); //$NON-NLS-1$ //$NON-NLS-2$
            fqn.append("entity set", entitySet.getName()); //$NON-NLS-1$
            t.setProperty(FQN, fqn.toString());
        }

        // add singletons sets as tables
        for (CsdlSingleton singleton : container.getSingletons()) {
            Table t = addTable(mf, singleton.getName(), singleton.getType(), ODataType.ENTITY_COLLECTION, metadata);
            FullyQualifiedName fqn = new FullyQualifiedName("entity container", container.getName()==null?"default":container.getName()); //$NON-NLS-1$ //$NON-NLS-2$
            fqn.append("singleton", singleton.getName()); //$NON-NLS-1$
            t.setProperty(FQN, fqn.toString());
        }

        // build relationships among tables
        for (CsdlEntitySet entitySet : container.getEntitySets()) {
            addNavigationProperties(mf, entitySet.getName(), entitySet,
                    metadata, container);
        }

        for (CsdlSingleton singleton : container.getSingletons()) {
            addNavigationProperties(mf, singleton.getName(), singleton,
                    metadata, container);
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
        List<CsdlProperty> complexTypes = new ArrayList<CsdlProperty>();
        for (CsdlProperty property : entityType.getProperties()) {
            if (!addProperty(mf, metadata, table, property)) {
                complexTypes.add(property);
            }
        }

        // add properties from base type; if any to flatten the model
        String baseType = entityType.getBaseType();
        while (baseType != null) {
            CsdlEntityType baseEntityType = getEntityType(metadata, baseType);
            for (CsdlProperty property : baseEntityType.getProperties()) {
                if (!addProperty(mf, metadata, table, property)) {
                    complexTypes.add(property);
                }
            }
            baseType = baseEntityType.getBaseType();
        }

        // add PK
        addPrimaryKey(mf, metadata, table, entityType);

        // add properties that depend on the pk
        for (CsdlProperty property : complexTypes) {
            addComplexPropertyAsTable(mf, property, getComplexType(metadata, property.getType()), metadata, table);
        }
    }

    private boolean addProperty(MetadataFactory mf, XMLMetadata metadata,
            Table table, CsdlProperty property) throws TranslatorException {
        if (isSimple(property.getType()) || isEnum(metadata, property.getType())) {
            if (table.getColumnByName(property.getName()) == null) {
                addPropertyAsColumn(mf, table, property);
            }
            return true;
        }
        return false;
    }

    static boolean isPseudo(Column column) {
        return Boolean.parseBoolean(column.getProperty(ODataMetadataProcessor.PSEUDO, false));
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
        String parent = parentTable.getProperty(FQN, false);
        childTable.setProperty(FQN, parent + FullyQualifiedName.SEPARATOR + new FullyQualifiedName("complex property", parentProperty.getName()).toString()); //$NON-NLS-1$
        childTable.setProperty(NAME_IN_SCHEMA, parentProperty.getType()); // complex type
        childTable.setProperty(ODATA_TYPE,
                parentProperty.isCollection() ?
                ODataType.COMPLEX_COLLECTION.name() : ODataType.COMPLEX.name()); // complex type

        // add a primary key to complex table
        KeyRecord pk = parentTable.getPrimaryKey();
        List<Column> pkColumns = new ArrayList<Column>();
        for (Column c : pk.getColumns()) {
            String colName = parentTable.getName() + NAME_SEPARATOR + c.getName();
            // if the parent is already a complex, just copy the its PK
            if (isComplexType(parentTable)) {
                colName = c.getName();
            }
            Column col = addColumn(mf, childTable, c, colName);
            pkColumns.add(col);
            col.setProperty(PSEUDO, String.valueOf(Boolean.TRUE));
        }
        mf.addPrimaryKey("PK0", getColumnNames(pkColumns), childTable);
        mf.addForeignKey("FK0", getColumnNames(pkColumns), getColumnNames(pk.getColumns()), parentTable.getFullName(),
                childTable);

        if (isComplexType(parentTable)) {
            childTable.setNameInSource(parentTable.getNameInSource()+"/"+parentProperty.getName());
        } else {
            childTable.setNameInSource(parentProperty.getName());
        }

        List<CsdlProperty> complexTypes = new ArrayList<CsdlProperty>();
        for (CsdlProperty property : complexType.getProperties()) {
            if (!addProperty(mf, metadata, childTable, property)) {
                complexTypes.add(property);
            }
        }

        // add properties from base type; if any to flatten the model
        String baseType = complexType.getBaseType();
        while(baseType != null) {
            CsdlComplexType baseComplexType = getComplexType(metadata, baseType);
            for (CsdlProperty property:baseComplexType.getProperties()) {
                if (!addProperty(mf, metadata, childTable, property)) {
                    complexTypes.add(property);
                }
            }
            baseType = baseComplexType.getBaseType();
        }

        for (CsdlProperty property : complexTypes) {
            addComplexPropertyAsTable(mf, property, getComplexType(metadata, property.getType()), metadata, childTable);
        }
    }

    void addPrimaryKey(MetadataFactory mf, XMLMetadata metadata, Table table, CsdlEntityType entityType)
            throws TranslatorException {
        List<CsdlPropertyRef> keys = entityType.getKey();
        List<String> pkNames = new ArrayList<String>();
        for (CsdlPropertyRef ref : keys) {
            if (!addProperty(mf, metadata, table, entityType.getProperty(ref.getName()))) {
                throw new AssertionError("Complex type not allowed as part of primary key"); //$NON-NLS-1$
            }
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

        if (name.startsWith("Collection")) {
            int start = name.indexOf('(');
            int end = name.indexOf(')');
            name = name.substring(start+1, end).trim();
        }

        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            String schemaName = name.substring(0, idx);
            CsdlSchema schema = metadata.getSchema(schemaName);
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, schemaName));
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
            CsdlBindingTarget entitySet, XMLMetadata metadata, CsdlEntityContainer container)
            throws TranslatorException {

        Table fromTable = mf.getSchema().getTable(tableName);
        CsdlEntityType fromEntityType = getEntityType(metadata,entitySet.getType());


        for (CsdlNavigationProperty property : fromEntityType.getNavigationProperties()) {
            CsdlNavigationPropertyBinding binding = getNavigationPropertyBinding(
                    entitySet, property.getName());

            Table toTable = null;

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
            } else if (property.isContainsTarget()) {
                // it is defined the set of rows are specific to this EntitySet
                toTable = addNavigationAsTable(mf, metadata, fromTable, property);
                String parent = fromTable.getProperty(FQN, false);
                toTable.setProperty(FQN, parent + FullyQualifiedName.SEPARATOR + new FullyQualifiedName("contained", property.getName()).toString()); //$NON-NLS-1$
            } else {
                //cannot determine the target
                //could also be an exception
                continue;
            }

            // support for self-joins
            if (same(fromTable, toTable)) {
                toTable = addNavigationAsTable(mf, metadata, fromTable, property);
                String parent = fromTable.getProperty(FQN, false);
                toTable.setProperty(FQN, parent + FullyQualifiedName.SEPARATOR + new FullyQualifiedName().append("self join", property.getName()).toString()); //$NON-NLS-1$
            }

            List<String> columnNames = new ArrayList<String>();
            List<String> referenceColumnNames = new ArrayList<String>();
            if (property.getReferentialConstraints().isEmpty()) {
                if (!isNavigationType(toTable) && !hasReverseNavigation(mf, metadata, container,
                        fromTable, property, binding, toTable)) {
                    if (property.isCollection()) {
                        addImplicitFk(mf, fromTable, property, toTable,
                                columnNames, referenceColumnNames);
                    } else {
                        addImplicitFk(mf, toTable, property, fromTable,
                                columnNames, referenceColumnNames);
                    }
                }
            } else if (!property.isCollection()) { //sanity check - it should not be a collection
                for (CsdlReferentialConstraint constraint : property.getReferentialConstraints()) {
                    columnNames.add(constraint.getProperty());
                    referenceColumnNames.add(constraint.getReferencedProperty());
                }
                mf.addForeignKey(join(fromTable.getName(), NAME_SEPARATOR, property.getName()), columnNames,
                        referenceColumnNames, toTable.getFullName(), fromTable);
            }
        }
    }

    private void addImplicitFk(MetadataFactory mf, Table fromTable,
            CsdlNavigationProperty property, Table toTable,
            List<String> columnNames, List<String> referenceColumnNames) {
        KeyRecord pk = getPKorUnique(fromTable);
        if (pk != null) {
            boolean matches = true;

            for (Column c : pk.getColumns()) {
                referenceColumnNames.add(c.getName());
                if (toTable.getColumnByName(c.getName()) != null) {
                    columnNames.add(c.getName());
                } else {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                mf.addForeignKey(join(fromTable.getName(), NAME_SEPARATOR, property.getName()), columnNames,
                        referenceColumnNames, fromTable.getFullName(), toTable);
            }
        }
    }

    private boolean hasReverseNavigation(MetadataFactory mf, XMLMetadata metadata,
            CsdlEntityContainer container, Table fromTable,
            CsdlNavigationProperty property,
            CsdlNavigationPropertyBinding binding, Table toTable)
            throws TranslatorException, AssertionError {
        if (!property.isCollection() || binding == null) {
            return false;
        }
        CsdlEntityType toEntityType = getEntityType(metadata,toTable.getProperty(NAME_IN_SCHEMA, false));
        for (CsdlNavigationProperty toProperty : toEntityType.getNavigationProperties()) {
            if (toProperty.isCollection() || toProperty.isContainsTarget()) {
                continue;
            }
            CsdlEntitySet toEntitySet = container.getEntitySet(binding.getTarget());
            if (toEntitySet == null) {
                continue;
            }
            CsdlNavigationPropertyBinding toBinding = getNavigationPropertyBinding(
                toEntitySet, toProperty.getName());
            if (toBinding == null) {
                continue;
            }
            String toTarget = toBinding.getTarget();
            int index = toTarget.lastIndexOf('/');
            if (index != -1) {
                toTarget = toTarget.substring(index+1);
            }
            Table reverseTable = mf.getSchema().getTable(toTarget);
            if (same(fromTable, reverseTable)) {
                return true;
            }
        }
        return false;
    }

    private Table addNavigationAsTable(MetadataFactory mf, XMLMetadata metadata, Table fromTable,
            CsdlNavigationProperty property) throws TranslatorException {
        String name = join(fromTable.getName(), NAME_SEPARATOR, property.getName());
        Table toTable = addTable(mf, name, property.getType(),
                property.isCollection()?ODataType.NAVIGATION_COLLECTION:ODataType.NAVIGATION,
                metadata);
        toTable.setNameInSource(property.getName());

        KeyRecord pk = fromTable.getPrimaryKey();
        List<String> columnNames = new ArrayList<String>();
        for (Column c : pk.getColumns()) {
            String columnName = join(fromTable.getName(), NAME_SEPARATOR, c.getName());
            Column column = mf.addColumn(columnName, c.getRuntimeType(), toTable);
            column.setProperty(PSEUDO, String.valueOf(Boolean.TRUE));
            columnNames.add(columnName);
        }
        mf.addForeignKey("FK0", columnNames, getColumnNames(pk.getColumns()), fromTable.getFullName(), toTable);
        return toTable;
    }

    static String join(String... records) {
        StringBuffer sb = new StringBuffer();
        for (String r: records) {
            sb.append(r);
        }
        return sb.toString();
    }

    boolean same(Table x, Table y) {
        return (x.getFullName().equalsIgnoreCase(y.getFullName()));
    }

    private Column addPropertyAsColumn(MetadataFactory mf, Table table,
            CsdlProperty property) {

        Column c = buildColumn(mf, table, property);

        c.setNullType(property.isNullable() ? NullType.Nullable
                : NullType.No_Nulls);

        if (property.getMaxLength() != null) {
            c.setLength(property.getMaxLength());
        }
        if (property.getDefaultValue() != null) {
            c.setDefaultValue(property.getDefaultValue());
        }

        if (property.getMimeType() != null) {
            c.setProperty("MIME-TYPE", property.getMimeType());
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

        // mismatch with timestamp type and odata
        if (c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.TIMESTAMP)) {
            if (property.getPrecision() != null){
                c.setScale(property.getPrecision());
            }
        } else {
            if (property.getPrecision() != null) {
                c.setPrecision(property.getPrecision());
            }
            if (property.getScale() != null) {
                c.setScale(property.getScale());
            }
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
        handleGeometryTypes(parameter.getSrid(), parameter.getType(), p);
        return p;
    }

    private Column buildColumn(MetadataFactory mf, Table table, CsdlProperty property) {
        String columnName = property.getName();
        Column c = mf.addColumn(columnName, ODataTypeManager.teiidType(property.getType(),
                property.isCollection()), table);
        handleGeometryTypes(property.getSrid(), property.getType(), c);
        if(property.isCollection()) {
            c.setSearchType(SearchType.Unsearchable);
        }
        c.setUpdatable(true);
        return c;
    }

    private void handleGeometryTypes(SRID srid, String type, BaseColumn c) {
        //TODO: geometry arrays
        if (!TypeFacility.RUNTIME_NAMES.GEOMETRY.equals(c.getDatatype().getName())) {
            return;
        }
        if (type.startsWith(EDM_GEOMETRY)) {
            if (type.length() > EDM_GEOMETRY.length()) {
                c.setProperty(BaseColumn.SPATIAL_TYPE, type.substring(EDM_GEOMETRY.length()).toUpperCase());
            } else {
                c.setProperty(BaseColumn.SPATIAL_TYPE, "GEOMETRY"); //$NON-NLS-1$
            }
        } else if (type.startsWith(EDM_GEOGRAPHY)) {
            c.setProperty(BaseColumn.SPATIAL_SRID, "4326"); //$NON-NLS-1$
            if (type.length() > EDM_GEOGRAPHY.length()) {
                c.setProperty(BaseColumn.SPATIAL_TYPE, type.substring(EDM_GEOGRAPHY.length()).toUpperCase());
            } else {
                c.setProperty(BaseColumn.SPATIAL_TYPE, "GEOMETRY"); //$NON-NLS-1$
            }
        }
        if (srid != null && srid.isNotDefault()) {
            String value = srid.toString();

            if (!value.equalsIgnoreCase("VARIABLE")) { //$NON-NLS-1$
                c.setProperty(BaseColumn.SPATIAL_SRID, value);
            }
        }
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
                CsdlComplexType childType = getComplexType(metadata, property.getType());
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
                CsdlComplexType childType = getComplexType(metadata, property.getType());
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

    KeyRecord getPKorUnique(Table table) {
        KeyRecord pk = table.getPrimaryKey();
        if (pk == null && !table.getUniqueKeys().isEmpty()) {
            pk = table.getUniqueKeys().get(0);
        }
        return pk;
    }

    static Table getComplexTableParentTable(RuntimeMetadata metadata, Table table) throws TranslatorException {
        for (Column c : table.getColumns()) {
            if (ODataMetadataProcessor.isPseudo(c)) {
                ForeignKey fk = table.getForeignKeys().get(0);
                String tableName = fk.getReferenceTableName();
                if (tableName.indexOf('.') == -1) {
                    tableName = fk.getReferenceKey().getParent().getFullName();
                }
                return metadata.getTable(tableName);
            }
        }
        return table;
    }

    static Column normalizePseudoColumn(RuntimeMetadata metadata, Column column) throws TranslatorException {
        if (ODataMetadataProcessor.isPseudo(column)) {
            Table table = (Table)column.getParent();
            ForeignKey fk = table.getForeignKeys().get(0);
            for (int i = 0; i < fk.getColumns().size(); i++) {
                Column c = fk.getColumns().get(i);
                if (c.getName().equals(column.getName())) {
                    String refColumn = fk.getReferenceColumns().get(i);
                    String tableName = fk.getReferenceTableName();
                    if (tableName.indexOf('.') == -1) {
                        tableName = fk.getReferenceKey().getParent().getFullName();
                    }
                    Table refTable = metadata.getTable(tableName);
                    return refTable.getColumnByName(refColumn);
                }
            }
        }
        return column;
    }

    @TranslatorProperty(display="Schema Namespace", category=PropertyType.IMPORT, description="Namespace of the schema to import")
    public String getSchemaNamespace() {
        return schemaNamespace;
    }
}
