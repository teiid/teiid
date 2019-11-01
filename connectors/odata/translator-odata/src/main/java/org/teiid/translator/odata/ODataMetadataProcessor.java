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

import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.ws.rs.core.Response.Status;

import org.odata4j.edm.*;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

public class ODataMetadataProcessor implements MetadataProcessor<WSConnection> {

    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Link Tables", description="Used to define navigation relationship in many to many case")
    public static final String LINK_TABLES = MetadataFactory.ODATA_PREFIX+"LinkTables"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Http Method", description="Http method used for procedure invocation", required=true)
    public static final String HTTP_METHOD = MetadataFactory.ODATA_PREFIX+"HttpMethod"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class, datatype=Boolean.class, display="Join Column", description="On Link tables this property defines the join column")
    public static final String JOIN_COLUMN = MetadataFactory.ODATA_PREFIX+"JoinColumn"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable= {Table.class, Procedure.class}, datatype=String.class, display="Entity Type Name", description="Name of the Entity Type in EDM", required=true)
    public static final String ENTITY_TYPE = MetadataFactory.ODATA_PREFIX+"EntityType"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Complex Type Name", description="Name of the Complex Type in EDM")
    public static final String COMPLEX_TYPE = MetadataFactory.ODATA_PREFIX+"ComplexType"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Column Group", description="Name of the Column Group")
    public static final String COLUMN_GROUP = MetadataFactory.ODATA_PREFIX+"ColumnGroup"; //$NON-NLS-1$

    private String entityContainer;
    private String schemaNamespace;
    private ODataExecutionFactory ef;

    public void setExecutionfactory(ODataExecutionFactory ef) {
        this.ef = ef;
    }

    private EdmDataServices getEds(WSConnection conn) throws TranslatorException {
        try {
            BaseQueryExecution execution = new BaseQueryExecution(ef, null, null, conn);
            BinaryWSProcedureExecution call = execution.executeDirect("GET", "$metadata", null, execution.getDefaultHeaders()); //$NON-NLS-1$ //$NON-NLS-2$
            if (call.getResponseCode() != Status.OK.getStatusCode()) {
                throw execution.buildError(call);
            }

            Blob out = (Blob)call.getOutputParameterValues().get(0);

            EdmDataServices eds = new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new InputStreamReader(out.getBinaryStream())));
            return eds;
        } catch (SQLException e) {
            throw new TranslatorException(e);
        } catch (Exception e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public void process(MetadataFactory mf, WSConnection conn) throws TranslatorException {
        EdmDataServices eds = getEds(conn);
        getMetadata(mf, eds);
    }

    public void getMetadata(MetadataFactory mf, EdmDataServices eds) throws TranslatorException {

        for (EdmSchema schema:eds.getSchemas()) {

            if (this.schemaNamespace != null && !this.schemaNamespace.equalsIgnoreCase(schema.getNamespace())) {
                continue;
            }

            for (EdmEntityContainer container:schema.getEntityContainers()) {
                if ((this.entityContainer != null && this.entityContainer.equalsIgnoreCase(container.getName()))
                        || container.isDefault()) {

                    // add entity sets as tables
                    for (EdmEntitySet entitySet:container.getEntitySets()) {
                        addEntitySetAsTable(mf, entitySet);
                    }

                    // build relations ships among tables
                    for (EdmEntitySet entitySet:container.getEntitySets()) {
                        addNavigationRelations(mf, entitySet.getName(), entitySet.getType());
                    }

                    // add procedures
                    for (EdmFunctionImport function:container.getFunctionImports()) {
                        addFunctionImportAsProcedure(mf, function);
                    }
                }
            }
        }
    }

    protected Table buildTable(MetadataFactory mf, EdmEntitySet entitySet) {
        Table table = mf.addTable(entitySet.getName());
        table.setSupportsUpdate(true);
        return table;
    }

    protected Table addEntitySetAsTable(MetadataFactory mf, EdmEntitySet entitySet) throws TranslatorException {
        Table table = buildTable(mf, entitySet);
        table.setProperty(ENTITY_TYPE, entitySet.getType().getFullyQualifiedTypeName());

        // add columns
        for (EdmProperty ep:entitySet.getType().getProperties().toList()) {
            if (ep.getType().isSimple()
                    || (ep.getType() instanceof EdmCollectionType
                    && ((EdmCollectionType)ep.getType()).getItemType().isSimple())) {
                addPropertyAsColumn(mf, table, ep, entitySet);
            }
            else {
                // this is complex type, i.e treat them as embeddable in the same table add all columns.
                // Have tried adding this as separate table with 1 to 1 mapping to parent table, however
                // that model fails when there are two instances of single complex type as column. This
                // creates verbose columns but safe.
                EdmComplexType embedded = (EdmComplexType)ep.getType();
                for (EdmProperty property:embedded.getProperties().toList()) {
                    if (property.getType().isSimple()
                            || (property.getType() instanceof EdmCollectionType
                            && ((EdmCollectionType)property.getType()).getItemType().isSimple())) {
                        Column column = addPropertyAsColumn(mf, table, property, entitySet, ep.getName());
                        column.setProperty(COMPLEX_TYPE, embedded.getFullyQualifiedTypeName()); // complex type
                        column.setProperty(COLUMN_GROUP, ep.getName()); // name of parent column
                    }
                    else {
                        throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17002, entitySet.getName(), ep.getName()));
                    }
                }
            }
        }

        // add PK
        mf.addPrimaryKey("PK", entitySet.getType().getKeys(), table); //$NON-NLS-1$
        return table;
    }

    void addNavigationRelations(MetadataFactory mf, String tableName, EdmEntityType fromEntity) throws TranslatorException {
        Table fromTable = mf.getSchema().getTable(tableName);

        for(EdmNavigationProperty nav:fromEntity.getNavigationProperties()) {
            EdmAssociation association = nav.getRelationship();

            EdmAssociationEnd fromEnd = nav.getFromRole();
            EdmAssociationEnd toEnd = nav.getToRole();

            EdmEntityType toEntity = toEnd.getType();

            // no support for self-joinsaddPropertyAsColumn
            if (same(fromEntity, toEntity)) {
                continue;
            }

            // Usually navigation name is navigation table name.
            Table toTable = mf.getSchema().getTable(nav.getName());
            if (toTable == null) {
                // if the table not found; then navigation name may be an alias name
                // find by the entity type
                toTable = getEntityTable(mf, toEntity);
            }

            if (isMultiplicityMany(fromEnd) && isMultiplicityMany(toEnd)) {
                if (mf.getSchema().getTable(association.getName()) == null) {
                    Table linkTable = mf.addTable(association.getName());
                    linkTable.setProperty(ENTITY_TYPE, "LinkTable"); //$NON-NLS-1$
                    linkTable.setProperty(LINK_TABLES, fromTable.getName()+","+toTable.getName()); //$NON-NLS-1$

                    //left table
                    List<String> leftNames = null;
                    if (association.getRefConstraint() != null) {
                        leftNames = association.getRefConstraint().getPrincipalReferences();
                    }
                    leftNames = addLinkTableColumns(mf, fromTable, leftNames, linkTable);

                    //right table
                    List<String> rightNames = null;
                    if (association.getRefConstraint() != null) {
                        rightNames = association.getRefConstraint().getDependentReferences();
                    }
                    rightNames = addLinkTableColumns(mf, toTable, rightNames, linkTable);

                    ArrayList<String> allKeys = new ArrayList<String>();
                    for(Column c:linkTable.getColumns()) {
                        allKeys.add(c.getName());
                    }
                    mf.addPrimaryKey("PK", allKeys, linkTable); //$NON-NLS-1$

                    // add fks for both left and right table
                    mf.addForeignKey(fromTable.getName() + "_FK", leftNames, fromTable.getName(), linkTable); //$NON-NLS-1$
                    mf.addForeignKey(toTable.getName() + "_FK", rightNames, toTable.getName(), linkTable); // //$NON-NLS-1$
                }

            } else if (isMultiplicityOne(fromEnd)) {
                addRelation(mf, fromTable, toTable, association, fromEnd.getRole());
            }
        }
    }


    private List<String> addLinkTableColumns(MetadataFactory mf, Table table, List<String> columnNames, Table linkTable)
            throws TranslatorException {
        if (columnNames != null) {
            for (String columnName:columnNames) {
                Column column = table.getColumnByName(columnName);
                if (column == null) {
                    throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17003, columnName, table.getName()));
                }
                column = mf.addColumn(column.getName(), column.getRuntimeType(), linkTable);
                column.setProperty(JOIN_COLUMN, String.valueOf(true));
            }
        }
        else {
            columnNames = new ArrayList<String>();
            for (Column column :table.getPrimaryKey().getColumns()) {
                columnNames.add(column.getName());
                if (linkTable.getColumnByName(column.getName()) == null) {
                    column = mf.addColumn(column.getName(), column.getRuntimeType(), linkTable);
                }
                column.setProperty(JOIN_COLUMN, String.valueOf(true));
            }
        }
        return columnNames;
    }

    private boolean isMultiplicityOne(EdmAssociationEnd end) {
        return end.getMultiplicity().equals(EdmMultiplicity.ONE) || end.getMultiplicity().equals(EdmMultiplicity.ZERO_TO_ONE);
    }

    private boolean isMultiplicityMany(EdmAssociationEnd end) {
        return end.getMultiplicity().equals(EdmMultiplicity.MANY);
    }

    private Table getEntityTable(MetadataFactory mf, EdmEntityType toEntity) throws TranslatorException {
        for (Table t:mf.getSchema().getTables().values()) {
            if (t.getProperty(ENTITY_TYPE, false).equals(toEntity.getFullyQualifiedTypeName())){
                return t;
            }
        }
        throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17004, toEntity.getFullyQualifiedTypeName()));
    }


    boolean same(EdmEntityType x, EdmEntityType y) {
        return (x.getFullyQualifiedTypeName().equalsIgnoreCase(y.getFullyQualifiedTypeName()));
    }

    private void addRelation(MetadataFactory mf, Table fromTable, Table toTable, EdmAssociation association, String primaryRole) {
        EdmReferentialConstraint refConstaint = association.getRefConstraint();
        if (refConstaint != null) {
            List<String> fromKeys = null;
            List<String> toKeys = null;
            if (refConstaint.getPrincipalRole().equals(primaryRole)) {
                fromKeys = refConstaint.getPrincipalReferences();
                toKeys = refConstaint.getDependentReferences();
            }
            else {
                fromKeys = refConstaint.getDependentReferences();
                toKeys = refConstaint.getPrincipalReferences();
            }
            if (matchesWithPkOrUnique(fromKeys, fromTable)) {
                mf.addForeignKey(association.getName(), toKeys, fromKeys, fromTable.getName(), toTable);
            }
            else {
                LogManager.logWarning(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17015, association.getName(), toTable.getName(), fromTable.getName()));
            }
        }
        else {
            // add the key columns from into many side
            ArrayList<String> fromKeys = new ArrayList<String>();
            for (Column column :fromTable.getPrimaryKey().getColumns()) {
                fromKeys.add(column.getName());
            }

            if (hasColumns(fromKeys, toTable)) {
                // create a FK on the columns added
                mf.addForeignKey(association.getName(), fromKeys, fromTable.getName(), toTable);
            }
            else {
                LogManager.logWarning(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17015, association.getName(), toTable.getName(), fromTable.getName()));
            }
        }
    }

    private boolean hasColumns(List<String> columnNames, Table table) {
        for (String columnName:columnNames) {
            if (table.getColumnByName(columnName) == null) {
                return false;
            }
        }
        return true;
    }

    boolean keyMatches(List<String> names, KeyRecord record) {
        if (names.size() != record.getColumns().size()) {
            return false;
        }
        Set<String> keyNames = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        for (Column c: record.getColumns()) {
            keyNames.add(c.getName());
        }
        for (int i = 0; i < names.size(); i++) {
            if (!keyNames.contains(names.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesWithPkOrUnique(List<String> names, Table table) {
        if (keyMatches(names, table.getPrimaryKey())) {
            return true;
        }

        for (KeyRecord record:table.getUniqueKeys()) {
            if (keyMatches(names, record)) {
                return true;
            }
        }
        return false;
    }

    private Column addPropertyAsColumn(MetadataFactory mf, Table table, EdmProperty ep, EdmEntitySet entitySet) {
        return addPropertyAsColumn(mf, table, ep, entitySet, null);
    }

    private Column addPropertyAsColumn(MetadataFactory mf, Table table, EdmProperty ep, EdmEntitySet entitySet, String prefix) {
        Column c = buildColumn(mf, table, ep, entitySet, prefix);
        if (ep.getFixedLength() != null) {
            c.setFixedLength(ep.getFixedLength());
        }
        if (ep.getMaxLength() != null){
            c.setLength(ep.getMaxLength());
        }
        c.setNullType(ep.isNullable()?NullType.Nullable:NullType.No_Nulls);
        if (ep.getDefaultValue() != null){
            c.setDefaultValue(ep.getDefaultValue());
        }
        // mismatch with timestamp type and odata
        if (c.getRuntimeType().equals(DataTypeManager.DefaultDataTypes.TIMESTAMP)) {
            if (ep.getPrecision() != null){
                c.setScale(ep.getPrecision());
            }
        } else {
            if (ep.getPrecision() != null){
                c.setPrecision(ep.getPrecision());
            }
            if (ep.getScale() != null){
                c.setScale(ep.getScale());
            }
        }
        return c;
    }

    @SuppressWarnings("unused")
    protected Column buildColumn(MetadataFactory mf, Table table, EdmProperty ep, EdmEntitySet entitySet, String prefix) {
        String columnName = ep.getName();
        if (prefix != null) {
            columnName = prefix+"_"+columnName; //$NON-NLS-1$
        }
        Column c = mf.addColumn(columnName, ODataTypeManager.teiidType(ep.getType().getFullyQualifiedTypeName()), table);
        c.setNameInSource(ep.getName());
        c.setUpdatable(true);
        return c;
    }

    void addFunctionImportAsProcedure(MetadataFactory mf, EdmFunctionImport function) throws TranslatorException {
        Procedure procedure = mf.addProcedure(function.getName());
        procedure.setProperty(HTTP_METHOD, function.getHttpMethod());

        // add parameters
        for (EdmFunctionParameter fp:function.getParameters()) {
            ProcedureParameter.Type type = ProcedureParameter.Type.In;
            if (fp.getMode().equals(EdmFunctionParameter.Mode.InOut)) {
                type = ProcedureParameter.Type.InOut;
            }
            else if (fp.getMode().equals(EdmFunctionParameter.Mode.Out)) {
                type = ProcedureParameter.Type.Out;
            }
            mf.addProcedureParameter(fp.getName(), ODataTypeManager.teiidType(fp.getType().getFullyQualifiedTypeName()), type, procedure);
        }

        // add return type
        EdmType returnType = function.getReturnType();
        if (returnType != null) {
            if (returnType.isSimple()) {
                mf.addProcedureParameter("return", ODataTypeManager.teiidType(((EdmSimpleType)returnType).getFullyQualifiedTypeName()), ProcedureParameter.Type.ReturnValue, procedure); //$NON-NLS-1$
            }
            else if (returnType instanceof EdmComplexType) {
                procedure.setProperty(ENTITY_TYPE, function.getReturnType().getFullyQualifiedTypeName());
                addProcedureTableReturn(mf, procedure, returnType);
            }
            else if (returnType instanceof EdmEntityType) {
                procedure.setProperty(ENTITY_TYPE, function.getReturnType().getFullyQualifiedTypeName());
                addProcedureTableReturn(mf, procedure, returnType);
            }
            else if (returnType instanceof EdmCollectionType) {
                procedure.setProperty(ENTITY_TYPE, ((EdmCollectionType)returnType).getItemType().getFullyQualifiedTypeName());
                addProcedureTableReturn(mf, procedure, ((EdmCollectionType)returnType).getItemType());
            }
            else {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17005, function.getName(), returnType.getFullyQualifiedTypeName()));
            }
        }
    }

    private void addProcedureTableReturn(MetadataFactory mf, Procedure procedure, EdmType type) throws TranslatorException {
        if (type.isSimple()) {
            mf.addProcedureResultSetColumn("return", ODataTypeManager.teiidType(((EdmSimpleType)type).getFullyQualifiedTypeName()), procedure); //$NON-NLS-1$
        }
        else if (type instanceof EdmComplexType) {
            EdmComplexType complexType = (EdmComplexType)type;
            for (EdmProperty ep:complexType.getProperties()) {
                if (ep.getType().isSimple()) {
                    mf.addProcedureResultSetColumn(ep.getName(), ODataTypeManager.teiidType(ep.getType().getFullyQualifiedTypeName()), procedure);
                }
                else {
                    addProcedureTableReturn(mf, procedure, ep.getType());
                }
            }
        }
        else if (type instanceof EdmEntityType) {
            Table table = getEntityTable(mf, (EdmEntityType)type);
            for (Column column:table.getColumns()) {
                mf.addProcedureResultSetColumn(column.getName(), column.getRuntimeType(), procedure);
            }
        }
        else {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17005, procedure.getName(), type.getFullyQualifiedTypeName()));
        }
    }

    public void setEntityContainer(String entityContainer) {
        this.entityContainer = entityContainer;
    }

    public void setSchemaNamespace(String namespace) {
        this.schemaNamespace = namespace;
    }

    List<String> getColumnNames(List<Column> columns){
        ArrayList<String> names = new ArrayList<String>();
        for (Column c:columns) {
            names.add(c.getName());
        }
        return names;
    }

    @TranslatorProperty(display="Entity Container Name", category=PropertyType.IMPORT, description="Entity Container Name to import")
    public String getEntityContainer() {
        return entityContainer;
    }

    @TranslatorProperty(display="Schema Namespace", category=PropertyType.IMPORT, description="Namespace of the schema to import")
    public String getSchemaNamespace() {
        return schemaNamespace;
    }
}
