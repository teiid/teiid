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
package org.teiid.translator.salesforce;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.util.FullyQualifiedName;

import com.sforce.soap.partner.ChildRelationship;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PicklistEntry;

public class SalesForceMetadataProcessor implements MetadataProcessor<SalesforceConnection>{
    private MetadataFactory metadataFactory;
    private SalesforceConnection connection;

    private Map<String, Table> tableMap = new LinkedHashMap<String, Table>();
    private List<Map.Entry<String, ChildRelationship[]>> relationships = new ArrayList<>();
    private boolean auditModelFields = false;
    private boolean normalizeNames = true;
    private Pattern excludeTables;
    private Pattern includeTables;
    private boolean importStatistics;
    private boolean includeExtensionMetadata = true;

    // Audit Fields
    public static final String AUDIT_FIELD_CREATED_BY_ID = "CreatedById"; //$NON-NLS-1$
    public static final String AUDIT_FIELD_CREATED_DATE = "CreatedDate"; //$NON-NLS-1$
    public static final String AUDIT_FIELD_LAST_MODIFIED_BY_ID = "LastModifiedById"; //$NON-NLS-1$
    public static final String AUDIT_FIELD_LAST_MODIFIED_DATE = "LastModifiedDate"; //$NON-NLS-1$
    public static final String AUDIT_FIELD_SYSTEM_MOD_STAMP = "SystemModstamp"; //$NON-NLS-1$

    // Model Extensions
    @ExtensionMetadataProperty(applicable= {Table.class}, datatype=Boolean.class, display="Supports Create")
    static final String TABLE_SUPPORTS_CREATE = MetadataFactory.SF_PREFIX+"Supports Create"; //$NON-NLS-1$
    @ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Delete")
    static final String TABLE_SUPPORTS_DELETE = MetadataFactory.SF_PREFIX+"Supports Delete"; //$NON-NLS-1$
    @ExtensionMetadataProperty(applicable={Table.class, Column.class}, datatype=Boolean.class, display="Custom")
    public static final String TABLE_CUSTOM = MetadataFactory.SF_PREFIX+"Custom"; //$NON-NLS-1$
    @ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports ID Lookup")
    static final String TABLE_SUPPORTS_LOOKUP = MetadataFactory.SF_PREFIX+"Supports ID Lookup"; //$NON-NLS-1$
    @ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Merge")
    static final String TABLE_SUPPORTS_MERGE = MetadataFactory.SF_PREFIX+"Supports Merge"; //$NON-NLS-1$
    @ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Query")
    public static final String TABLE_SUPPORTS_QUERY = MetadataFactory.SF_PREFIX+"Supports Query"; //$NON-NLS-1$
    @ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Replicate")
    static final String TABLE_SUPPORTS_REPLICATE = MetadataFactory.SF_PREFIX+"Supports Replicate"; //$NON-NLS-1$
    @ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Retrieve")
    public static final String TABLE_SUPPORTS_RETRIEVE = MetadataFactory.SF_PREFIX+"Supports Retrieve"; //$NON-NLS-1$
    @ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Search")
    static final String TABLE_SUPPORTS_SEARCH = MetadataFactory.SF_PREFIX+"Supports Search"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable={Column.class}, datatype=Boolean.class, display="Defaulted on Create")
    static final String COLUMN_DEFAULTED = MetadataFactory.SF_PREFIX+"Defaulted on Create"; //$NON-NLS-1$
    static final String COLUMN_CUSTOM = TABLE_CUSTOM;
    @ExtensionMetadataProperty(applicable={Column.class}, datatype=Boolean.class, display="Calculated")
    static final String COLUMN_CALCULATED = MetadataFactory.SF_PREFIX+"Calculated"; //$NON-NLS-1$
    @ExtensionMetadataProperty(applicable={Column.class}, datatype=String.class, display="Picklist Values")
    static final String COLUMN_PICKLIST_VALUES = MetadataFactory.SF_PREFIX+"Picklist Values"; //$NON-NLS-1$

    @Override
    public void process(MetadataFactory mf, SalesforceConnection connection) throws TranslatorException {
        this.connection = connection;
        this.metadataFactory = mf;

        processMetadata();

        addProcedrues(metadataFactory);
    }

    public static void addProcedrues(MetadataFactory metadataFactory) {
        Procedure p1 = metadataFactory.addProcedure("GetUpdated"); //$NON-NLS-1$
        p1.setAnnotation("Gets the updated objects"); //$NON-NLS-1$
        ProcedureParameter param = metadataFactory.addProcedureParameter("ObjectName", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p1); //$NON-NLS-1$
        param.setAnnotation("ObjectName"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("StartDate", TypeFacility.RUNTIME_NAMES.TIMESTAMP, Type.In, p1); //$NON-NLS-1$
        param.setAnnotation("Start Time"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("EndDate", TypeFacility.RUNTIME_NAMES.TIMESTAMP, Type.In, p1); //$NON-NLS-1$
        param.setAnnotation("End Time"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("LatestDateCovered", TypeFacility.RUNTIME_NAMES.TIMESTAMP, Type.In, p1); //$NON-NLS-1$
        param.setAnnotation("Latest Date Covered"); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("ID", TypeFacility.RUNTIME_NAMES.STRING, p1); //$NON-NLS-1$


        Procedure p2 = metadataFactory.addProcedure("GetDeleted"); //$NON-NLS-1$
        p2.setAnnotation("Gets the deleted objects"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("ObjectName", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p2); //$NON-NLS-1$
        param.setAnnotation("ObjectName"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("StartDate", TypeFacility.RUNTIME_NAMES.TIMESTAMP, Type.In, p2); //$NON-NLS-1$
        param.setAnnotation("Start Time"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("EndDate", TypeFacility.RUNTIME_NAMES.TIMESTAMP, Type.In, p2); //$NON-NLS-1$
        param.setAnnotation("End Time"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("EarliestDateAvailable", TypeFacility.RUNTIME_NAMES.TIMESTAMP, Type.In, p2); //$NON-NLS-1$
        param.setAnnotation("Earliest Date Available"); //$NON-NLS-1$
        param = metadataFactory.addProcedureParameter("LatestDateCovered", TypeFacility.RUNTIME_NAMES.TIMESTAMP, Type.In, p2); //$NON-NLS-1$
        param.setAnnotation("Latest Date Covered"); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("ID", TypeFacility.RUNTIME_NAMES.STRING, p2); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("DeletedDate", TypeFacility.RUNTIME_NAMES.TIMESTAMP, p2); //$NON-NLS-1$
    }

    public void processMetadata() throws TranslatorException {
        DescribeGlobalResult globalResult = connection.getObjects();
        DescribeGlobalSObjectResult[] objects = globalResult.getSobjects();
        for (DescribeGlobalSObjectResult object : objects) {
            addTable(object);
        }

        List<String> names = new ArrayList<String>();
        for (String name : this.tableMap.keySet()) {
            names.add(name);
            if (names.size() < 100) {
                continue;
            }
            getColumnsAndRelationships(names);
        }
        if (!names.isEmpty()) {
            getColumnsAndRelationships(names);
        }

        addRelationships();

        // Mark id fields are auto increment values, as they are not allowed to be updated
        for (Table table:this.metadataFactory.getSchema().getTables().values()) {
            if (importStatistics) {
                try {
                    Long val = this.connection.getCardinality(table.getSourceName());
                    if (val != null) {
                        table.setCardinality(val);
                    }
                } catch (Exception e) {
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Could not get cardinality for", table); //$NON-NLS-1$
                }
            }
            if (table.getPrimaryKey() == null) {
                continue;
            }
            for (Column column:table.getPrimaryKey().getColumns()) {
                if (!column.isUpdatable()) {
                    column.setAutoIncremented(true);
                }
            }
        }
    }

    private void getColumnsAndRelationships(List<String> names)
            throws TranslatorException {
        DescribeSObjectResult objectMetadatas[] = connection.getObjectMetaData(names.toArray(new String[names.size()]));
        for (DescribeSObjectResult objectMetadata : objectMetadatas) {
            getRelationships(objectMetadata);
            Table table = this.tableMap.get(objectMetadata.getName());
            boolean hasUpdateableColumn = addColumns(objectMetadata, table);
            // Some SF objects return true for isUpdateable() but have no updateable columns.
            if(objectMetadata.isDeletable() || (hasUpdateableColumn && (objectMetadata.isUpdateable() || objectMetadata.isCreateable()))) {
                table.setSupportsUpdate(true);
            }
        }
        names.clear();
    }

    private void addRelationships() {
        for (Map.Entry<String, ChildRelationship[]> entry : this.relationships) {
            for (ChildRelationship relationship : entry.getValue()) {
                if (relationship.getRelationshipName() == null) {
                    continue; //not queryable
                }
                if (!isModelAuditFields() && isAuditField(relationship.getField())) {
                    continue;
                }

                Table parent = tableMap.get(entry.getKey());
                KeyRecord pk = parent.getPrimaryKey();
                if (null == pk) {
                    throw new RuntimeException("ERROR !!primary key column not found!!"); //$NON-NLS-1$
                }

                Table child = tableMap.get(relationship.getChildSObject());

                if (child == null) {
                    continue; //child must have been excluded
                }

                Column col = null;
                List<Column> columns = child.getColumns();
                for (Iterator<Column> colIter = columns.iterator(); colIter.hasNext();) {
                    Column column = colIter.next();
                    if(column.getSourceName().equals(relationship.getField())) {
                        col = column;
                    }
                }
                if (null == col)
                 {
                    throw new RuntimeException(
                            "ERROR !!foreign key column not found!! " + child.getName() + relationship.getField()); //$NON-NLS-1$
                }


                String name = "FK_" + parent.getName() + "_" + col.getName();//$NON-NLS-1$ //$NON-NLS-2$
                ArrayList<String> columnNames = new ArrayList<String>();
                columnNames.add(col.getName());
                ForeignKey fk = metadataFactory.addForeignKey(name, columnNames, parent.getName(), child);
                fk.setNameInSource(relationship.getRelationshipName());
                if (isAuditField(relationship.getField())) {
                    fk.setProperty(ForeignKey.ALLOW_JOIN, "INNER"); //$NON-NLS-1$
                }
            }
        }
    }

    public static boolean isAuditField(String name) {
        boolean result = false;
        if(name.equals(AUDIT_FIELD_CREATED_BY_ID) ||
                name.equals(AUDIT_FIELD_CREATED_DATE) ||
                name.equals(AUDIT_FIELD_LAST_MODIFIED_BY_ID) ||
                name.equals(AUDIT_FIELD_LAST_MODIFIED_DATE) ||
                name.equals(AUDIT_FIELD_SYSTEM_MOD_STAMP)) {
            result = true;
        }
        return result;
    }

    private void addTable(DescribeGlobalSObjectResult objectMetadata) {
        String name = objectMetadata.getName();
        if (normalizeNames) {
            name = NameUtil.normalizeName(name);
        }
        if (!allowedToAdd(name)) {
            return;
        }
        Table table = metadataFactory.addTable(name);
        FullyQualifiedName fqn = new FullyQualifiedName("sobject", objectMetadata.getName()); //$NON-NLS-1$
        table.setProperty(FQN, fqn.toString());
        if (!table.getName().equals(objectMetadata.getName())) {
            table.setNameInSource(objectMetadata.getName());
        }
        tableMap.put(objectMetadata.getName(), table);

        table.setProperty(TABLE_CUSTOM, String.valueOf(objectMetadata.isCustom()));
        table.setProperty(TABLE_SUPPORTS_QUERY, String.valueOf(objectMetadata.isQueryable()));
        table.setProperty(TABLE_SUPPORTS_RETRIEVE, String.valueOf(objectMetadata.isRetrieveable()));

        if (isIncludeExtensionMetadata()) {
            table.setNameInSource(objectMetadata.getName());
            //metadata that is not currently used in execution
            table.setProperty(TABLE_SUPPORTS_CREATE, String.valueOf(objectMetadata.isCreateable()));
            table.setProperty(TABLE_SUPPORTS_DELETE, String.valueOf(objectMetadata.isDeletable()));
            table.setProperty(TABLE_SUPPORTS_MERGE, String.valueOf(objectMetadata.isMergeable()));
            table.setProperty(TABLE_SUPPORTS_SEARCH, String.valueOf(objectMetadata.isSearchable()));
            table.setProperty(TABLE_SUPPORTS_REPLICATE, String.valueOf(objectMetadata.isReplicateable()));
        }
    }

    boolean allowedToAdd(String name) {
        if (!shouldInclude(name)) {
            return false;
        }

        if (shouldExclude(name)) {
            return false;
        }
        return true;
    }

    private void getRelationships(DescribeSObjectResult objectMetadata) {
        ChildRelationship[] children = objectMetadata.getChildRelationships();
        if(children != null && children.length > 0) {
            this.relationships.add(new AbstractMap.SimpleEntry<String, ChildRelationship[]>(objectMetadata.getName(), children));
        }
    }

    private boolean addColumns(DescribeSObjectResult objectMetadata, Table table) {
        boolean hasUpdateableColumn = false;
        Field[] fields = objectMetadata.getFields();
        for (Field field : fields) {
            String normalizedName = field.getName();
            if (normalizeNames) {
                normalizedName = NameUtil.normalizeName(normalizedName);
            }
            FieldType fieldType = field.getType();
            if(isAuditField(field.getName())) {
                if (!isModelAuditFields()) {
                    continue;
                }
                if (field.getName().equals(AUDIT_FIELD_CREATED_BY_ID) || field.getName().equals(AUDIT_FIELD_LAST_MODIFIED_BY_ID)) {
                    ChildRelationship relationship = new ChildRelationship();
                    relationship.setChildSObject(objectMetadata.getName());
                    relationship.setField(field.getName());
                    relationship.setRelationshipName(objectMetadata.getName()+"s"); //$NON-NLS-1$
                    this.relationships.add(new AbstractMap.SimpleEntry<String, ChildRelationship[]>("User", new ChildRelationship[]{relationship})); //$NON-NLS-1$
                }
            }
            String sfTypeName = fieldType.name();
            Column column = null;
            switch (fieldType) {
            case string:
            case combobox:
            case reference:
            case phone:
            case id:
            case url:
            case email:
            case encryptedstring:
            case anyType:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
                column.setNativeType(sfTypeName);
                if(sfTypeName.equals(FieldType.id.name())) {
                    column.setNullType(NullType.No_Nulls);
                    ArrayList<String> columnNames = new ArrayList<String>();
                    columnNames.add(field.getName());
                    metadataFactory.addPrimaryKey(field.getName()+"_PK", columnNames, table); //$NON-NLS-1$
                }
                break;
            case picklist:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
                if(field.isRestrictedPicklist()) {
                    column.setNativeType("restrictedpicklist"); //$NON-NLS-1$
                } else {
                    column.setNativeType(sfTypeName);
                }

                column.setProperty(COLUMN_PICKLIST_VALUES, getPicklistValues(field));
                break;
            case multipicklist:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
                if(field.isRestrictedPicklist()) {
                    column.setNativeType("restrictedmultiselectpicklist");//$NON-NLS-1$
                } else {
                    column.setNativeType(sfTypeName);
                }
                column.setProperty(COLUMN_PICKLIST_VALUES, getPicklistValues(field));
                break;
            case base64:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.BLOB, table);
                column.setNativeType(sfTypeName);
                break;
            case _boolean:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.BOOLEAN, table);
                column.setNativeType(sfTypeName);
                break;
            case currency:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DOUBLE, table);
                column.setNativeType(sfTypeName);
                column.setCurrency(true);
                column.setScale(field.getScale());
                column.setPrecision(field.getPrecision());
                break;
            case textarea:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
                column.setNativeType(sfTypeName);
                break;
            case _int:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.INTEGER, table);
                column.setNativeType(sfTypeName);
                column.setPrecision(field.getPrecision());
                break;
            case _double:
            case percent:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DOUBLE, table);
                column.setNativeType(sfTypeName);
                column.setScale(field.getScale());
                column.setPrecision(field.getPrecision());
                break;
            case date:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DATE, table);
                column.setNativeType(sfTypeName);
                break;
            case datetime:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.TIMESTAMP, table);
                column.setNativeType(sfTypeName);
                break;
            case time:
                column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.TIME, table);
                column.setNativeType(sfTypeName);
                break;
            default:
                if (sfTypeName.equals("address")) { //$NON-NLS-1$
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Ignoring composite address field", normalizedName); //$NON-NLS-1$
                } else {
                    LogManager.logWarning(LogConstants.CTX_CONNECTOR, SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13001, sfTypeName));
                }
                continue;
            }

            if (!field.getFilterable()) {
                column.setSearchType(SearchType.Unsearchable);
                //corner case for teiid, it may be sortable
            } else if (!field.getSortable()) {
                column.setSearchType(SearchType.Equality_Only);
            }

            if (!column.getName().equals(field.getName())) {
                column.setNameInSource(field.getName());
            }

            column.setLength(field.getLength());
            if(field.isUpdateable() || field.isCreateable()) {
                column.setUpdatable(true);
                hasUpdateableColumn  = true;
            }
            if (this.isIncludeExtensionMetadata()) {
                column.setNameInSource(field.getName());
                column.setProperty(COLUMN_CALCULATED, String.valueOf(field.isCalculated()));
                column.setProperty(COLUMN_CUSTOM, String.valueOf(field.isCustom()));
                column.setProperty(COLUMN_DEFAULTED, String.valueOf(field.isDefaultedOnCreate()));
            }
            if (field.isDefaultedOnCreate()) {
                column.setDefaultValue("sf default"); //$NON-NLS-1$
            }
            column.setNullType(field.isNillable()?NullType.Nullable:NullType.No_Nulls);
        }
        return hasUpdateableColumn;
    }

    private String getPicklistValues(Field field) {
        StringBuffer picklistValues = new StringBuffer();
        if(null != field.getPicklistValues() && field.getPicklistValues().length > 0) {
            PicklistEntry[] entries = field.getPicklistValues();
            boolean first = true;
            for (PicklistEntry entry : entries) {
                if (!first) {
                    picklistValues.append(',');
                }
                first = false;
                picklistValues.append(entry.getValue());
            }
        }
        return picklistValues.toString();
    }

    @TranslatorProperty(display="Model Audit Fields", category=PropertyType.IMPORT, description="Determines if the salesforce audit fields are modeled")
    public boolean isModelAuditFields() {
        return this.auditModelFields;
    }

    public void setModelAuditFields(boolean modelAuditFields) {
        this.auditModelFields = modelAuditFields;
    }

    @TranslatorProperty(display="Normalize Names", category=PropertyType.IMPORT, description="Normalize the object/field names to not need quoting")
    public boolean isNormalizeNames() {
        return normalizeNames;
    }

    public void setNormalizeNames(boolean normalizeNames) {
        this.normalizeNames = normalizeNames;
    }

    public void setExcludeTables(String excludeTables) {
        this.excludeTables = Pattern.compile(excludeTables, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    }

    @TranslatorProperty(display="Exclude Tables", category=PropertyType.IMPORT, description="A case-insensitive regular expression that when matched against a fully qualified Teiid table name will exclude it from import.  Applied after table names are retrieved.  Use a negative look-ahead (?!<inclusion pattern>).* to act as an inclusion filter")
    public String getExcludeTables() {
        return this.excludeTables.pattern();
    }

    protected boolean shouldExclude(String fullName) {
        if (this.excludeTables == null) {
            return false;
        }
        return excludeTables != null && excludeTables.matcher(fullName).matches();
    }

    public void setIncludeTables(String excludeTables) {
        this.includeTables = Pattern.compile(excludeTables, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    }

    @TranslatorProperty(display="Include Tables", category=PropertyType.IMPORT, description="A case-insensitive regular expression that when matched against a fully qualified Teiid table name will included in import.  Applied after table names are retrieved.")
    public String getIncludeTables() {
        return this.includeTables.pattern();
    }

    protected boolean shouldInclude(String fullName) {
        if (includeTables == null) {
            return true;
        }
        return includeTables != null && includeTables.matcher(fullName).matches();
    }

    @TranslatorProperty(display="Import Statistics", category=PropertyType.IMPORT, description="Set to true to retrieve cardinalities during import.")
    public boolean isImportStatistics() {
        return importStatistics;
    }

    public void setImportStatistics(boolean importStatistics) {
        this.importStatistics = importStatistics;
    }

    @TranslatorProperty(display="Add Extension Metadata", category=PropertyType.IMPORT, description="Set to true to add additional extension metadata during the import")
    public boolean isIncludeExtensionMetadata() {
        return this.includeExtensionMetadata;
    }

    public void setIncludeExtensionMetadata(boolean flag) {
        this.includeExtensionMetadata = flag;
    }
}
