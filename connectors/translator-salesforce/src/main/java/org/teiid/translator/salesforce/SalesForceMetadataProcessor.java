package org.teiid.translator.salesforce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;

import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.*;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;

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
	
	private Map<String, Table> tableMap = new HashMap<String, Table>();
	private Map<String, ChildRelationship[]> relationships = new LinkedHashMap<String, ChildRelationship[]>();
	private List<Column> columns;
	private boolean auditModelFields = false;
	private boolean normalizeNames = true;

	// Audit Fields
	public static final String AUDIT_FIELD_CREATED_BY_ID = "CreatedById"; //$NON-NLS-1$
	public static final String AUDIT_FIELD_CREATED_DATE = "CreatedDate"; //$NON-NLS-1$
	public static final String AUDIT_FIELD_LAST_MODIFIED_BY_ID = "LastModifiedById"; //$NON-NLS-1$
	public static final String AUDIT_FIELD_LAST_MODIFIED_DATE = "LastModifiedDate"; //$NON-NLS-1$
	public static final String AUDIT_FIELD_SYSTEM_MOD_STAMP = "SystemModstamp"; //$NON-NLS-1$

	// Model Extensions
	@ExtensionMetadataProperty(applicable= {Table.class}, datatype=Boolean.class, display="Supports Create")
	static final String TABLE_SUPPORTS_CREATE = MetadataFactory.SF_URI+"Supports Create"; //$NON-NLS-1$
	@ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Delete")
	static final String TABLE_SUPPORTS_DELETE = MetadataFactory.SF_URI+"Supports Delete"; //$NON-NLS-1$
	@ExtensionMetadataProperty(applicable={Table.class, Column.class}, datatype=Boolean.class, display="Custom")
	static final String TABLE_CUSTOM = MetadataFactory.SF_URI+"Custom"; //$NON-NLS-1$
	@ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports ID Lookup")
	static final String TABLE_SUPPORTS_LOOKUP = MetadataFactory.SF_URI+"Supports ID Lookup"; //$NON-NLS-1$
	@ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Merge")
	static final String TABLE_SUPPORTS_MERGE = MetadataFactory.SF_URI+"Supports Merge"; //$NON-NLS-1$
	@ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Query")
	static final String TABLE_SUPPORTS_QUERY = MetadataFactory.SF_URI+"Supports Query"; //$NON-NLS-1$
	@ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Replicate")
	static final String TABLE_SUPPORTS_REPLICATE = MetadataFactory.SF_URI+"Supports Replicate"; //$NON-NLS-1$
	@ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Retrieve")
	static final String TABLE_SUPPORTS_RETRIEVE = MetadataFactory.SF_URI+"Supports Retrieve"; //$NON-NLS-1$
	@ExtensionMetadataProperty(applicable={Table.class}, datatype=Boolean.class, display="Supports Search")
	static final String TABLE_SUPPORTS_SEARCH = MetadataFactory.SF_URI+"Supports Search"; //$NON-NLS-1$
	
	@ExtensionMetadataProperty(applicable={Column.class}, datatype=Boolean.class, display="Defaulted on Create")
	static final String COLUMN_DEFAULTED = MetadataFactory.SF_URI+"Defaulted on Create"; //$NON-NLS-1$
	static final String COLUMN_CUSTOM = TABLE_CUSTOM;
	@ExtensionMetadataProperty(applicable={Column.class}, datatype=Boolean.class, display="Calculated")
	static final String COLUMN_CALCULATED = MetadataFactory.SF_URI+"calculated"; //$NON-NLS-1$
	@ExtensionMetadataProperty(applicable={Column.class}, datatype=String.class, display="Picklist Values")
	static final String COLUMN_PICKLIST_VALUES = MetadataFactory.SF_URI+"Picklist Values"; //$NON-NLS-1$
	
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
		try {
			DescribeGlobalResult globalResult = connection.getObjects();
			DescribeGlobalSObjectResult[] objects = globalResult.getSobjects();
			for (DescribeGlobalSObjectResult object : objects) {
				addTable(object);
			}  
			addRelationships();
			
			// Mark id fields are auto increment values, as they are not allowed to be updated
			for (Table table:this.metadataFactory.getSchema().getTables().values()) {
				for (Column column:table.getPrimaryKey().getColumns()) {
					if (!column.isUpdatable()) {
						column.setAutoIncremented(true);
					}
				}
			}
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
	}

	private void addRelationships() {
		for (Map.Entry<String, ChildRelationship[]> entry : this.relationships.entrySet()) {
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
				
				Column col = null;
				columns = child.getColumns();
				for (Iterator<Column> colIter = columns.iterator(); colIter.hasNext();) {
					Column column = colIter.next();
					if(column.getNameInSource().equals(relationship.getField())) {
						col = column;
					}
				}
				if (null == col) throw new RuntimeException(
	                    "ERROR !!foreign key column not found!! " + child.getName() + relationship.getField()); //$NON-NLS-1$
	
				
				String name = "FK_" + parent.getName() + "_" + col.getName();//$NON-NLS-1$ //$NON-NLS-2$
				ArrayList<String> columnNames = new ArrayList<String>();
				columnNames.add(col.getName());	
				ForeignKey fk = metadataFactory.addForiegnKey(name, columnNames, parent.getName(), child);
				fk.setNameInSource(relationship.getRelationshipName()); //TODO: only needed for custom relationships
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

	private void addTable(DescribeGlobalSObjectResult object) throws TranslatorException {
		DescribeSObjectResult objectMetadata = null;
		try {
			objectMetadata = connection.getObjectMetaData(object.getName());
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
		
		String name = objectMetadata.getName();
		if (normalizeNames) {
			name = NameUtil.normalizeName(name);
		}
		Table table = metadataFactory.addTable(name);
		
		table.setNameInSource(objectMetadata.getName());
		tableMap.put(objectMetadata.getName(), table);
		getRelationships(objectMetadata);

		table.setProperty(TABLE_CUSTOM, String.valueOf(objectMetadata.isCustom()));
		table.setProperty(TABLE_SUPPORTS_CREATE, String.valueOf(objectMetadata.isCreateable()));
		table.setProperty(TABLE_SUPPORTS_DELETE, String.valueOf(objectMetadata.isDeletable()));
		table.setProperty(TABLE_SUPPORTS_MERGE, String.valueOf(objectMetadata.isMergeable()));
		table.setProperty(TABLE_SUPPORTS_QUERY, String.valueOf(objectMetadata.isQueryable()));
		table.setProperty(TABLE_SUPPORTS_REPLICATE, String.valueOf(objectMetadata.isReplicateable()));
		table.setProperty(TABLE_SUPPORTS_RETRIEVE, String.valueOf(objectMetadata.isRetrieveable()));
		table.setProperty(TABLE_SUPPORTS_SEARCH, String.valueOf(objectMetadata.isSearchable()));

		boolean hasUpdateableColumn = addColumns(objectMetadata, table);
		
		// Some SF objects return true for isUpdateable() but have no updateable columns.
		if(hasUpdateableColumn && objectMetadata.isUpdateable()) {
			table.setSupportsUpdate(true);
		}
	}

	private void getRelationships(DescribeSObjectResult objectMetadata) {
		ChildRelationship[] children = objectMetadata.getChildRelationships();
		if(children != null && children.length > 0) {
			this.relationships.put(objectMetadata.getName(), children);
		}
	}

	private boolean addColumns(DescribeSObjectResult objectMetadata, Table table) throws TranslatorException {
		boolean hasUpdateableColumn = false;
		Field[] fields = objectMetadata.getFields();
		for (Field field : fields) {
			String normalizedName = field.getName();
			if (normalizeNames) {
				normalizedName = NameUtil.normalizeName(normalizedName);
			}
			FieldType fieldType = field.getType();
			if(!isModelAuditFields() && isAuditField(field.getName())) {
				continue;
			}
			String sfTypeName = fieldType.name();
			Column column = null;
			if(sfTypeName.equals(FieldType.string.name()) || //string
					sfTypeName.equals(FieldType.combobox.name()) || //"combobox"
					sfTypeName.equals(FieldType.reference.name()) || //"reference"
					sfTypeName.equals(FieldType.phone.name()) || //"phone"
					sfTypeName.equals(FieldType.id.name()) || //"id"
					sfTypeName.equals(FieldType.url.name()) || //"url"
					sfTypeName.equals(FieldType.email.name()) || //"email"
					sfTypeName.equals(FieldType.encryptedstring.name()) || //"encryptedstring"
					sfTypeName.equals(FieldType.anyType.name())) {  //"anytype"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				column.setNativeType(sfTypeName);
				if(sfTypeName.equals(FieldType.id.name())) {
					column.setNullType(NullType.No_Nulls);
					ArrayList<String> columnNames = new ArrayList<String>();
					columnNames.add(field.getName());
					metadataFactory.addPrimaryKey(field.getName()+"_PK", columnNames, table); //$NON-NLS-1$
				}
			}
			else if(sfTypeName.equals(FieldType.picklist.name())) { // "picklist"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				if(field.isRestrictedPicklist()) {
					column.setNativeType("restrictedpicklist"); //$NON-NLS-1$
				} else {
					column.setNativeType(sfTypeName);
				}
				
				column.setProperty(COLUMN_PICKLIST_VALUES, getPicklistValues(field));
			}
			else if(sfTypeName.equals(FieldType.multipicklist.name())) { //"multipicklist"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				if(field.isRestrictedPicklist()) {
					column.setNativeType("restrictedmultiselectpicklist");//$NON-NLS-1$
				} else {
					column.setNativeType(sfTypeName);
				}
				column.setProperty(COLUMN_PICKLIST_VALUES, getPicklistValues(field));
			}
			else if(sfTypeName.equals(FieldType.base64.name())) { //"base64"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.BLOB, table);
				column.setNativeType(sfTypeName);
			}
			else if(sfTypeName.equals(FieldType._boolean.name())) { //"boolean"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.BOOLEAN, table);
				column.setNativeType(sfTypeName);
			}
			else if(sfTypeName.equals(FieldType.currency.name())) { //"currency"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DOUBLE, table);
				column.setNativeType(sfTypeName);
				column.setCurrency(true);
				column.setScale(field.getScale());
				column.setPrecision(field.getPrecision());
			}
			else if(sfTypeName.equals(FieldType.textarea.name())) { //"textarea"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				column.setNativeType(sfTypeName);
				column.setSearchType(SearchType.Unsearchable);
			}
			else if(sfTypeName.equals(FieldType._int.name())) { //"int"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.INTEGER, table);
				column.setNativeType(sfTypeName);
				column.setPrecision(field.getPrecision());
			}
			else if(sfTypeName.equals(FieldType._double.name()) || //"double"
					sfTypeName.equals(FieldType.percent.name())) { //"percent"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DOUBLE, table);
				column.setNativeType(sfTypeName);
				column.setScale(field.getScale());
				column.setPrecision(field.getPrecision());
			}
			else if(sfTypeName.equals(FieldType.date.name())) { //"date"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DATE, table);
				column.setNativeType(sfTypeName);
			}
			else if(sfTypeName.equals(FieldType.datetime.name())) { //"datetime"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.TIMESTAMP, table);
				column.setNativeType(sfTypeName);
			}
			else if(sfTypeName.equals(FieldType.time.name())) { //"time"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.TIME, table);
				column.setNativeType(sfTypeName);
			}
			
			if(column == null) {
				LogManager.logError(LogConstants.CTX_CONNECTOR, SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13001, sfTypeName));
				continue;
			} 
			
			column.setNameInSource(field.getName());
			column.setLength(field.getLength());
			if(field.isUpdateable()) {
				column.setUpdatable(true);
				hasUpdateableColumn  = true;
			}
			column.setProperty(COLUMN_CALCULATED, String.valueOf(field.isCalculated()));
			column.setProperty(COLUMN_CUSTOM, String.valueOf(field.isCustom()));
			column.setProperty(COLUMN_DEFAULTED, String.valueOf(field.isDefaultedOnCreate()));
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
	
    @TranslatorProperty(display="Audit Model Fields", category=PropertyType.IMPORT, description="Audit Model Fields")
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
}
