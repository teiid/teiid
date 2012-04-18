package org.teiid.translator.salesforce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;

import org.teiid.core.types.DataTypeManager;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column.SearchType;
import org.teiid.translator.TranslatorException;

import com.sforce.soap.partner.ChildRelationship;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PicklistEntry;

public class MetadataProcessor {
	private MetadataFactory metadataFactory;
	private SalesforceConnection connection;
	private SalesForceExecutionFactory connectorEnv;
	
	private Map<String, Table> tableMap = new HashMap<String, Table>();
	private List<Relationship> relationships = new ArrayList<Relationship>();
	private boolean hasUpdateableColumn = false;
	private List<Column> columns;

	// Audit Fields
	public static final String AUDIT_FIELD_CREATED_BY_ID = "CreatedById"; //$NON-NLS-1$
	public static final String AUDIT_FIELD_CREATED_DATE = "CreatedDate"; //$NON-NLS-1$
	public static final String AUDIT_FIELD_LAST_MODIFIED_BY_ID = "LastModifiedById"; //$NON-NLS-1$
	public static final String AUDIT_FIELD_LAST_MODIFIED_DATE = "LastModifiedDate"; //$NON-NLS-1$
	public static final String AUDIT_FIELD_SYSTEM_MOD_STAMP = "SystemModstamp"; //$NON-NLS-1$

	// Model Extensions
	static final String TABLE_SUPPORTS_CREATE = "Supports Create"; //$NON-NLS-1$
	static final String TABLE_SUPPORTS_DELETE = "Supports Delete"; //$NON-NLS-1$
	static final String TABLE_CUSTOM = "Custom"; //$NON-NLS-1$
	static final String TABLE_SUPPORTS_LOOKUP = "Supports ID Lookup"; //$NON-NLS-1$
	static final String TABLE_SUPPORTS_MERGE = "Supports Merge"; //$NON-NLS-1$
	static final String TABLE_SUPPORTS_QUERY = "Supports Query"; //$NON-NLS-1$
	static final String TABLE_SUPPORTS_REPLICATE = "Supports Replicate"; //$NON-NLS-1$
	static final String TABLE_SUPPORTS_RETRIEVE = "Supports Retrieve"; //$NON-NLS-1$
	static final String TABLE_SUPPORTS_SEARCH = "Supports Search"; //$NON-NLS-1$
	
	static final String COLUMN_DEFAULTED = "Defaulted on Create"; //$NON-NLS-1$
	static final String COLUMN_CUSTOM = "Custom"; //$NON-NLS-1$
	static final String COLUMN_CALCULATED = "Calculated"; //$NON-NLS-1$
	static final String COLUMN_PICKLIST_VALUES = "Picklist Values"; //$NON-NLS-1$
	
	public MetadataProcessor(SalesforceConnection connection, MetadataFactory metadataFactory, SalesForceExecutionFactory env) {
		this.connection = connection;
		this.metadataFactory = metadataFactory;
		this.connectorEnv = env;
	}

	public void processMetadata() throws TranslatorException {
		try {
			DescribeGlobalResult globalResult = connection.getObjects();
			List<DescribeGlobalSObjectResult> objects = globalResult.getSobjects();
			for (DescribeGlobalSObjectResult object : objects) {
				addTable(object);
			}  
			addRelationships();
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
	}

	private void addRelationships() throws TranslatorException {
		for (Iterator<Relationship> iterator = relationships.iterator(); iterator.hasNext();) {
			Relationship relationship = iterator.next();
			if (!this.connectorEnv.isModelAuditFields() && isAuditField(relationship.getForeignKeyField())) {
                continue;
            }

			Table parent = tableMap.get(NameUtil.normalizeName(relationship.getParentTable()));
			KeyRecord pk = parent.getPrimaryKey();
			if (null == pk) {
                throw new RuntimeException("ERROR !!primary key column not found!!"); //$NON-NLS-1$
            }
			ArrayList<String> columnNames = new ArrayList<String>();
			columnNames.add(pk.getName());
			
			
			Table child = tableMap.get(NameUtil.normalizeName(relationship.getChildTable()));
			
			Column col = null;
			columns = child.getColumns();
			for (Iterator colIter = columns.iterator(); colIter.hasNext();) {
				Column column = (Column) colIter.next();
				if(column.getName().equals(relationship.getForeignKeyField())) {
					col = column;
				}
			}
			if (null == col) throw new RuntimeException(
                    "ERROR !!foreign key column not found!! " + child.getName() + relationship.getForeignKeyField()); //$NON-NLS-1$

			
			String columnName = "FK_" + parent.getName() + "_" + col.getName();//$NON-NLS-1$ //$NON-NLS-2$
			ArrayList<String> columnNames2 = new ArrayList<String>();
			columnNames2.add(col.getName());	
			metadataFactory.addForiegnKey(columnName, columnNames2, parent, child);
	        
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
		
		String name = NameUtil.normalizeName(objectMetadata.getName());
		Table table = metadataFactory.addTable(name);
		
		table.setNameInSource(objectMetadata.getName());
		tableMap.put(name, table);
		getRelationships(objectMetadata);

		table.setProperty(TABLE_CUSTOM, String.valueOf(objectMetadata.isCustom()));
		table.setProperty(TABLE_SUPPORTS_CREATE, String.valueOf(objectMetadata.isCreateable()));
		table.setProperty(TABLE_SUPPORTS_DELETE, String.valueOf(objectMetadata.isDeletable()));
		table.setProperty(TABLE_SUPPORTS_MERGE, String.valueOf(objectMetadata.isMergeable()));
		table.setProperty(TABLE_SUPPORTS_QUERY, String.valueOf(objectMetadata.isQueryable()));
		table.setProperty(TABLE_SUPPORTS_REPLICATE, String.valueOf(objectMetadata.isReplicateable()));
		table.setProperty(TABLE_SUPPORTS_RETRIEVE, String.valueOf(objectMetadata.isRetrieveable()));
		table.setProperty(TABLE_SUPPORTS_SEARCH, String.valueOf(objectMetadata.isSearchable()));

		hasUpdateableColumn = false;
		addColumns(objectMetadata, table);
		
		// Some SF objects return true for isUpdateable() but have no updateable columns.
		if(hasUpdateableColumn && objectMetadata.isUpdateable()) {
			table.setSupportsUpdate(true);
		}
	}

	private void getRelationships(DescribeSObjectResult objectMetadata) {
		List<ChildRelationship> children = objectMetadata.getChildRelationships();
		if(children != null && children.size() != 0) {
			for (ChildRelationship childRelation : children) {
				Relationship newRelation = new RelationshipImpl();
				newRelation.setParentTable(objectMetadata.getName());
				newRelation.setChildTable(childRelation.getChildSObject());
				newRelation.setForeignKeyField(childRelation.getField());
				newRelation.setCascadeDelete(childRelation.isCascadeDelete());
				relationships.add(newRelation);
			}
		}
	}

	private void addColumns(DescribeSObjectResult objectMetadata, Table table) throws TranslatorException {
		List<Field> fields = objectMetadata.getFields();
		for (Field field : fields) {
			String normalizedName = NameUtil.normalizeName(field.getName());
			FieldType fieldType = field.getType();
			if(!this.connectorEnv.isModelAuditFields() && isAuditField(field.getName())) {
				continue;
			}
			String sfTypeName = fieldType.value();
			Column column = null;
			if(sfTypeName.equals(FieldType.STRING.value()) || //string
					sfTypeName.equals(FieldType.COMBOBOX.value()) || //"combobox"
					sfTypeName.equals(FieldType.REFERENCE.value()) || //"reference"
					sfTypeName.equals(FieldType.PHONE.value()) || //"phone"
					sfTypeName.equals(FieldType.ID.value()) || //"id"
					sfTypeName.equals(FieldType.URL.value()) || //"url"
					sfTypeName.equals(FieldType.EMAIL.value()) || //"email"
					sfTypeName.equals(FieldType.ENCRYPTEDSTRING.value()) || //"encryptedstring"
					sfTypeName.equals(FieldType.ANY_TYPE.value())) {  //"anytype"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				column.setNativeType(sfTypeName);
				if(sfTypeName.equals(FieldType.ID.value())) {
					column.setNullType(NullType.No_Nulls);
					ArrayList<String> columnNames = new ArrayList<String>();
					columnNames.add(field.getName());
					metadataFactory.addPrimaryKey(field.getName()+"_PK", columnNames, table); //$NON-NLS-1$
				}
			}
			else if(sfTypeName.equals(FieldType.PICKLIST.value())) { // "picklist"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				if(field.isRestrictedPicklist()) {
					column.setNativeType("restrictedpicklist"); //$NON-NLS-1$
				} else {
					column.setNativeType(sfTypeName);
				}
				
				column.setProperty(COLUMN_PICKLIST_VALUES, getPicklistValues(field));
			}
			else if(sfTypeName.equals(FieldType.MULTIPICKLIST)) { //"multipicklist"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				if(field.isRestrictedPicklist()) {
					column.setNativeType("restrictedmultiselectpicklist");//$NON-NLS-1$
				} else {
					column.setNativeType(sfTypeName);
				}
				column.setProperty(COLUMN_PICKLIST_VALUES, getPicklistValues(field));
			}
			else if(sfTypeName.equals(FieldType.BASE_64.value())) { //"base64"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.BLOB, table);
				column.setNativeType(sfTypeName);
			}
			else if(sfTypeName.equals(FieldType.BOOLEAN.value())) { //"boolean"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.BOOLEAN, table);
				column.setNativeType(sfTypeName);
			}
			else if(sfTypeName.equals(FieldType.CURRENCY.value())) { //"currency"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DOUBLE, table);
				column.setNativeType(sfTypeName);
				column.setCurrency(true);
				column.setScale(field.getScale());
				column.setPrecision(field.getPrecision());
			}
			else if(sfTypeName.equals(FieldType.TEXTAREA.value())) { //"textarea"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				column.setNativeType(sfTypeName);
				column.setSearchType(SearchType.Unsearchable);
			}
			else if(sfTypeName.equals(FieldType.INT.value())) { //"int"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.INTEGER, table);
				column.setNativeType(sfTypeName);
				column.setPrecision(field.getPrecision());
			}
			else if(sfTypeName.equals(FieldType.DOUBLE.value()) || //"double"
					sfTypeName.equals(FieldType.PERCENT.value())) { //"percent"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DOUBLE, table);
				column.setNativeType(sfTypeName);
				column.setScale(field.getScale());
				column.setPrecision(field.getPrecision());
			}
			else if(sfTypeName.equals(FieldType.DATE.value())) { //"date"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DATE, table);
				column.setNativeType(sfTypeName);
			}
			else if(sfTypeName.equals(FieldType.DATETIME.value())) { //"datetime"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.TIMESTAMP, table);
				column.setNativeType(sfTypeName);
			}
			else if(sfTypeName.equals(FieldType.TIME.value())) { //"time"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.TIME, table);
				column.setNativeType(sfTypeName);
			}
			
			if(column == null) {
				LogManager.logError(LogConstants.CTX_CONNECTOR, "Unknown type returned by SalesForce: " + sfTypeName);
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
	}
	
	private String getPicklistValues(Field field) {
		StringBuffer picklistValues = new StringBuffer();
		if(null != field.getPicklistValues() && 0 != field.getPicklistValues().size()) {
			List<PicklistEntry> entries = field.getPicklistValues();
			for (Iterator<PicklistEntry> iterator = entries.iterator(); iterator.hasNext();) {
				PicklistEntry entry = iterator.next();
				picklistValues.append(entry.getValue());
				if(iterator.hasNext()) {
					picklistValues.append(',');
				}
			}
		}
		return picklistValues.toString();
	}
}
