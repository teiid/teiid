package com.metamatrix.connector.salesforce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.KeyRecord;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.Table;
import org.teiid.connector.metadata.runtime.BaseColumn.NullType;
import org.teiid.connector.metadata.runtime.Column.SearchType;
import org.teiid.connector.visitor.util.SQLReservedWords;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.connector.salesforce.connection.SalesforceConnection;
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
	private ConnectorLogger logger;
	
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
	
	public MetadataProcessor(SalesforceConnection connection, MetadataFactory metadataFactory, ConnectorLogger logger) {
		this.connection = connection;
		this.metadataFactory = metadataFactory;
		this.logger = logger;
	}

	public void processMetadata() throws ConnectorException {
		DescribeGlobalResult globalResult = connection.getObjects();
		DescribeGlobalSObjectResult[] objects = globalResult.getSobjects();
		for (int i=0;i < objects.length;i++) {
			DescribeGlobalSObjectResult object = objects[i];
			addTable(object);
		}
		addRelationships();
	}

	private void addRelationships() throws ConnectorException {
		for (Iterator<Relationship> iterator = relationships.iterator(); iterator.hasNext();) {
			Relationship relationship = iterator.next();
			ConnectorState state = Connector.getConnector().getState();
			if (!state.isModelAuditFields() && isAuditField(relationship.getForeignKeyField())) {
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

			
			String columnName = "FK_" + parent.getName() + "_" + col.getName();
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

	private void addTable(DescribeGlobalSObjectResult object) throws ConnectorException {
		DescribeSObjectResult objectMetadata = connection.getObjectMetaData(object.getName());
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
		ChildRelationship[] children = objectMetadata.getChildRelationships();
		if(children != null && children.length != 0) {
			for (int i = 0; i < children.length; i++) {
				ChildRelationship childRelation = children[i];
				Relationship newRelation = new RelationshipImpl();
				newRelation.setParentTable(objectMetadata.getName());
				newRelation.setChildTable(childRelation.getChildSObject());
				newRelation.setForeignKeyField(childRelation.getField());
				newRelation.setCascadeDelete(childRelation.isCascadeDelete());
				relationships.add(newRelation);
			}
		}
	}

	private void addColumns(DescribeSObjectResult objectMetadata, Table table) throws ConnectorException {
		Field[] fields = objectMetadata.getFields();
		for (int i=0;i < fields.length;i++) {
			Field field = fields[i];
			String normalizedName = NameUtil.normalizeName(field.getName());
			FieldType fieldType = field.getType();
			if(!Connector.getConnector().getState().isModelAuditFields() && isAuditField(field.getName())) {
				continue;
			}
			String sfTypeName = fieldType.getValue();
			Column column = null;
			if(sfTypeName.equals(FieldType._value1) || //string
					sfTypeName.equals(FieldType._value4) || //"combobox"
					sfTypeName.equals(FieldType._value5) || //"reference"
					sfTypeName.equals(FieldType._value13) || //"phone"
					sfTypeName.equals(FieldType._value14) || //"id"
					sfTypeName.equals(FieldType._value18) || //"url"
					sfTypeName.equals(FieldType._value19) || //"email"
					sfTypeName.equals(FieldType._value20) || //"encryptedstring"
					sfTypeName.equals(FieldType._value21)) {  //"anytype"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				column.setNativeType(sfTypeName);
				if(sfTypeName.equals(FieldType._value14)) {
					column.setNullType(NullType.No_Nulls);
					ArrayList<String> columnNames = new ArrayList<String>();
					columnNames.add(field.getName());
					metadataFactory.addPrimaryKey(field.getName()+"_PK", columnNames, table);
				}
			}
			else if(sfTypeName.equals(FieldType._value2)) { // "picklist"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				if(field.isRestrictedPicklist()) {
					column.setNativeType("restrictedpicklist");
				} else {
					column.setNativeType(FieldType._value2);
				}
				
				column.setProperty(COLUMN_PICKLIST_VALUES, getPicklistValues(field));
			}
			else if(sfTypeName.equals(FieldType._value3)) { //"multipicklist"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				if(field.isRestrictedPicklist()) {
					column.setNativeType("restrictedmultiselectpicklist");
				} else {
					column.setNativeType(FieldType._value3);
				}
				column.setProperty(COLUMN_PICKLIST_VALUES, getPicklistValues(field));
			}
			else if(sfTypeName.equals(FieldType._value6)) { //"base64"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.BLOB, table);
				column.setNativeType(FieldType._value6);
			}
			else if(sfTypeName.equals(FieldType._value7)) { //"boolean"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.BOOLEAN, table);
				column.setNativeType(FieldType._value7);
			}
			else if(sfTypeName.equals(FieldType._value8)) { //"currency"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DOUBLE, table);
				column.setNativeType(FieldType._value8);
				column.setCurrency(true);
				column.setScale(field.getScale());
				column.setPrecision(field.getPrecision());
			}
			else if(sfTypeName.equals(FieldType._value9)) { //"textarea"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.STRING, table);
				column.setNativeType(FieldType._value9);
				column.setSearchType(SearchType.Unsearchable);
			}
			else if(sfTypeName.equals(FieldType._value10)) { //"int"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.INTEGER, table);
				column.setNativeType(FieldType._value10);
				column.setPrecision(field.getPrecision());
			}
			else if(sfTypeName.equals(FieldType._value11) || //"double"
					sfTypeName.equals(FieldType._value12)) { //"percent"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DOUBLE, table);
				column.setNativeType(sfTypeName);
				column.setScale(field.getScale());
				column.setPrecision(field.getPrecision());
			}
			else if(sfTypeName.equals(FieldType._value15)) { //"date"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.DATE, table);
				column.setNativeType(FieldType._value15);
			}
			else if(sfTypeName.equals(FieldType._value16)) { //"datetime"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.TIMESTAMP, table);
				column.setNativeType(FieldType._value16);
			}
			else if(sfTypeName.equals(FieldType._value17)) { //"time"
				column = metadataFactory.addColumn(normalizedName, DataTypeManager.DefaultDataTypes.TIME, table);
				column.setNativeType(FieldType._value17);
			}
			if(null == column) {
				logger.logError("Unknown type returned by SalesForce: " + sfTypeName);
				continue;
			} else {
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
	}
	
	private String getPicklistValues(Field field) {
		StringBuffer picklistValues = new StringBuffer();
		if(null != field.getPicklistValues() && 0 != field.getPicklistValues().length) {
			List<PicklistEntry> entries = Arrays.asList(field.getPicklistValues());
			for (Iterator iterator = entries.iterator(); iterator.hasNext();) {
				PicklistEntry entry = (PicklistEntry) iterator.next();
				picklistValues.append(entry.getValue());
				if(iterator.hasNext()) {
					picklistValues.append(',');
				}
			}
		}
		return picklistValues.toString();
	}
}
