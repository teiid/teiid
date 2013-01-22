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
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.*;
import org.teiid.translator.TranslatorException;

public class ODataMetadataProcessor {
	//public static final String PARENT_TABLE = "ParentTable"; //$NON-NLS-1$
	public static final String LINK_TABLES = "LinkTables"; //$NON-NLS-1$
	public static final String HTTP_METHOD = "HttpMethod"; //$NON-NLS-1$
	public static final String JOIN_COLUMN = "JoinColumn"; //$NON-NLS-1$
	public static final String ENTITY_TYPE = "EntityType"; //$NON-NLS-1$
	public static final String ENTITY_ALIAS = "EntityAlias"; //$NON-NLS-1$
	
	private String entityContainer;
	private String schemaNamespace;
	
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
						addEntitySetAsTable(mf, entitySet.getName(), entitySet.getType());
					}
					
					// build relations ships among tables
					for (EdmEntitySet entitySet:container.getEntitySets()) {
						addNavigationRelations(mf, entitySet.getName(), entitySet.getType());
					}
					
					// add procedures
					for (EdmFunctionImport function:container.getFunctionImports()) {
						addFunimportAsProcedure(mf, function);
					}
				}
			}
		}		
	}
	

	void addEntitySetAsTable(MetadataFactory mf, String name, EdmEntityType entity) throws TranslatorException {
		Table table = mf.addTable(name);
		table.setProperty(ENTITY_TYPE, entity.getFullyQualifiedTypeName());
		
		// add columns
		for (EdmProperty ep:entity.getProperties().toList()) {
			if (ep.getType().isSimple()) {
				addPropertyAsColumn(mf, table, ep); 
			}
			else {
				// this is complex type, i.e treat them as embeddable in the same table add all columns.
				// Have tried adding this as separate table with 1 to 1 mapping to parent table, however
				// that model fails when there are two instances of single complex type as column. This
				// creates verbose columns but safe.
				EdmComplexType embedded = (EdmComplexType)ep.getType();
				for (EdmProperty property:embedded.getProperties().toList()) {
					if (property.getType().isSimple()) {
						Column column = addPropertyAsColumn(mf, table, property, ep.getName());
						column.setProperty(ENTITY_TYPE, ep.getName());
						column.setNameInSource(property.getName());
					}
					else {
						throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17002, name, ep.getName()));
					}
				}				
			}
		}
		
		// add PK
		mf.addPrimaryKey("PK", entity.getKeys(), table); //$NON-NLS-1$

		/*
		// add complex types are embedded types, expose them as tables 
		// with 1 to 1 relationship with access pattern on them
		for (EdmProperty ep:entity.getProperties().toList()) {
			if (!ep.getType().isSimple()) {
				addComplexTypeAsTable(mf, (EdmComplexType)ep.getType(), table);
			}
		}
		*/
	}
	
	
	/*
	private void addComplexTypeAsTable(MetadataFactory mf, EdmComplexType embedded, Table parentTable) throws TranslatorException{

		// check if table already added
		Table table = mf.getSchema().getTable(embedded.getName());
		if (table == null) {
			// child table
			table = mf.addTable(embedded.getName());
			table.setProperty(PARENT_TABLE, parentTable.getName());
			table.setProperty(ENTITY_TYPE, "EdmComplexType"); //$NON-NLS-1$
			
			// add columns
			for (EdmProperty ep:embedded.getProperties().toList()) {
				if (ep.getType().isSimple()) {
					addPropertyAsColumn(mf, table, ep);
				}
				else {
					throw new TranslatorException("embedded_can_not_embed");
				}
			}
			
			// add all parent table's PK as keys
			ArrayList<String> pkNames = new ArrayList<String>();
			List<Column> columns = parentTable.getPrimaryKey().getColumns();
			for (Column c: columns) {
				String name = parentTable.getName()+"_"+c.getName(); //$NON-NLS-1$
				Column addedColumn = mf.addColumn(name, c.getDatatype().getRuntimeTypeName(), table);
				addedColumn.setProperty(JOIN_COLUMN, String.valueOf(true));
				pkNames.add(name);
			}
			// add PK to the table and have access pattern on it			
			mf.addPrimaryKey(parentTable.getName()+"_PK", pkNames, table); //$NON-NLS-1$
			mf.addAccessPattern(parentTable.getName()+"_embed", pkNames, table); //$NON-NLS-1$
			mf.addForiegnKey(parentTable.getName()+"_FK", pkNames, parentTable.getName(), table); //$NON-NLS-1$
		}
		else {
			if (!table.getProperty(PARENT_TABLE, false).equals(parentTable.getName())) {
				// add all parent table's PK as keys
				ArrayList<String> pkNames = new ArrayList<String>();
				List<Column> columns = parentTable.getPrimaryKey().getColumns();
				for (Column c: columns) {
					String name = parentTable.getName()+"_"+c.getName(); //$NON-NLS-1$
					Column addedColumn = mf.addColumn(name, c.getDatatype().getRuntimeTypeName(), table);
					addedColumn.setProperty(JOIN_COLUMN, String.valueOf(true));
					pkNames.add(name);
				}
				mf.addAccessPattern(parentTable.getName()+"_embed", pkNames, table); //$NON-NLS-1$
				mf.addForiegnKey(parentTable.getName()+"_FK", pkNames, parentTable.getName(), table); //$NON-NLS-1$				
			}
		}
	}
	*/
	
	void addNavigationRelations(MetadataFactory mf, String tableName, EdmEntityType orderEntity) throws TranslatorException {
		Table orderTable = mf.getSchema().getTable(tableName);
		
		for(EdmNavigationProperty nav:orderEntity.getNavigationProperties()) {
			EdmAssociation association = nav.getRelationship();
			
			EdmAssociationEnd orderEnd = nav.getFromRole();
			EdmAssociationEnd detailsEnd = nav.getToRole();
			
			EdmEntityType detailsEntity = detailsEnd.getType();
			
			// no support for self-joins
			if (same(orderEntity, detailsEntity)) {
				return;
			}

			// Usually navigation name is navigation table name.
			Table detailsTable = mf.getSchema().getTable(nav.getName());
			if (detailsTable == null) {
				// if the table not found; then navigation name may be an alias name
				// find by the entity type
				detailsTable = getEntityTable(mf, detailsEntity);
				detailsTable.setProperty(ENTITY_ALIAS, nav.getName());
			}
			
			if (isMultiplicityMany(orderEnd) && isMultiplicityMany(detailsEnd)) {
				if (mf.getSchema().getTable(association.getName()) == null) {
					Table linkTable = mf.addTable(association.getName());
					linkTable.setProperty(ENTITY_TYPE, "LinkTable"); //$NON-NLS-1$
					linkTable.setProperty(LINK_TABLES, orderTable.getName()+","+detailsTable.getName()); //$NON-NLS-1$
					
					//left table
					List<String> leftNames = null;
					if (association.getRefConstraint() != null) {
						leftNames = association.getRefConstraint().getPrincipalReferences();
					}
					leftNames = addLinkTableKeys(mf, orderTable, leftNames, linkTable);
					
					//right table
					List<String> rightNames = null;
					if (association.getRefConstraint() != null) {
						rightNames = association.getRefConstraint().getDependentReferences();
					}
					rightNames = addLinkTableKeys(mf, detailsTable, rightNames, linkTable);	
					
					ArrayList<String> allKeys = new ArrayList<String>();
					for(Column c:linkTable.getColumns()) {
						allKeys.add(c.getName());
					}
					mf.addPrimaryKey("PK", allKeys, linkTable); //$NON-NLS-1$
					
					// add fks in both left and right tables
					mf.addForiegnKey(association.getName(), leftNames,
							getColumnNames(linkTable.getPrimaryKey().getColumns()), 
							linkTable.getName(), orderTable);

					mf.addForiegnKey(association.getName(), rightNames,
							getColumnNames(linkTable.getPrimaryKey().getColumns()), 
							linkTable.getName(), detailsTable);
				}
				
			} else if (isMultiplicityOne(orderEnd)) {
				addRelation(mf, orderTable, detailsTable, association, orderEnd.getRole());
			}
		}
	}


	private List<String> addLinkTableKeys(MetadataFactory mf, Table orderTable, List<String> columnNames, Table linkTable)
			throws TranslatorException {
		if (columnNames != null) {
			for (String columnName:columnNames) {
				Column column = orderTable.getColumnByName(columnName);
				if (column == null) {
					throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17003, columnName, orderTable.getName()));
				}
				String name = orderTable.getName()+"_"+column.getName(); //$NON-NLS-1$
				column = mf.addColumn(name, column.getDatatype().getRuntimeTypeName(), linkTable);
				column.setProperty(JOIN_COLUMN, String.valueOf(true));
			}
		}
		else {
			columnNames = new ArrayList<String>();
			for (Column column :orderTable.getPrimaryKey().getColumns()) {
				columnNames.add(column.getName());
				String name = orderTable.getName()+"_"+column.getName(); //$NON-NLS-1$
				column = mf.addColumn(name, column.getDatatype().getRuntimeTypeName(), linkTable);
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

	private void addRelation(MetadataFactory mf, Table orderTable, Table detailsTable, EdmAssociation association, String primaryRole) {
		EdmReferentialConstraint refConstaint = association.getRefConstraint();
		if (refConstaint != null) {
			List<String> orderKeys = null;
			List<String> detailsKeys = null;			
			if (refConstaint.getPrincipalRole().equals(primaryRole)) {
				orderKeys = refConstaint.getPrincipalReferences();
				detailsKeys = refConstaint.getDependentReferences();
			}
			else {
				orderKeys = refConstaint.getDependentReferences();
				detailsKeys = refConstaint.getPrincipalReferences();
			}
			mf.addForiegnKey(association.getName(), detailsKeys, orderKeys, orderTable.getName(), detailsTable);
		}
		else {
			// add the key columns from into many side
			ArrayList<String> names = new ArrayList<String>();
			for (Column c :orderTable.getPrimaryKey().getColumns()) {
				if (detailsTable.getColumnByName(c.getName()) == null) {
					Column addedColumn = mf.addColumn(c.getName(), c.getDatatype().getRuntimeTypeName(), detailsTable);
					addedColumn.setProperty(JOIN_COLUMN, String.valueOf(true));
				}
				names.add(c.getName());
			}
			
			// create a FK on the columns added
			mf.addForiegnKey(association.getName(), names, orderTable.getName(), detailsTable);
		}
	}

	private Column addPropertyAsColumn(MetadataFactory mf, Table table, EdmProperty ep) {
		return addPropertyAsColumn(mf, table, ep, null);
	}
	
	private Column addPropertyAsColumn(MetadataFactory mf, Table table, EdmProperty ep, String prefix) {
		String columnName = ep.getName();
		if (prefix != null) {
			columnName = prefix+"_"+columnName; //$NON-NLS-1$
		}
		Column c = mf.addColumn(columnName, ODataTypeManager.teiidType(ep.getType().getFullyQualifiedTypeName()), table);
		if (ep.getFixedLength() != null) {
			c.setFixedLength(ep.getFixedLength());
		}
		c.setNullType(ep.isNullable()?NullType.Nullable:NullType.No_Nulls);
		if (ep.getMaxLength() != null) {
			c.setLength(ep.getMaxLength());
		}
		return c;
	}	

	
	void addFunimportAsProcedure(MetadataFactory mf, EdmFunctionImport function) throws TranslatorException {
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
		if (returnType.isSimple()) {
			mf.addProcedureParameter("return", ODataTypeManager.teiidType(((EdmSimpleType)returnType).getFullyQualifiedTypeName()), ProcedureParameter.Type.ReturnValue, procedure); //$NON-NLS-1$
		}
		else if (returnType instanceof EdmCollectionType) {
			addProcedureTableReturn(mf, procedure, ((EdmCollectionType)returnType).getItemType(), function.getEntitySet().getName());
		}
		else {
			throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17005, function.getName(), returnType.getFullyQualifiedTypeName()));
		}
	}
	
	private void addProcedureTableReturn(MetadataFactory mf, Procedure procedure, EdmType type, String enitySetName) throws TranslatorException {
		if (type.isSimple()) {
			mf.addProcedureResultSetColumn("return", ODataTypeManager.teiidType(((EdmSimpleType)type).getFullyQualifiedTypeName()), procedure); //$NON-NLS-1$
		}
		else if (type instanceof EdmComplexType) {
			EdmComplexType complexType = (EdmComplexType)type;
			// read from table beacause we already normalized the embedded types etc.
			Table table = mf.getSchema().getTable(complexType.getName());
			for (Column column:table.getColumns()) {
				mf.addProcedureResultSetColumn(column.getName(), column.getDatatype().getRuntimeTypeName(), procedure);
			}
		}
		else if (type instanceof EdmEntityType) {
			Table table = mf.getSchema().getTable(enitySetName);
			for (Column column:table.getColumns()) {
				mf.addProcedureResultSetColumn(column.getName(), column.getDatatype().getRuntimeTypeName(), procedure);
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
}
