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
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ODataMetadataProcessor {
	public static final String LINK_TABLES = MetadataFactory.ODATA_URI+"LinkTables"; //$NON-NLS-1$
	public static final String HTTP_METHOD = MetadataFactory.ODATA_URI+"HttpMethod"; //$NON-NLS-1$
	public static final String JOIN_COLUMN = MetadataFactory.ODATA_URI+"JoinColumn"; //$NON-NLS-1$
	public static final String ENTITY_TYPE = MetadataFactory.ODATA_URI+"EntityType"; //$NON-NLS-1$

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
						addFunctionImportAsProcedure(mf, function);
					}
				}
			}
		}
	}


	Table addEntitySetAsTable(MetadataFactory mf, String name, EdmEntityType entity) throws TranslatorException {
		Table table = mf.addTable(name);
		table.setProperty(ENTITY_TYPE, entity.getFullyQualifiedTypeName());
		table.setSupportsUpdate(true);

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
		return table;
	}

	void addNavigationRelations(MetadataFactory mf, String tableName, EdmEntityType fromEntity) throws TranslatorException {
		Table fromTable = mf.getSchema().getTable(tableName);

		for(EdmNavigationProperty nav:fromEntity.getNavigationProperties()) {
			EdmAssociation association = nav.getRelationship();

			EdmAssociationEnd fromEnd = nav.getFromRole();
			EdmAssociationEnd toEnd = nav.getToRole();

			EdmEntityType toEntity = toEnd.getType();

			// no support for self-joins
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
					mf.addForiegnKey(fromTable.getName() + "_FK", leftNames, fromTable.getName(), linkTable); //$NON-NLS-1$
					mf.addForiegnKey(toTable.getName() + "_FK", rightNames, toTable.getName(), linkTable); // //$NON-NLS-1$
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
				column = mf.addColumn(column.getName(), column.getDatatype().getRuntimeTypeName(), linkTable);
				column.setProperty(JOIN_COLUMN, String.valueOf(true));
			}
		}
		else {
			columnNames = new ArrayList<String>();
			for (Column column :table.getPrimaryKey().getColumns()) {
				columnNames.add(column.getName());
				if (linkTable.getColumnByName(column.getName()) == null) {
					column = mf.addColumn(column.getName(), column.getDatatype().getRuntimeTypeName(), linkTable);
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
				mf.addForiegnKey(association.getName(), toKeys, fromKeys, fromTable.getName(), toTable);
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
				mf.addForiegnKey(association.getName(), fromKeys, fromTable.getName(), toTable);
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

	private boolean keyMatches(List<String> names, KeyRecord record) {
		if (names.size() != record.getColumns().size()) {
			return false;
		}
		for (int i = 0; i < names.size(); i++) {
			if (!names.get(i).equalsIgnoreCase(record.getColumns().get(i).getName())) {
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
		c.setUpdatable(true);
		return c;
	}


	void addFunctionImportAsProcedure(MetadataFactory mf, EdmFunctionImport function) throws TranslatorException {
		Procedure procedure = mf.addProcedure(function.getName());
		procedure.setProperty(ENTITY_TYPE, function.getReturnType().getFullyQualifiedTypeName());
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
		else if (returnType instanceof EdmComplexType) {
			addProcedureTableReturn(mf, procedure, returnType, null);
		}
		else if (returnType instanceof EdmEntityType) {
			addProcedureTableReturn(mf, procedure, returnType, function.getEntitySet().getName());
		}
		else if (returnType instanceof EdmCollectionType) {
			addProcedureTableReturn(mf, procedure, ((EdmCollectionType)returnType).getItemType(), function.getEntitySet().getName());
		}
		else {
			throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17005, function.getName(), returnType.getFullyQualifiedTypeName()));
		}
	}

	private void addProcedureTableReturn(MetadataFactory mf, Procedure procedure, EdmType type, String enitySetName) throws TranslatorException {
		if (enitySetName != null) {
			// when this is true, the proc returns collection; in teiid we do not have to any different
		}
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
					addProcedureTableReturn(mf, procedure, ep.getType(), enitySetName);
				}
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
