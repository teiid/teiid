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
package org.teiid.translator.object;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.BaseLanguageObject;
import org.teiid.language.ColumnReference;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.util.ObjectUtil;

/**
 * @author vanhalbert
 *
 */
public class ObjectVisitor extends HierarchyVisitor {
	

	private List<DerivedColumn> projectedColumns = new ArrayList<DerivedColumn>();
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	
	private NamedTable table;
	
	// will be non-null only when a child table is being processed
	private String rootTableName = null;
	private int limit;
	private int numForeignKeys;
	private ForeignKey fk = null;	
	private String fkeyColNIS = null;
	private Column pkkeyCol = null;
	private BaseLanguageObject command=null;
	private boolean isSelect = false;

	protected Condition condition=null;
	protected OrderBy orderBy=null;
	
	private Insert insert;
	private Update update;
	private Delete delete;


	public List<DerivedColumn> getProjectedColumns() {
		return projectedColumns;
	}
	
	public Condition getWhereCriteria() {
		return this.condition;
	}
	
	public OrderBy getOrderBy() {
		return this.orderBy;
	}
	
	public int getLimit() {
		return limit;
	}

	public List<TranslatorException> getExceptions() {
		return exceptions;
	}
	
	public void addException(TranslatorException e) {
		exceptions.add(e);
	}

	public NamedTable getTable() {
		return table;
	}
	
	/**
	 * Will return non-null value only when the getTable() represents
	 * a child table.
	 * @return String for parent table
	 */
	public String getRootTableName() {
		return rootTableName;
	}
	
	public Column getPrimaryKeyCol() {
		return pkkeyCol;
	}
	
	public ForeignKey getForeignKey() {
		return this.fk;
	}
	
	public String getForeignKeyNameInSource() {
		return this.fkeyColNIS;
	}
	
	public boolean isSelectCommand(){
		return this.isSelect;
	}
		
	public Insert getInsert() {
		return insert;
	}
	
	public Update getUpdate() {
		return update;
	}
	
	public Delete getDelete() {
		return delete;
	}

	@Override
	public void visit(DerivedColumn obj) {
		super.visit(obj);
		this.projectedColumns.add(obj);
	}
	
	@Override
	public void visit(NamedTable obj) {
		super.visit(obj);
		this.table = obj;
		
			if (obj.getMetadataObject().getPrimaryKey() != null) {
				this.pkkeyCol = table.getMetadataObject().getPrimaryKey()
						.getColumns().get(0);					
			}
		
			List<ForeignKey> fkeys = obj.getMetadataObject().getForeignKeys();
			if (fkeys.size() > 0) { 
				numForeignKeys++;
				fk = fkeys.get(0);
				fkeyColNIS = getForeignKeyNIS(obj, fk);
				if (fkeyColNIS == null) {
					this.addException(new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21006, new Object[] { ( this.isSelect ? "Select" : "Update"), obj.getName()})));
				}
				
				rootTableName = fk.getReferenceKey().getParent().getName();
			}
			
			
			if (fkeyColNIS == null && this.pkkeyCol == null) {
				addException( new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21006, new Object[] {( this.isSelect ? "Select" : "Update"),obj.getName()})));
			}			
		
		if (numForeignKeys > 1) {
			addException(new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21002, new Object[] {command})));
		}		
	}

	
	@Override
	public void visit(Limit obj) {
		this.limit = obj.getRowLimit();
		super.visit(obj);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.language.visitor.AbstractLanguageVisitor#visitNode(org.teiid.language.LanguageObject)
	 */
	@Override
	public void visitNode(LanguageObject obj) {
		command = (BaseLanguageObject) obj;
		super.visitNode(obj);
		
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.language.visitor.HierarchyVisitor#visit(org.teiid.language.Select)
	 */
	@Override
	public void visit(Select obj) {
		this.condition = obj.getWhere();
		this.orderBy = obj.getOrderBy();
		this.isSelect = true;
		super.visit(obj);
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.language.visitor.HierarchyVisitor#visit(org.teiid.language.Insert)
	 */
	@Override
	public void visit(Insert obj) {
		super.visit(obj);
		this.insert = obj;
		
		List<ColumnReference> columns = obj.getColumns();
		List<Expression> values = ((ExpressionValueSource) obj
				.getValueSource()).getValues();
		if (columns.size() != values.size()) {
			this.addException(new TranslatorException(
					"Program error, Column Metadata Size [" + columns.size() + "] and Value Size [" + values.size() + "] don't match"));
		}
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.language.visitor.HierarchyVisitor#visit(org.teiid.language.Delete)
	 */
	@Override
	public void visit(Delete obj) {
		this.condition = obj.getWhere();
		this.delete = obj;
		super.visit(obj);
	}	
	

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.language.visitor.HierarchyVisitor#visit(org.teiid.language.Update)
	 */
	@Override
	public void visit(Update obj) {
		this.condition = obj.getWhere();
		this.update = obj;
		super.visit(obj);
	}		
	
	protected String getForeignKeyNIS(NamedTable table, ForeignKey fk)  {

		String fkeyColNIS = null;
		
		if (fk != null) {
			if (fk.getReferenceKey() != null) {
				Column fkeyCol = fk.getReferenceKey().getColumns().get(0);
				fkeyColNIS = ObjectUtil.getRecordName(fkeyCol);
			} else if (fk.getReferenceColumns() != null) {
				fkeyColNIS =fk.getReferenceColumns().get(0);
			}
		}
		
		return fkeyColNIS;

	}
	
	public void cleanUp() {
		if (projectedColumns != null) projectedColumns.clear();
		projectedColumns = null;
		if (exceptions != null) exceptions.clear();
		exceptions = null;
		
		
		fk = null;	
		pkkeyCol = null;
		command=null;

		condition=null;
		orderBy=null;

	}

}
