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
package org.teiid.translator.infinispan.cache;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.BaseLanguageObject;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectPlugin;
import org.teiid.translator.object.ObjectSelectVisitor;

public class SearchByInfinispanVisitor extends ObjectSelectVisitor {

	private List<DerivedColumn> projectedColumns = new ArrayList<DerivedColumn>();
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	
	private ArrayList<Object> values = new ArrayList<Object>();
	private NamedTable table;
	private int limit;
	private int numForeignKeys;
	private ForeignKey fk = null;	
	private Column pkkeyCol = null;
	private Condition condition=null;
	private OrderBy orderBy=null;
	private BaseLanguageObject command=null;
	private boolean isSelect = false;
		
	@Override
	public List<DerivedColumn> getProjectedColumns() {
		return projectedColumns;
	}
	
	@Override
	public Condition getWhereCriteria() {
		return this.condition;
	}
	
	@Override
	public OrderBy getOrderBy() {
		return this.orderBy;
	}
	
	@Override
	public int getLimit() {
		return limit;
	}

	@Override
	public List<TranslatorException> getExceptions() {
		return exceptions;
	}

	@Override
	public NamedTable getTable() {
		return table;
	}
	
	public List<Object> getCriteriaValues() {
		return values;
	}
	
	@Override
	public Column getPrimaryKeyCol() {
		return pkkeyCol;
	}
	
	@Override
	public ForeignKey getForeignKey() {
		return this.fk;
	}
	
	@Override
	public boolean isSelectCommand(){
		return this.isSelect;
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
			}
		
		if (numForeignKeys > 1) {
			exceptions.add(new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21002, new Object[] {command})));
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

//	/**
//	 * {@inheritDoc}
//	 *
//	 * @see org.teiid.translator.object.ObjectSelectVisitor#visitNode(org.teiid.language.Command)
//	 */
//	@Override
//	public void visitNode(Command obj) {
//		super.visitNode(obj);
//	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.language.visitor.HierarchyVisitor#visit(org.teiid.language.Insert)
	 */
	@Override
	public void visit(Insert obj) {
		super.visit(obj);
	}


	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.language.visitor.HierarchyVisitor#visit(org.teiid.language.Delete)
	 */
	@Override
	public void visit(Delete obj) {
		this.condition = obj.getWhere();
		super.visit(obj);
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
	 * @see org.teiid.language.visitor.HierarchyVisitor#visit(org.teiid.language.Update)
	 */
	@Override
	public void visit(Update obj) {
		this.condition = obj.getWhere();
		super.visit(obj);
	}
}
