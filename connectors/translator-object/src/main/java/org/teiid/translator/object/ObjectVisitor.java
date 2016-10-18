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

import org.teiid.core.util.StringUtil;
import org.teiid.language.AndOr;
import org.teiid.language.BaseLanguageObject;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.In;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.metadata.JavaBeanMetadataProcessor;
import org.teiid.translator.object.util.ObjectUtil;

/**
 * @author vanhalbert
 *
 */
public class ObjectVisitor extends HierarchyVisitor {
	

	private List<DerivedColumn> projectedColumns = new ArrayList<DerivedColumn>();
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	
	private ArrayList<Object> values = new ArrayList<Object>();

	
	private NamedTable table;
	private String tableName;
	// identifies the table that the staging table is staging on behalf 
	private String primaryTable;
	
	// will be non-null only when a child table is being processed
	private String rootTableName = null;
	private int limit;
	private int numForeignKeys;
	private ForeignKey fk = null;	
	private String fkeyRefColumnName = null;
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
	
	private void setTable(NamedTable t) {
		String tn = t.getName(); 
		// remove any folders that exist within the model (these are not folders that the models rsides in).
		if (tn.contains(".")) {
			tn = StringUtil.getLastToken(tn, ".");
		}
		
		this.tableName=tn;
		this.table = t;
		
		this.primaryTable =  t.getMetadataObject().getProperty(JavaBeanMetadataProcessor.PRIMARY_TABLE_PROPERTY, false);
		if (this.primaryTable != null && primaryTable.contains(".")) {
			primaryTable = StringUtil.getLastToken(primaryTable, ".");
		}

	}
		
	public String getTableName() {
		return this.tableName;
	}
	
	public String getPrimaryTable() {
		return this.primaryTable;
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
	
	public String getForeignKeyReferenceColName() {
		return this.fkeyRefColumnName;
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
		this.setTable(obj);
		
			if (obj.getMetadataObject().getPrimaryKey() != null) {
				this.pkkeyCol = table.getMetadataObject().getPrimaryKey()
						.getColumns().get(0);					
			}
		
			List<ForeignKey> fkeys = obj.getMetadataObject().getForeignKeys();
			if (fkeys.size() > 0) { 
				numForeignKeys++;
				fk = fkeys.get(0);
				fkeyRefColumnName = getForeignKeyRefcolumn(obj, fk);
				if (fkeyRefColumnName == null) {
					this.addException(new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21006, new Object[] { ( this.isSelect ? "Select" : "Update"), obj.getName()})));
				}
				
				rootTableName = fk.getReferenceKey().getParent().getName();
			}
			
			
			if (fkeyRefColumnName == null && this.pkkeyCol == null) {
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
		super.visit(obj);
		this.condition = obj.getWhere();
		this.delete = obj;
	}	
	

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.language.visitor.HierarchyVisitor#visit(org.teiid.language.Update)
	 */
	@Override
	public void visit(Update obj) {
		super.visit(obj);
		this.condition = obj.getWhere();
		this.update = obj;
	}	
	
	public List<Object> getCriteriaValues() {
		return values;
	}
	

	@Override
    public void visit(AndOr obj) {
        visitNode(obj.getLeftCondition());
        visitNode(obj.getRightCondition());
    }
	
	@Override
	public void visit(Comparison obj) {
		super.visit(obj);
		
		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
		"Parsing Comparison criteria."); //$NON-NLS-1$

		Expression lhs = obj.getLeftExpression();
		Expression rhs = obj.getRightExpression();

		// joins between the objects is assumed as if no criteria, and therefore, return all
		if (lhs instanceof ColumnReference && rhs instanceof ColumnReference) {
			return;
		} else if (lhs instanceof Literal && rhs instanceof Literal) {
			return;
		}

		Object value=null;
		Column mdIDElement = null;
		Literal literal = null;
		if (lhs instanceof ColumnReference) {

			mdIDElement = ((ColumnReference) lhs).getMetadataObject();
			literal = (Literal) rhs;
			value = literal.getValue();

		} else if (rhs instanceof ColumnReference ){
			mdIDElement = ((ColumnReference) rhs).getMetadataObject();
			literal = (Literal) lhs;
			value = literal.getValue();
		}	
		
		Object criteria;
		try {
			criteria = ObjectUtil.convertValueToObjectType(value, mdIDElement);
			values.add(criteria);
		} catch (TranslatorException e) {
			exceptions.add(e);
		}

	}
	
	@Override
	public void visit(In obj) {
		super.visit(obj);
		if (obj.isNegated()) {
			this.addException(new TranslatorException("ObjectQueryVisitor: Not In criteria is not supported"));
			return;
		}

		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Parsing IN criteria."); //$NON-NLS-1$
		
		Expression lhs = obj.getLeftExpression();
		Column col = ((ColumnReference) lhs).getMetadataObject();
		
		List<Expression> rhsList = obj.getRightExpressions();
		
		for (Expression expr : rhsList) {

			Literal literal = (Literal) expr;
			
			Object criteria;
			try {
				criteria = ObjectUtil.convertValueToObjectType(literal.getValue(), col);
				values.add(criteria);
			} catch (TranslatorException e) {
				exceptions.add(e);
			}
			
		}				
	
	}	
	
	protected String getForeignKeyRefcolumn(NamedTable table, ForeignKey fk)  {

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
	
	protected String getForeignKeyColumnName(NamedTable table, ForeignKey fk)  {

		String fkeyColNIS = null;
		
		if (fk != null) {
			return fk.getNameInSource();
//			if (fk.getReferenceKey() != null) {
//				Column fkeyCol = fk.getReferenceKey().getColumns().get(0);
//				fkeyColNIS = ObjectUtil.getRecordName(fkeyCol);
//			} else if (fk.getReferenceColumns() != null) {
//				fkeyColNIS =fk.getReferenceColumns().get(0);
//			}
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
