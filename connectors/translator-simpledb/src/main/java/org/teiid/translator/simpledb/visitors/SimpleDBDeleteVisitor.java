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

package org.teiid.translator.simpledb.visitors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Delete;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.resource.adpter.simpledb.SimpleDbAPIClass;

public class SimpleDBDeleteVisitor extends HierarchyVisitor {

	private boolean hasWhere = false;
	private String tableName;
	private boolean isSimpleDelete = false;
	private String itemName = ""; //$NON-NLS-1$
	private SimpleDbAPIClass apiClass;
	private List<String> itemNames;
	
	public SimpleDBDeleteVisitor(Delete delete, SimpleDbAPIClass apiClass) {
		this.apiClass = apiClass;
		visit(delete);
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public boolean hasWhere(){
		return hasWhere;
	}
	
	public boolean isSimpleDelete(){
		return isSimpleDelete;
	}
	
	public String getItemName(){
		return itemName;
	}
	
	public List<String> getItemNames() {
		return itemNames;
	}

	public void setItemNames(List<String> itemNames) {
		this.itemNames = itemNames;
	}

	@Override
	public void visit(Delete obj) {
		visitNode(obj.getTable());
		if (obj.getWhere() != null){
			hasWhere = true;
			visitNode(obj.getWhere());
		}
	}
	
	@Override
	public void visit(NamedTable obj) {
		super.visit(obj);
		tableName = obj.getName();
	}
	
	@Override
	public void visit(Comparison obj) {
		//check whether it's simple delete (itemName() = <value>)
		if (obj.getLeftExpression() instanceof ColumnReference){
			if (((ColumnReference)obj.getLeftExpression()).getMetadataObject().getName().equals("itemName()") && obj.getOperator() == Operator.EQ){ //$NON-NLS-1$
				isSimpleDelete = true;
				if (obj.getRightExpression() instanceof Literal){
					itemName = (String) ((Literal)obj.getRightExpression()).getValue();
				}else{
					//Could here be something else than literal?
//					throw new TranslatorException("Wrong DELETE Format");
				}
				super.visit(obj);
				return;
			}
		}else if (obj.getRightExpression() instanceof ColumnReference){
			if (((ColumnReference)obj.getRightExpression()).getMetadataObject().getName().equals("itemName()") && obj.getOperator() == Operator.EQ){ //$NON-NLS-1$
				isSimpleDelete = true;
				if (obj.getLeftExpression() instanceof Literal){
					itemName = (String) ((Literal)obj.getRightExpression()).getValue();
				}else{
					//Could here be something else than literal?
//					throw new TranslatorException("Wrong DELETE Format");
				}
				super.visit(obj);
				return;
			}
		}
		getItemNamesForCriteria(obj);
		//non trivial DELETE StatementgetItemNamesForCriteria(obj);
		super.visit(obj);
	}
	
	private void getItemNamesForCriteria(Comparison obj){
		ArrayList<String> columns = new ArrayList<String>();
		if (itemNames == null){
			itemNames = new ArrayList<String>();
		}
		columns.add("itemName()"); //$NON-NLS-1$
		Iterator<List<String>> response = apiClass.performSelect("SELECT itemName() FROM "+tableName+" WHERE "+SimpleDBSQLVisitor.getSQLString(obj), columns); //$NON-NLS-1$ //$NON-NLS-2$
		while (response.hasNext()) {
			itemNames.add(response.next().get(0));
		}
	}
}
