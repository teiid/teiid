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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.visitor.HierarchyVisitor;
public class SimpleDBInsertVisitor extends HierarchyVisitor {
	
	private Map<String, String> columnsValuesMap;
	private List<String> columnNames;
	
	public SimpleDBInsertVisitor() {
		columnsValuesMap = new HashMap<String, String>();
		columnNames = new ArrayList<String>();
	}
	
	public Map<String, String> returnColumnsValuesMap() {
		return columnsValuesMap;
	}
	
	public static Map<String, String> getColumnsValuesMap(Insert insert) {
		SimpleDBInsertVisitor visitor = new SimpleDBInsertVisitor();
		visitor.visit(insert);
		return visitor.returnColumnsValuesMap();
	}
	
	public static String getDomainName(Insert insert){
		return insert.getTable().getName();
	}
	
	@Override
	public void visit(ColumnReference obj) {
		columnNames.add(obj.getMetadataObject().getName());
		super.visit(obj);
	}
	
	@Override
	public void visit(ExpressionValueSource obj) {
		List<Expression> values = obj.getValues();
		for (int i = 0; i< obj.getValues().size(); i++){
			if (values.get(i) instanceof Literal){
				Literal lit = (Literal) values.get(i);
				columnsValuesMap.put(columnNames.get(i), (String) lit.getValue());
			}else{
				throw new RuntimeException("Just literals are allowed in VALUES section so far"); //$NON-NLS-1$
			}
		}
		super.visit(obj);
	}
}
