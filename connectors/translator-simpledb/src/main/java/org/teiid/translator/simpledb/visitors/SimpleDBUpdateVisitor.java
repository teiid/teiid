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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.language.Comparison;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.resource.adpter.simpledb.SimpleDbAPIClass;

public class SimpleDBUpdateVisitor extends HierarchyVisitor{

	private SimpleDbAPIClass apiClass;
	private String tableName;
	private Map<String, String> attributes;
	private List<String> itemNames;
	
	public SimpleDBUpdateVisitor(Update update, SimpleDbAPIClass apiClass) {
		attributes = new HashMap<String, String>();
		itemNames = new ArrayList<String>();
		this.apiClass = apiClass;
		visit(update);
	}
	
	@Override
	public void visit(Update obj) {
		tableName = obj.getTable().getName();
		for(SetClause setClause : obj.getChanges()){
			visitNode(setClause);
		}
		if (obj.getWhere() != null){
			visitNode(obj.getWhere());
		}
		super.visit(obj);
	}
	
	@Override
	public void visit(SetClause obj) {
		if (obj.getValue() instanceof Literal){
			Literal l = (Literal) obj.getValue();
			attributes.put(obj.getSymbol().getName(), (String) l.getValue());
		}
		super.visit(obj);
	}
	
	@Override
	public void visit(Comparison obj) {
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("itemName()"); //$NON-NLS-1$
		Iterator<List<String>> response = apiClass.performSelect("SELECT itemName() FROM "+tableName+" WHERE "+SimpleDBSQLVisitor.getSQLString(obj), columns); //$NON-NLS-1$ //$NON-NLS-2$
		while (response.hasNext()) {
			itemNames.add(response.next().get(0));
		}
		super.visit(obj);
	}

	public String getTableName() {
		return tableName;
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public List<String> getItemNames() {
		return itemNames;
	}
	
	
	
}
