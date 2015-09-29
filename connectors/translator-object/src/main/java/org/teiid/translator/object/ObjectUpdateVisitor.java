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


import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Update;
import org.teiid.translator.TranslatorException;

/**
 * @author vanhalbert
 *
 */
public class ObjectUpdateVisitor extends ObjectSelectVisitor {
	private Insert insert;
	private Update update;
	private Delete delete;

	
	public Insert getInsert() {
		return insert;
	}
	
	public Update getUpdate() {
		return update;
	}
	
	public Delete getDelete() {
		return delete;
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
