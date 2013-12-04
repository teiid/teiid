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

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.SQLStringVisitor;

public class SimpleDBSQLVisitor extends SQLStringVisitor {
	
	
	@Override
	public void visit(Select obj) {
		buffer.append(SELECT).append(Tokens.SPACE);
		if (obj.getDerivedColumns().size()>1){
			List<DerivedColumn> columnsList = new ArrayList<DerivedColumn>();
			for (DerivedColumn column : obj.getDerivedColumns()) {
				ColumnReference ref = (ColumnReference) column.getExpression();
				if (!ref.getMetadataObject().getName().equals("itemName()")){ //$NON-NLS-1$
					columnsList.add(column);
				}
			}
			append(columnsList);
		}else{
			append(obj.getDerivedColumns());
		}
		buffer.append(Tokens.SPACE);
		if (obj.getFrom() != null && !obj.getFrom().isEmpty()){
			buffer.append(FROM).append(Tokens.SPACE);
			append(obj.getFrom());
			buffer.append(Tokens.SPACE);
		}
		if (obj.getWhere() != null){
			buffer.append(WHERE).append(Tokens.SPACE);
			append(obj.getWhere());
		}
		if (obj.getLimit() != null){
			append(obj.getLimit());
		}
	}
	
	
	@Override
	public void visit(ColumnReference obj) {
		buffer.append(obj.getName());
	}
	
	public static String getSQLString(LanguageObject obj){
		SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
		visitor.append(obj);
		return visitor.toString();
	}
	
	@Override
	public void visit(Limit obj) {
		if (obj != null){
			buffer.append(LIMIT).append(Tokens.SPACE).append(obj.getRowLimit());
		}
	}
	
	@Override
	public void visit(Like obj) {
		if (obj != null){
			if (obj.getLeftExpression() instanceof ColumnReference){
				ColumnReference cr = (ColumnReference) obj.getLeftExpression();
				buffer.append(cr.getName()).append(Tokens.SPACE);
			}
			buffer.append(LIKE).append(Tokens.SPACE).append(obj.getRightExpression()).append(Tokens.SPACE);
		}
	}
	
	@Override
	public void visit(Comparison obj) {
		if (obj.getOperator().equals(Operator.NE)){
			Comparison c = new Comparison(obj.getLeftExpression(), obj.getRightExpression(), Operator.EQ);
			append(new Not(c));
		}else{
			if (obj.getLeftExpression() instanceof ColumnReference){
				ColumnReference left = (ColumnReference) obj.getLeftExpression();
				buffer.append(left.getName());
			}else{
				buffer.append(obj.getLeftExpression().toString());
			}
			buffer.append(Tokens.SPACE).append(obj.getOperator().toString()).append(Tokens.SPACE);
			if(obj.getRightExpression() instanceof ColumnReference){
				ColumnReference right = (ColumnReference) obj.getRightExpression();
				buffer.append(right.getName());
			}else{
				buffer.append(obj.getRightExpression().toString());
			}
			buffer.append(Tokens.SPACE);
		}
	}
}
