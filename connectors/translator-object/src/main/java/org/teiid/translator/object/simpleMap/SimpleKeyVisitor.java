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
package org.teiid.translator.object.simpleMap;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.AndOr;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectVisitor;
import org.teiid.translator.object.util.ObjectUtil;

/**
 * @author vanhalbert
 *
 */
public class SimpleKeyVisitor extends ObjectVisitor {
		
	private ArrayList<Object> values = new ArrayList<Object>();
	
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

}
