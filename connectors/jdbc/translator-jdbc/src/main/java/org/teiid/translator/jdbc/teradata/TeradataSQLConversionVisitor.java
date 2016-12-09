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
package org.teiid.translator.jdbc.teradata;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.AndOr;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.AndOr.Operator;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class TeradataSQLConversionVisitor extends SQLConversionVisitor {

	public TeradataSQLConversionVisitor(TeradataExecutionFactory ef) {
		super(ef);
	}

    @Override
    public void visit(In obj) {
    	List<Expression> exprs = obj.getRightExpressions();
    	
    	boolean decompose = false;
    	for (Expression expr:exprs) {
    		if (!(expr instanceof Literal)) {
    			decompose = true;
    			break;
    		}
    	}
    	
    	if (decompose) {
    		List<Expression> literals = new ArrayList<Expression>();
    		Comparison.Operator opCode = obj.isNegated()?Comparison.Operator.NE:Comparison.Operator.EQ;
	    	if (exprs.size() > 1) {
	    		Condition left = null;
	    		for (Expression expr : obj.getRightExpressions()) {
	    			if (expr instanceof Literal) {
	    				literals.add(expr);
	    			} else {
	    				if (left == null) {
	    					left = LanguageFactory.INSTANCE.createCompareCriteria(opCode, obj.getLeftExpression(), expr);
	    				} else {
	    		    		left = LanguageFactory.INSTANCE.createAndOr(obj.isNegated()?Operator.AND:Operator.OR, left, LanguageFactory.INSTANCE.createCompareCriteria(opCode, obj.getLeftExpression(), expr));
	    				}
	    			}
	    		}
	    		if (!literals.isEmpty()) {
	    			left = LanguageFactory.INSTANCE.createAndOr(obj.isNegated()?Operator.AND:Operator.OR, left, new In(obj.getLeftExpression(), literals, obj.isNegated()));
	    		}
		    	buffer.append(Tokens.LPAREN);
		    	super.visit((AndOr)left);
	    		buffer.append(Tokens.RPAREN);
	    	}
	    	else {
	    		super.visit(LanguageFactory.INSTANCE.createCompareCriteria(opCode, obj.getLeftExpression(), exprs.get(0)));
	    	}
    	}
    	else {
    		super.visit(obj);
    	}
    }	
}
