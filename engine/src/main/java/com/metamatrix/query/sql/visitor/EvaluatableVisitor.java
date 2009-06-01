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

/**
 * 
 */
package com.metamatrix.query.sql.visitor;

import java.util.TreeSet;

import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;

/**
 * <p>This visitor class will traverse a language object tree, and determine
 * if the current expression can be evaluated</p>
 */
public class EvaluatableVisitor extends LanguageVisitor {
	
	public enum EvaluationLevel {
		PLANNING,
		PROCESSING,
		PUSH_DOWN,
	}

	private TreeSet<EvaluationLevel> levels = new TreeSet<EvaluationLevel>();
	private EvaluationLevel targetLevel;
	private int determinismLevel;
	private boolean hasCorrelatedReferences;
	    
    public void visit(Function obj) {
        this.setDeterminismLevel(obj.getFunctionDescriptor().getDeterministic());
        if (obj.getFunctionDescriptor().getPushdown() == FunctionMethod.MUST_PUSHDOWN) {
            evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
        } else if (obj.getName().equalsIgnoreCase(FunctionLibrary.LOOKUP)
        		//TODO: if we had the context here we could plan better for non-prepared requests
        		|| obj.getFunctionDescriptor().getDeterministic() >= FunctionMethod.COMMAND_DETERMINISTIC) {
            evaluationNotPossible(EvaluationLevel.PROCESSING);
        }
    }
    
    @Override
    public void visit(Constant obj) {
    	if (obj.isMultiValued()) {
            evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    	}
    }
    
    private void setDeterminismLevel(int value) {
    	determinismLevel = Math.max(determinismLevel, value);
    }
    
    private void evaluationNotPossible(EvaluationLevel newLevel) {
    	levels.add(newLevel);
    	EvaluationLevel level = levels.last();
    	if (targetLevel != null && level.compareTo(targetLevel) > 0) {
    		setAbort(true);
    	}
    }
        
    public void visit(ElementSymbol obj) {
    	//if the element is a variable, or an element that will have a value, it will be evaluatable at runtime
		//begin hack for not having the metadata passed in
		if (obj.getGroupSymbol().getMetadataID() instanceof TempMetadataID) {
			TempMetadataID tid = (TempMetadataID)obj.getGroupSymbol().getMetadataID();
			if (tid.isScalarGroup()) {
				evaluationNotPossible(EvaluationLevel.PROCESSING);
				return;
			}
		}
		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    }
    
    public void visit(ExpressionSymbol obj) {
		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    }
    
    public void visit(AggregateSymbol obj) {
		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    }
    
    /**
     * We assume the non-push down for correlation variables,
     * then make specific checks when correlated variables are allowed.
     */
    public void visit(Reference obj) {
        hasCorrelatedReferences |= obj.isCorrelated();
    	evaluationNotPossible(EvaluationLevel.PROCESSING);
    }
    
    public void visit(StoredProcedure proc){
		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    }
    
    public void visit(ScalarSubquery obj){
		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    }
    
    public void visit(DependentSetCriteria obj) {
		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    }
    
    public void visit(ExistsCriteria obj) {
		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    }        

    public void visit(SubquerySetCriteria obj) {
		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    }        

    public void visit(SubqueryCompareCriteria obj) {
		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    }
    
    private boolean isEvaluationPossible() {
    	if (levels.isEmpty()) {
    		return true;
    	}
    	return levels.last().compareTo(targetLevel) <= 0;
    }
    
    public static final boolean isEvaluatable(LanguageObject obj, EvaluationLevel target) {
        EvaluatableVisitor visitor = new EvaluatableVisitor();
        visitor.targetLevel = target;
        PreOrderNavigator.doVisit(obj, visitor);
        return visitor.isEvaluationPossible();
    }
    
    public static final boolean willBecomeConstant(LanguageObject obj, boolean pushdown) {
        EvaluatableVisitor visitor = new EvaluatableVisitor();
        visitor.targetLevel = EvaluationLevel.PROCESSING;
        PreOrderNavigator.doVisit(obj, visitor);
        if (pushdown && (visitor.hasCorrelatedReferences || visitor.determinismLevel >= FunctionMethod.NONDETERMINISTIC)) {
        	return false;
        }
        return visitor.isEvaluationPossible();
    }
    
    public static final boolean needsProcessingEvaluation(LanguageObject obj) {
        EvaluatableVisitor visitor = new EvaluatableVisitor();
        DeepPreOrderNavigator.doVisit(obj, visitor);
        return visitor.levels.contains(EvaluationLevel.PROCESSING);
    }
}
