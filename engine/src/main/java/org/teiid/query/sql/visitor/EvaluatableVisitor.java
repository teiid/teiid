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
package org.teiid.query.sql.visitor;

import java.util.TreeSet;

import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;


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
	private Determinism determinismLevel = Determinism.DETERMINISTIC;
	private boolean hasCorrelatedReferences;
	    
    public void visit(Function obj) {
        this.setDeterminismLevel(obj.getFunctionDescriptor().getDeterministic());
        if (obj.getFunctionDescriptor().getPushdown() == PushDown.MUST_PUSHDOWN || obj.getFunctionDescriptor().getDeterministic() == Determinism.NONDETERMINISTIC) {
            evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
        } else if (obj.getName().equalsIgnoreCase(FunctionLibrary.LOOKUP)
        		//TODO: if we had the context here we could plan better for non-prepared requests
        		|| obj.getFunctionDescriptor().getDeterministic().compareTo(Determinism.COMMAND_DETERMINISTIC) <= 0) {
            evaluationNotPossible(EvaluationLevel.PROCESSING);
        }
    }
    
    @Override
    public void visit(Constant obj) {
    	if (obj.isMultiValued()) {
            evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    	}
    }
    
    private void setDeterminismLevel(Determinism value) {
    	if (determinismLevel == null || value.compareTo(determinismLevel) < 0) {
    		determinismLevel = value;
    	}
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
		for (SPParameter param : proc.getInputParameters()) {
			if (!(param.getExpression() instanceof Constant)) {
				evaluationNotPossible(EvaluationLevel.PROCESSING);
			}
		}
    }
    
    public void visit(ScalarSubquery obj){
    	if (obj.shouldEvaluate()) {
    		evaluationNotPossible(EvaluationLevel.PROCESSING);
    	} else {
    		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    	}
    }
    
    public void visit(DependentSetCriteria obj) {
		evaluationNotPossible(EvaluationLevel.PROCESSING);
    }
    
    public void visit(ExistsCriteria obj) {
    	if (obj.shouldEvaluate()) {
    		evaluationNotPossible(EvaluationLevel.PROCESSING);
    	} else {
    		evaluationNotPossible(EvaluationLevel.PUSH_DOWN);
    	}
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
    
    /**
	 *  Will return true if the expression can be deterministically evaluated at runtime, but it may not be
	 *  evaluatable during planning
	 */
	public static final boolean willBecomeConstant(LanguageObject obj) {
	    return willBecomeConstant(obj, false);
	}

	/**
	 *  Should be called to check if the object can fully evaluated
	 */
	public static final boolean isFullyEvaluatable(LanguageObject obj, boolean duringPlanning) {
	    return isEvaluatable(obj, duringPlanning?EvaluationLevel.PLANNING:EvaluationLevel.PROCESSING);
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
        if (pushdown && (visitor.hasCorrelatedReferences || visitor.determinismLevel == Determinism.NONDETERMINISTIC)) {
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
