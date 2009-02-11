/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.Iterator;

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
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;

/**
 * <p>This visitor class will traverse a language object tree, and determine
 * if the current expression can be evaluated</p>
 * 
 * <p>The public visit() methods should NOT be called directly.</p>
 * 
 * There are three possible scenarios:
 * 
 * duringPlanning | fullyEvaluatable
 * ---------------------------------
 * false          | false            = will become processing time constant
 * false          | true             = processing time evaluation possible
 * true           | true             = planning time evaluation possible (should always be deterministic)
 * 
 */
public class EvaluatableVisitor extends LanguageVisitor {

    protected boolean evaluationPossible = true;
    private boolean duringPlanning = false;
    private boolean fullyEvaluatable = false;
    private boolean deterministic = false;
    
    public EvaluatableVisitor(boolean duringPlanning, boolean fullyEvaluatable) {
        this.duringPlanning = duringPlanning;
        this.fullyEvaluatable = fullyEvaluatable;
    }

    public void visit(Function obj) {
        if (obj.getFunctionDescriptor().getPushdown() == FunctionMethod.MUST_PUSHDOWN) {
            evaluationNotPossible();
        } else if (duringPlanning) {
            if (obj.getName().equalsIgnoreCase(FunctionLibrary.LOOKUP)) {
                evaluationNotPossible();
            } else if (obj.getFunctionDescriptor().getDeterministic() >= FunctionMethod.COMMAND_DETERMINISTIC) {
                evaluationNotPossible();
            }
        } else if (deterministic && obj.getFunctionDescriptor().getDeterministic() == FunctionMethod.NONDETERMINISTIC) {
            evaluationNotPossible();
        }
    }
    
    private boolean evaluationNotPossible() {
        evaluationPossible = false;
        setAbort(true);
        return evaluationPossible;
    }
        
    public void visit(ElementSymbol obj) {
    	//if the element is a variable, or an element that will have a value, it will be evaluatable at runtime
    	if (duringPlanning || fullyEvaluatable) {
    		evaluationNotPossible();
    	} else {
    		//begin hack for not having the metadata passed in
    		if (obj.getGroupSymbol().getMetadataID() instanceof TempMetadataID) {
    			TempMetadataID tid = (TempMetadataID)obj.getGroupSymbol().getMetadataID();
    			if (tid.isScalarGroup()) {
    				return;
    			}
    		}
    		evaluationNotPossible();
    	}
    }
    
    public void visit(ExpressionSymbol obj) {
        evaluationNotPossible();
    }
    
    public void visit(AggregateSymbol obj) {
        evaluationNotPossible();
    }
    
    public void visit(Reference obj) {
        if (duringPlanning) {
            evaluationNotPossible();
        } else if (fullyEvaluatable) {
            if (obj.getExpression() != null) {
                for (Iterator i = ReferenceCollectorVisitor.getReferences(obj.getExpression()).iterator(); i.hasNext();) {
                    PreOrderNavigator.doVisit((Reference)i.next(), this);  //follow reference
                }
                if (!ElementCollectorVisitor.getElements(obj.getExpression(), false).isEmpty() && obj.getTuple() == null) {
                    evaluationNotPossible();
                }
            }
        } else if (obj.isCorrelated()) {
            evaluationNotPossible();
        }
    }
    
    public void visit(StoredProcedure proc){
        evaluationNotPossible();  
    }
    
    public void visit(ScalarSubquery obj){
        evaluationNotPossible();  
    }
    
    public void visit(DependentSetCriteria obj) {
        evaluationNotPossible();
    }
    
    public void visit(ExistsCriteria obj) {
        evaluationNotPossible(); 
    }        

    public void visit(SubquerySetCriteria obj) {
        evaluationNotPossible(); 
    }        

    public void visit(SubqueryCompareCriteria obj) {
        evaluationNotPossible(); 
    }
    
    public boolean isEvaluationPossible() {
        return evaluationPossible;
    }
    
    static final boolean isEvaluatable(LanguageObject obj, boolean duringPlanning, boolean fullyEvaluatable, boolean deterministic) {
        EvaluatableVisitor visitor = new EvaluatableVisitor(duringPlanning, fullyEvaluatable);
        visitor.deterministic = deterministic;
        PreOrderNavigator.doVisit(obj, visitor);
        return visitor.isEvaluationPossible();
    }
}
