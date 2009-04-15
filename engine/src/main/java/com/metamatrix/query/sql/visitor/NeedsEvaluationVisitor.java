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

package com.metamatrix.query.sql.visitor;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.Reference;

/**
 * Checks the language object for expressions or criteria that need to be evaluated at execution time
 */
public class NeedsEvaluationVisitor extends LanguageVisitor {
    
    private boolean needsEvaluation = false;
    
    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.symbol.Reference)
     */
    @Override
    public void visit(Reference obj) {
        setNeedsEvaluation();
    }
    
    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.DependentSetCriteria)
     */
    @Override
    public void visit(DependentSetCriteria obj) {
        setNeedsEvaluation();
    }
    
    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.symbol.Function)
     */
    @Override
    public void visit(Function obj) {
        if (EvaluatableVisitor.isEvaluatable(obj, false, true, false, false)) {
            setNeedsEvaluation();
        }
    }

    public void setNeedsEvaluation() {
        this.needsEvaluation = true;
        setAbort(true);
    }
    
    public static boolean needsEvaluation(LanguageObject obj) {
        NeedsEvaluationVisitor visitor = new NeedsEvaluationVisitor();
        DeepPreOrderNavigator.doVisit(obj, visitor);
        return visitor.needsEvaluation;
    }
    
}
