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

package com.metamatrix.query.optimizer.xml;

import java.util.List;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.visitor.ExpressionMappingVisitor;

/**
 * <p>This visitor class will traverse a language object tree, find any Reference
 * objects, and replace them with the corresponding parsed Expression which is the
 * Reference's binding.  The List of parsed bindings must contain Expressions, and
 * of course the number and order of the bindings must match up with the number
 * and order of the References that are in the Language Object.</p>
 * 
 * <p>The easiest way to use this visitor is to call the static methods which create 
 * the visitor and run it.
 * The public visit() methods should NOT be called directly.</p>
 */
public class ReferenceBindingReplacerVisitor extends ExpressionMappingVisitor {

    private List parsedBindingExpressions;

    /**
     * Construct a new visitor with the default collection type, which is a 
     * {@link java.util.HashSet}.  
     */
    public ReferenceBindingReplacerVisitor(List parsedBindingExpressions) { 
        super(null);
        this.parsedBindingExpressions = parsedBindingExpressions;
    }
    
    public Expression replaceExpression(Expression element) {
        if (!(element instanceof Reference)) {
            return element;
        }
        
        Reference reference = (Reference)element;
        
        return (Expression)parsedBindingExpressions.get(reference.getIndex());
    }
    
    /**
     * Helper to quickly get the references from obj in the references collection
     * @param obj Language object
     * @param elements Collection to collect references in
     */
    public static final void replaceReferences(LanguageObject obj, List parsedBindingExpressions) {
        if (parsedBindingExpressions == null || parsedBindingExpressions.isEmpty()) {
            return;
        }
        
        ReferenceBindingReplacerVisitor visitor = new ReferenceBindingReplacerVisitor(parsedBindingExpressions);
        DeepPreOrderNavigator.doVisit(obj, visitor);
    }

}

