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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p>This visitor class will traverse a language object tree and collect all variable
 *  symbol  references it finds.  It uses a collection to collect the elements in so
 * different collections will give you different collection properties - for instance,
 * using a Set will remove duplicates.</p>
 *
 * <p>The easiest way to use this visitor is to call the static methods which create
 * the visitor (and possibly the collection), run the visitor, and return the collection.
 * The public visit() methods should NOT be called directly.</p>
 */
public class VariableCollectorVisitor extends LanguageVisitor {

    private Collection variables;

    /**
     * Construct a new visitor with the specified collection, which should
     * be non-null.
     * @param variables Collection to use for variables
     * @throws IllegalArgumentException If elements is null
     */
    public VariableCollectorVisitor(Collection variables) {
        if(variables == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0028));
        }
        this.variables = variables;
    }


    /**
     * Get the variables collected by the visitor.  This should best be called
     * after the visitor has been run on the language object tree.
     * @return Collection of {@link com.metamatrix.query.sql.symbol.ElementSymbol}
     */
    public Collection getVariables() {
        return this.variables;
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(ElementSymbol obj) {
        if(obj.isExternalReference()) {
            this.variables.add(obj);
        }
    }

    /**
     * Helper to quickly get the variables from obj in the variables collection
     * @param obj Language object
     * @param variables Collection to collect variables in
     */
    public static final void getVariables(LanguageObject obj, Collection variables) {
        VariableCollectorVisitor visitor = new VariableCollectorVisitor(variables);
        PreOrderNavigator.doVisit(obj, visitor);
    }

    /**
     * Helper to quickly get the variables from obj in a collection.  The
     * removeDuplicates flag affects whether duplicate variables will be
     * filtered out.
     * @param obj Language object
     * @param removeDuplicates True to remove duplicates
     * @return Collection of {@link com.metamatrix.query.sql.symbol.ElementSymbol}
     */
    public static final Collection getVariables(LanguageObject obj, boolean removeDuplicates) {
        Collection variables = null;
        if(removeDuplicates) {
            variables = new HashSet();
        } else {
            variables = new ArrayList();
        }
        VariableCollectorVisitor visitor = new VariableCollectorVisitor(variables);
        PreOrderNavigator.doVisit(obj, visitor);
        return variables;
    }

}
