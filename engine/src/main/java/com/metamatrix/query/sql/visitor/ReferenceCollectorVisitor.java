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
import java.util.List;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;
import com.metamatrix.query.sql.symbol.Reference;

/**
 * <p>This visitor class will traverse a language object tree and collect all
 * references it finds.  </p>
 *
 * <p>The easiest way to use this visitor is to call the static methods which create
 * the visitor (and possibly the collection), run the visitor, and return the collection.
 * The public visit() methods should NOT be called directly.</p>
 */
public class ReferenceCollectorVisitor extends LanguageVisitor {

    private List<Reference> references = new ArrayList<Reference>();

    /**
     * Get the references collected by the visitor.  This should best be called
     * after the visitor has been run on the language object tree.
     * @return Collection of {@link com.metamatrix.query.sql.symbol.ElementSymbol}
     */
    public List<Reference> getReferences() {
        return this.references;
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(Reference obj) {
        this.references.add(obj);
    }

	/**
     * Helper to quickly get the references from obj in a collection.
     * @param obj Language object
     * @return List of {@link com.metamatrix.query.sql.symbol.Reference}
     */
    public static List<Reference> getReferences(LanguageObject obj) {
    	ReferenceCollectorVisitor visitor = new ReferenceCollectorVisitor();
        DeepPreOrderNavigator.doVisit(obj, visitor);

        return visitor.getReferences();
    }
    
}

