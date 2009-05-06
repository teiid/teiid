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
import java.util.Collections;
import java.util.HashSet;

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.AllInGroupSymbol;
import com.metamatrix.query.sql.symbol.AllSymbol;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p>This visitor class will traverse a language object tree and collect all element
 * symbol references it finds.  It uses a collection to collect the elements in so
 * different collections will give you different collection properties - for instance,
 * using a Set will remove duplicates.</p>
 *
 * <p>The easiest way to use this visitor is to call the static methods which create
 * the visitor (and possibly the collection), run the visitor, and return the collection.
 * The public visit() methods should NOT be called directly.</p>
 */
public class ElementCollectorVisitor extends LanguageVisitor {

    private Collection<ElementSymbol> elements;

    /**
     * Construct a new visitor with the specified collection, which should
     * be non-null.
     * @param elements Collection to use for elements
     * @throws IllegalArgumentException If elements is null
     */
	public ElementCollectorVisitor(Collection<ElementSymbol> elements) {
        if(elements == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0021));
        }
        this.elements = elements;
    }

    /**
     * Get the elements collected by the visitor.  This should best be called
     * after the visitor has been run on the language object tree.
     * @return Collection of {@link com.metamatrix.query.sql.symbol.ElementSymbol}
     */
    public Collection<ElementSymbol> getElements() {
        return this.elements;
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(ElementSymbol obj) {
        this.elements.add(obj);
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(AllInGroupSymbol obj) {
        if(obj.getElementSymbols() != null) {
	        this.elements.addAll(obj.getElementSymbols());
        }
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(AllSymbol obj) {
        if(obj.getElementSymbols() != null) {
	        this.elements.addAll(obj.getElementSymbols());
        }
    }

    /**
     * Helper to quickly get the elements from obj in the elements collection
     * @param obj Language object
     * @param elements Collection to collect elements in
     */
    public static final void getElements(LanguageObject obj, Collection<ElementSymbol> elements) {
    	if(obj == null) {
    		return;
    	}
        ElementCollectorVisitor visitor = new ElementCollectorVisitor(elements);
        PreOrderNavigator.doVisit(obj, visitor);
    }
    
    public static final void getElements(Collection<? extends LanguageObject> objs, Collection<ElementSymbol> elements) {
    	if(objs == null) {
    		return;
    	}
        ElementCollectorVisitor visitor = new ElementCollectorVisitor(elements);
        for (LanguageObject object : objs) {
            PreOrderNavigator.doVisit(object, visitor);
		}
    }

    /**
     * Helper to quickly get the elements from obj in a collection.  The
     * removeDuplicates flag affects whether duplicate elements will be
     * filtered out.
     * @param obj Language object
     * @param removeDuplicates True to remove duplicates
     * @return Collection of {@link com.metamatrix.query.sql.symbol.ElementSymbol}
     */
    public static final Collection<ElementSymbol> getElements(LanguageObject obj, boolean removeDuplicates) {
        return ElementCollectorVisitor.getElements(obj, removeDuplicates, false);
    }

    /**
     * Helper to quickly get the elements from obj in a collection.  The
     * removeDuplicates flag affects whether duplicate elements will be
     * filtered out.
     * @param obj Language object
     * @param removeDuplicates True to remove duplicates
     * @param useDeepIteration indicates whether or not to iterate into nested
     * subqueries of the query 
     * @return Collection of {@link com.metamatrix.query.sql.symbol.ElementSymbol}
     */
    public static final Collection<ElementSymbol> getElements(LanguageObject obj, boolean removeDuplicates, boolean useDeepIteration) {
        if(obj == null) {
            return Collections.emptyList();
        }
        Collection<ElementSymbol> elements = null;
        if(removeDuplicates) {
            elements = new HashSet<ElementSymbol>();
        } else {
            elements = new ArrayList<ElementSymbol>();
        }
        ElementCollectorVisitor visitor = null;
        if (useDeepIteration){
            visitor = new ElementCollectorVisitor(elements);
            DeepPreOrderNavigator.doVisit(obj, visitor);
        } else {
            visitor = new ElementCollectorVisitor(elements);
            PreOrderNavigator.doVisit(obj, visitor);
        }
        
        return elements;
    }

}
