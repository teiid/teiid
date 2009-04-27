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
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.ScalarSubquery;

/**
 * <p>This visitor class will traverse a language object tree and collect all language
 * objects that implement {@link com.metamatrix.sql.util.ValueIteratorProvider.  These
 * all currently also implement {@link com.metamatrix.sql.lang.SubqueryContainer}.  
 * By default it uses a java.util.ArrayList to collect the objects in the order 
 * they're found.</p>
 * 
 * <p>The easiest way to use this visitor is to call one of the static methods which create 
 * the visitor, run the visitor, and get the collection. 
 * The public visit() methods should NOT be called directly.</p>
 */
public class ValueIteratorProviderCollectorVisitor extends LanguageVisitor {

    private List<SubqueryContainer> valueIteratorProviders;
    
    /**
     * Construct a new visitor with the default collection type, which is a 
     * {@link java.util.ArrayList}.  
     */
    public ValueIteratorProviderCollectorVisitor() { 
        this.valueIteratorProviders = new ArrayList<SubqueryContainer>();
    }   

	/**
	 * Construct a new visitor with the given Collection to accumulate
     * ValueIteratorProvider instances
	 * @param valueIteratorProviders Collection to accumulate found 
	 */
	ValueIteratorProviderCollectorVisitor(List<SubqueryContainer> valueIteratorProviders) { 
		this.valueIteratorProviders = valueIteratorProviders;
	}   
    
    /**
     * Get the value iterator providers collected by the visitor.  This should best be called 
     * after the visitor has been run on the language object tree.
     * @return Collection of {@link com.metamatrix.sql.util.ValueIteratorProvider}
     * (by default, this is a java.util.ArrayList)
     */
    public List<SubqueryContainer> getValueIteratorProviders() { 
        return this.valueIteratorProviders;
    }
    
    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(SubquerySetCriteria obj) {
        this.valueIteratorProviders.add(obj);
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(SubqueryCompareCriteria obj) {
        this.valueIteratorProviders.add(obj);
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(ExistsCriteria obj) {
        this.valueIteratorProviders.add(obj);
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(ScalarSubquery obj) {
        this.valueIteratorProviders.add(obj);
    }

    /**
     * Helper to quickly get the ValueIteratorProvider instances from obj
     * @param obj Language object
     * @return java.util.ArrayList of found ValueIteratorProvider
     */
    public static final List<SubqueryContainer> getValueIteratorProviders(LanguageObject obj) {
        ValueIteratorProviderCollectorVisitor visitor = new ValueIteratorProviderCollectorVisitor();
        PreOrderNavigator.doVisit(obj, visitor);
        return visitor.getValueIteratorProviders();
    }

	public static final void getValueIteratorProviders(LanguageObject obj, List<SubqueryContainer> valueIteratorProviders) {
		ValueIteratorProviderCollectorVisitor visitor = new ValueIteratorProviderCollectorVisitor(valueIteratorProviders);
        PreOrderNavigator.doVisit(obj, visitor);
	}
          	
    public static final List<SubqueryContainer> getValueIteratorProviders(Collection<? extends LanguageObject> languageObjects) {
    	if (languageObjects == null || languageObjects.isEmpty()) {
    		return Collections.emptyList();
    	}
    	List<SubqueryContainer> result = new LinkedList<SubqueryContainer>();
        ValueIteratorProviderCollectorVisitor visitor = new ValueIteratorProviderCollectorVisitor(result);
        for (LanguageObject obj : languageObjects) {
            PreOrderNavigator.doVisit(obj, visitor);
        }
        return result;
    }            
}
