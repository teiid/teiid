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

import java.util.Collection;

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p>This visitor class will traverse a language object tree and collect all expression 
 * element symbol references it finds.  It uses a collection to collect the symbols in so
 * different collections will give you different collection properties - for instance,
 * using a Set will remove duplicates.</p>
 *
 * <p>The easiest way to use this visitor is to call the static methods which create
 * the visitor (and possibly the collection), run the visitor, and return the collection.
 * The public visit() methods should NOT be called directly.</p>
 */
public class ExpressionSymbolCollector extends LanguageVisitor {
    private Collection symbols;

    /**
     * Construct a new visitor with the specified collection, which should
     * be non-null.
     * @param elements Collection to use for elements
     * @throws IllegalArgumentException If elements is null
     */
	public ExpressionSymbolCollector(Collection elements) {
        if(elements == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0021));
        }
        this.symbols = elements;
    }

    /**
     * Get the elements collected by the visitor.  This should best be called
     * after the visitor has been run on the language object tree.
     * @return Collection of {@link com.metamatrix.query.sql.symbol.ElementSymbol}
     */
    public Collection getSymbols() {
        return this.symbols;
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(ExpressionSymbol obj) {
        this.symbols.add(obj);
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(AggregateSymbol obj) {
        this.symbols.add(obj);
    }
    
    /**
     * Helper to quickly get the elements from obj in the elements collection
     * @param obj Language object
     * @param elements Collection to collect elements in
     */
    public static final void getSymbols(LanguageObject obj, Collection elements) {
    	if(obj == null) {
    		return;
    	}
    	ExpressionSymbolCollector visitor = new ExpressionSymbolCollector(elements);
        PreOrderNavigator.doVisit(obj, visitor);
    }
}
