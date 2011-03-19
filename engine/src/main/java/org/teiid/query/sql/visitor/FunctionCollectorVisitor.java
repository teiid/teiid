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

package org.teiid.query.sql.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.symbol.Function;


/**
 * <p>This visitor class will traverse a language object tree and collect all Function
 * references it finds.  It uses a collection to collect the Functions in so
 * different collections will give you different collection properties - for instance,
 * using a Set will remove duplicates.</p>
 * 
 * <p>This visitor can optionally collect functions of only a specific name</p>
 *
 * <p>The easiest way to use this visitor is to call the static methods which create
 * the visitor (and possibly the collection), run the visitor, and return the collection.
 * The public visit() methods should NOT be called directly.</p>
 */
public class FunctionCollectorVisitor extends LanguageVisitor {

    private Collection<Function> functions;
    
    private String functionName;

    /**
     * Construct a new visitor with the specified collection, which should
     * be non-null.
     * @param elements Collection to use for elements
     * @throws IllegalArgumentException If elements is null
     */
	public FunctionCollectorVisitor(Collection<Function> functions) {
        this(functions, null);
	}

    /**
     * Construct a new visitor with the specified collection, which should
     * be non-null.
     * @param elements Collection to use for elements
     * @throws IllegalArgumentException If elements is null
     */
    public FunctionCollectorVisitor(Collection<Function> functions, String functionName) {
        if(functions == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0022")); //$NON-NLS-1$
        }
        this.functions = functions;
        this.functionName = functionName;
    }    
    
    /**
     * Get the elements collected by the visitor.  This should best be called
     * after the visitor has been run on the language object tree.
     * @return Collection of {@link org.teiid.query.sql.symbol.ElementSymbol}
     */
    public Collection<Function> getFunctions() {
        return this.functions;
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(Function obj) {
        if (this.functionName == null || obj.getName().equalsIgnoreCase(this.functionName)) {
            this.functions.add(obj);
        }
    }

    /**
     * Helper to quickly get the elements from obj in the elements collection
     * @param obj Language object
     * @param elements Collection to collect elements in
     */
    public static final void getFunctions(LanguageObject obj, Collection<Function> functions) {
        getFunctions(obj, functions, false);
    }
    
    /**
     * Helper to quickly get the elements from obj in the elements collection
     * @param obj Language object
     * @param elements Collection to collect elements in
     */
    public static final void getFunctions(LanguageObject obj, Collection<Function> functions, boolean deep) {
        FunctionCollectorVisitor visitor = new FunctionCollectorVisitor(functions);
        if (!deep) {
            PreOrderNavigator.doVisit(obj, visitor);
        } else {
            DeepPreOrderNavigator.doVisit(obj, visitor);
        }
    }

    /**
     * Helper to quickly get the elements from obj in a collection.  The
     * removeDuplicates flag affects whether duplicate elements will be
     * filtered out.
     * @param obj Language object
     * @param removeDuplicates True to remove duplicates
     * @return Collection of {@link org.teiid.query.sql.symbol.ElementSymbol}
     */
    public static final Collection<Function> getFunctions(LanguageObject obj, boolean removeDuplicates) {
        return getFunctions(obj, removeDuplicates, false);
    }

    public static final Collection<Function> getFunctions(LanguageObject obj, boolean removeDuplicates, boolean deep) {
        Collection<Function> functions = null;
        if(removeDuplicates) {
            functions = new HashSet<Function>();
        } else {
            functions = new ArrayList<Function>();
        }
        getFunctions(obj, functions, deep);
        return functions;
    }
    
	public static boolean isNonDeterministic(LanguageObject ex) {
		Collection<Function> functions = FunctionCollectorVisitor.getFunctions(ex, true, false);
		for (Function function : functions) {
			if ( function.getFunctionDescriptor().getDeterministic() == Determinism.NONDETERMINISTIC) {
				return true;
			}
		}
		return false;
	}

}
