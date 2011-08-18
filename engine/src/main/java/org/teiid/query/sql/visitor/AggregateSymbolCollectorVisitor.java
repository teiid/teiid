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
import java.util.Collections;
import java.util.LinkedHashSet;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.WindowFunction;


public class AggregateSymbolCollectorVisitor extends LanguageVisitor {
    
    public static class AggregateStopNavigator extends PreOrPostOrderNavigator {
    	
    	private Collection<? extends Expression> groupingCols;
    	private Collection<? super Expression> groupingColsUsed;
    	
        public AggregateStopNavigator(LanguageVisitor visitor, Collection<? super Expression> groupingColsUsed, Collection<? extends Expression> groupingCols) {
            super(visitor, PreOrPostOrderNavigator.PRE_ORDER, false);
            this.groupingCols = groupingCols;
            this.groupingColsUsed = groupingColsUsed;
        }
        
        public void visit(AggregateSymbol obj) {
            // Visit aggregate symbol but do not dive into it's expression
            preVisitVisitor(obj);
            postVisitVisitor(obj);
        }
        
        @Override
        protected void visitNode(LanguageObject obj) {
        	if (groupingCols != null && obj instanceof Expression && groupingCols.contains(obj)) {
        		if (groupingColsUsed != null) {
        			groupingColsUsed.add((Expression)obj);
        		}
        		return;
        	}
        	super.visitNode(obj);
        }
        
    }

    private Collection<? super AggregateSymbol> aggregates;
    private Collection<? super ElementSymbol> otherElements;
    private Collection<? super WindowFunction> windowFunctions;
    
	public AggregateSymbolCollectorVisitor(Collection<? super AggregateSymbol> aggregates, Collection<? super ElementSymbol> elements) { 
        this.aggregates = aggregates;
        this.otherElements = elements;
	}	
    
    public void visit(AggregateSymbol obj) {
        if (aggregates != null && !obj.isWindowed()) {
            this.aggregates.add(obj);
        }
    }
    
    public void visit(WindowFunction windowFunction) {
    	if (this.windowFunctions != null) {
    		this.windowFunctions.add(windowFunction);
    	}
    }
    
    public void visit(ElementSymbol obj) {
        if (this.otherElements != null && !obj.isExternalReference()) {
            this.otherElements.add(obj);  
        }
    }

    public static final void getAggregates(LanguageObject obj, 
    		Collection<? super AggregateSymbol> aggregates, 
    		Collection<? super ElementSymbol> otherElements, 
    		Collection<? super Expression> groupingColsUsed, 
    		Collection<? super WindowFunction> windowFunctions, 
    		Collection<? extends Expression> groupingCols) {
        AggregateSymbolCollectorVisitor visitor = new AggregateSymbolCollectorVisitor(aggregates, otherElements);
        visitor.windowFunctions = windowFunctions;
        AggregateStopNavigator asn = new AggregateStopNavigator(visitor, groupingColsUsed, groupingCols);
        asn.visitNode(obj);
    }

    public static final Collection<AggregateSymbol> getAggregates(LanguageObject obj, boolean removeDuplicates) {
    	if (obj == null) {
    		return Collections.emptyList();
    	}
        Collection<AggregateSymbol> aggregates = null;
        if (removeDuplicates) {
            aggregates = new LinkedHashSet<AggregateSymbol>();
        } else {
            aggregates = new ArrayList<AggregateSymbol>();    
        }
        AggregateSymbolCollectorVisitor visitor = new AggregateSymbolCollectorVisitor(aggregates, null);
        AggregateStopNavigator asn = new AggregateStopNavigator(visitor, null, null);
        obj.acceptVisitor(asn);
        return aggregates;
    }
        
}
