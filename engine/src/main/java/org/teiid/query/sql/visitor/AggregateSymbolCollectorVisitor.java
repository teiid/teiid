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
import java.util.HashSet;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;


public class AggregateSymbolCollectorVisitor extends LanguageVisitor {
    
    public static class AggregateStopNavigator extends PreOrPostOrderNavigator {
        
        public AggregateStopNavigator(LanguageVisitor visitor) {
            super(visitor, PreOrPostOrderNavigator.POST_ORDER, false);
        }
        
        public void visit(AggregateSymbol obj) {
            // Visit aggregate symbol but do not dive into it's expression
            preVisitVisitor(obj);
            postVisitVisitor(obj);
        }
        
        /** 
         * @see org.teiid.query.sql.navigator.PreOrPostOrderNavigator#visit(org.teiid.query.sql.symbol.ExpressionSymbol)
         */
        @Override
        public void visit(ExpressionSymbol obj) {
            if (obj.isDerivedExpression()) {
                preVisitVisitor(obj);
                postVisitVisitor(obj);
            } else {
                super.visit(obj);
            }
        }
         
    }

    private Collection<AggregateSymbol> aggregates;
    private Collection<SingleElementSymbol> groupingSymbols;
    
	public AggregateSymbolCollectorVisitor(Collection<AggregateSymbol> aggregates, Collection<SingleElementSymbol> elements) { 
        this.aggregates = aggregates;
        this.groupingSymbols = elements;
	}	
    
    public void visit(AggregateSymbol obj) {
        if (aggregates != null) {
            this.aggregates.add(obj);
        }
    }
    
    public void visit(ExpressionSymbol obj) {
        if (this.groupingSymbols != null && obj.isDerivedExpression()) {
            this.groupingSymbols.add(obj);     
        }
    }

    public void visit(ElementSymbol obj) {
        if (this.groupingSymbols != null) {
            this.groupingSymbols.add(obj);  
        }
    }

    public static final void getAggregates(LanguageObject obj, Collection<SingleElementSymbol> aggregates, Collection<SingleElementSymbol> elements) {
        AggregateSymbolCollectorVisitor visitor = new AggregateSymbolCollectorVisitor(new ArrayList<AggregateSymbol>(), elements);
        AggregateStopNavigator asn = new AggregateStopNavigator(visitor);
        obj.acceptVisitor(asn);
        aggregates.addAll(visitor.aggregates);
    }

    public static final Collection<AggregateSymbol> getAggregates(LanguageObject obj, boolean removeDuplicates) {
    	if (obj == null) {
    		return Collections.emptyList();
    	}
        Collection<AggregateSymbol> aggregates = null;
        if (removeDuplicates) {
            aggregates = new HashSet<AggregateSymbol>();
        } else {
            aggregates = new ArrayList<AggregateSymbol>();    
        }
        AggregateSymbolCollectorVisitor visitor = new AggregateSymbolCollectorVisitor(aggregates, null);
        AggregateStopNavigator asn = new AggregateStopNavigator(visitor);
        obj.acceptVisitor(asn);
        return aggregates;
    }
        
}
