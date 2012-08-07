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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;


/**
 * <p>This visitor class will traverse a language object tree and collect references that
 * correspond to correlated subquery references.</p>
 * 
 * <p>The easiest way to use this visitor is to call the static method which creates the 
 * the visitor by passing it the Language Object and the variable context to be looked up.
 * The public visit() methods should NOT be called directly.</p>
 */
public class CorrelatedReferenceCollectorVisitor extends LanguageVisitor {

	// index of the reference on the language object    
    private Collection<GroupSymbol> groupSymbols;
    private List<Reference> references;

    public CorrelatedReferenceCollectorVisitor(Collection<GroupSymbol> groupSymbols, List<Reference> correlatedReferences) {
        this.groupSymbols = groupSymbols;
        this.references = correlatedReferences;
    }

    public List<Reference> getReferences(){
        return this.references;
    }

	// ############### Visitor methods for language objects ##################    

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be 
     * called directly.
     * @param obj Language object
     */
    public void visit(Reference obj) {
        ElementSymbol e = obj.getExpression();
        if (e == null) {
        	return;
        }
        GroupSymbol g = e.getGroupSymbol();
        if (this.groupSymbols.contains(g) && e.isExternalReference()){
            this.references.add(obj);
        }
    }

    /**
     * <p>Helper to use this visitor.</p>
     * @param obj The Language object that is to be visited
     * @param groupSymbols Collection of GroupSymbols to restrict collection to - these are the groups
     * that the client (outer query) is interested in references to from the correlated subquery
     * @param correlatedReferences List of References collected
     */
    public static final void collectReferences(final LanguageObject obj, final Collection<GroupSymbol> groupSymbols, List<Reference> correlatedReferences){
    	final Set<GroupSymbol> groups = new HashSet<GroupSymbol>(groupSymbols);
        CorrelatedReferenceCollectorVisitor visitor =
            new CorrelatedReferenceCollectorVisitor(groups, correlatedReferences);
        obj.acceptVisitor(new PreOrPostOrderNavigator(visitor, PreOrPostOrderNavigator.PRE_ORDER, true) {

        	@Override
        	public void visit(Query query) {
        		//don't allow confusion with deep nesting by removing intermediate groups
        		List<GroupSymbol> fromGroups = null;
				if (query != obj && query.getFrom() != null) {
					fromGroups = query.getFrom().getGroups();
        			if (!groups.removeAll(fromGroups)) {
        				fromGroups = null;
        			}
        		}
    			super.visit(query);
    			if (fromGroups != null) {
    				fromGroups.retainAll(groupSymbols);
    				groups.addAll(fromGroups);
    			}
        	}
        });
    }

}
