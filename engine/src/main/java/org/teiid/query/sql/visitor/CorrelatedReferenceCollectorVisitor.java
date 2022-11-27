/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.sql.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;


/**
 * <p>This visitor class will traverse a language object tree and collect references that
 * correspond to correlated subquery references.
 *
 * <p>The easiest way to use this visitor is to call the static method which creates the
 * the visitor by passing it the Language Object and the variable context to be looked up.
 * The public visit() methods should NOT be called directly.
 */
public class CorrelatedReferenceCollectorVisitor extends LanguageVisitor {

    // index of the reference on the language object
    private List<Reference> references;
    private ArrayList<Map<GroupSymbol,GroupSymbol>> outerGroups = new ArrayList<Map<GroupSymbol,GroupSymbol>>(2);
    private QueryMetadataInterface metadata;
    private boolean queryRoot;

    CorrelatedReferenceCollectorVisitor(Collection<GroupSymbol> groupSymbols, List<Reference> correlatedReferences) {
        HashMap<GroupSymbol, GroupSymbol> groupMap = new HashMap<GroupSymbol, GroupSymbol>();
        for (GroupSymbol g : groupSymbols) {
            groupMap.put(g, g);
        }
        outerGroups.add(groupMap);
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
        if (e == null|| !e.isExternalReference()) {
            return;
        }
        GroupSymbol g = e.getGroupSymbol();
        for (int i = this.outerGroups.size() - (queryRoot?2:1); i >= 0; i--) {
            GroupSymbol outer = this.outerGroups.get(i).get(g);
            if (outer == null) {
                continue;
            }
            try {
                if (ResolverUtil.resolveElementsInGroup(outer, metadata).contains(e)) {
                    //add if correlated to the root groups
                    if (i == 0) {
                        this.references.add(obj);
                    }
                    return;
                }
            } catch (TeiidComponentException e1) {
                throw new TeiidRuntimeException(e1);
            }
        }
    }

    /**
     * <p>Helper to use this visitor.
     * @param obj The Language object that is to be visited
     * @param groupSymbols Collection of GroupSymbols to restrict collection to - these are the groups
     * that the client (outer query) is interested in references to from the correlated subquery
     * @param correlatedReferences List of References collected
     */
    public static final void collectReferences(final LanguageObject obj, final Collection<GroupSymbol> groupSymbols, List<Reference> correlatedReferences, QueryMetadataInterface metadata) {
        final Set<GroupSymbol> groups = new HashSet<GroupSymbol>(groupSymbols);
        final CorrelatedReferenceCollectorVisitor visitor = new CorrelatedReferenceCollectorVisitor(groups, correlatedReferences);
        visitor.metadata = metadata;
        visitor.queryRoot = obj instanceof Command;
        obj.acceptVisitor(new PreOrPostOrderNavigator(visitor, PreOrPostOrderNavigator.PRE_ORDER, true) {

            @Override
            public void visit(Query query) {
                //don't allow confusion with deep nesting by removing intermediate groups
                List<GroupSymbol> fromGroups = null;
                if (query.getFrom() != null) {
                    fromGroups = query.getFrom().getGroups();
                    HashMap<GroupSymbol, GroupSymbol> groupMap = new HashMap<GroupSymbol, GroupSymbol>();
                    for (GroupSymbol g : fromGroups) {
                        groupMap.put(g, g);
                    }
                    visitor.outerGroups.add(groupMap);
                } else {
                    //the logic still expects a group set to be preset
                    visitor.outerGroups.add(Collections.emptyMap());
                }
                super.visit(query);
                visitor.outerGroups.remove(visitor.outerGroups.size() - 1);
            }
        });
    }

}
