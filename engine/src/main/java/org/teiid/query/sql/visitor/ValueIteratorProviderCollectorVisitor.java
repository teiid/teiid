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
import java.util.LinkedList;
import java.util.List;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.symbol.ScalarSubquery;


/**
 * <p>This visitor class will traverse a language object tree and collect all language
 * objects that implement {@link SubqueryContainer}.
 * By default it uses a java.util.ArrayList to collect the objects in the order
 * they're found.
 *
 * <p>The easiest way to use this visitor is to call one of the static methods which create
 * the visitor, run the visitor, and get the collection.
 * The public visit() methods should NOT be called directly.
 */
public class ValueIteratorProviderCollectorVisitor extends LanguageVisitor {

    private List<SubqueryContainer<?>> valueIteratorProviders;
    private boolean collectLateral;

    /**
     * Construct a new visitor with the default collection type, which is a
     * {@link java.util.ArrayList}.
     */
    public ValueIteratorProviderCollectorVisitor() {
        this.valueIteratorProviders = new ArrayList<SubqueryContainer<?>>();
    }

    /**
     * Construct a new visitor with the given Collection to accumulate
     * ValueIteratorProvider instances
     * @param valueIteratorProviders Collection to accumulate found
     */
    ValueIteratorProviderCollectorVisitor(List<SubqueryContainer<?>> valueIteratorProviders) {
        this.valueIteratorProviders = valueIteratorProviders;
    }

    /**
     * Get the value iterator providers collected by the visitor.  This should best be called
     * after the visitor has been run on the language object tree.
     * @return Collection of {@link SubqueryContainer}
     * (by default, this is a java.util.ArrayList)
     */
    public List<SubqueryContainer<?>> getValueIteratorProviders() {
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
        if (obj.getCommand() != null) {
            this.valueIteratorProviders.add(obj);
        }
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

    public void visit(SubqueryFromClause obj) {
        if (collectLateral && obj.isLateral()) {
            this.valueIteratorProviders.add(obj);
        } else {
            getValueIteratorProviders(obj.getCommand(), this.valueIteratorProviders);
        }
    }

    /**
     * Helper to quickly get the ValueIteratorProvider instances from obj
     * @param obj Language object
     * @return java.util.ArrayList of found ValueIteratorProvider
     */
    public static final List<SubqueryContainer<?>> getValueIteratorProviders(LanguageObject obj) {
        ValueIteratorProviderCollectorVisitor visitor = new ValueIteratorProviderCollectorVisitor();
        PreOrderNavigator.doVisit(obj, visitor);
        return visitor.getValueIteratorProviders();
    }

    public static final void getValueIteratorProviders(LanguageObject obj, List<SubqueryContainer<?>> valueIteratorProviders) {
        ValueIteratorProviderCollectorVisitor visitor = new ValueIteratorProviderCollectorVisitor(valueIteratorProviders);
        PreOrderNavigator.doVisit(obj, visitor);
    }

    public static final List<SubqueryContainer<?>> getValueIteratorProviders(Collection<? extends LanguageObject> languageObjects) {
        if (languageObjects == null || languageObjects.isEmpty()) {
            return Collections.emptyList();
        }
        List<SubqueryContainer<?>> result = new LinkedList<SubqueryContainer<?>>();
        ValueIteratorProviderCollectorVisitor visitor = new ValueIteratorProviderCollectorVisitor(result);
        for (LanguageObject obj : languageObjects) {
            PreOrderNavigator.doVisit(obj, visitor);
        }
        return result;
    }

    public void setCollectLateral(boolean b) {
        this.collectLateral = b;
    }
}
