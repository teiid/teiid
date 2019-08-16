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

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.PreOrderNavigator;


/**
 * <p>Walk a tree of language objects and collect any predicate criteria that are found.
 * A predicate criteria is of the following types:
 *
 * <ul>
 * <li>{@link org.teiid.query.sql.lang.CompareCriteria} CompareCriteria</li>
 * <li>{@link org.teiid.query.sql.lang.MatchCriteria} MatchCriteria</li>
 * <li>{@link org.teiid.query.sql.lang.SetCriteria} SetCriteria</li>
 * <li>{@link org.teiid.query.sql.lang.SubquerySetCriteria} SubquerySetCriteria</li>
 * <li>{@link org.teiid.query.sql.lang.IsNullCriteria} IsNullCriteria</li>
 * </ul>
 */
public class PredicateCollectorVisitor extends LanguageVisitor {

    private Collection<Criteria> predicates;

    /**
     * Construct a new visitor with the default collection type, which is a
     * {@link java.util.ArrayList}.
     */
    public PredicateCollectorVisitor() {
        this.predicates = new ArrayList<Criteria>();
    }

    /**
     * Visit a language object and collect criteria.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(BetweenCriteria obj) {
        this.predicates.add(obj);
    }

    /**
     * Visit a language object and collect criteria.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(CompareCriteria obj) {
        this.predicates.add(obj);
    }

    /**
     * Visit a language object and collect criteria.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(IsNullCriteria obj) {
        this.predicates.add(obj);
    }

    /**
     * Visit a language object and collect criteria.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(MatchCriteria obj) {
        this.predicates.add(obj);
    }

    /**
     * Visit a language object and collect criteria.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(SetCriteria obj) {
        this.predicates.add(obj);
    }

    /**
     * Visit a language object and collect criteria.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(DependentSetCriteria obj) {
        this.predicates.add(obj);
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.ExistsCriteria)
     */
    public void visit(ExistsCriteria obj) {
        this.predicates.add(obj);
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.SubqueryCompareCriteria)
     */
    public void visit(SubqueryCompareCriteria obj) {
        this.predicates.add(obj);
    }

    /**
     * Visit a language object and collect criteria.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(SubquerySetCriteria obj) {
        this.predicates.add(obj);
    }

    /**
     * Get a collection of predicates discovered while visiting.
     * @return Collection of {@link org.teiid.query.sql.lang.PredicateCriteria} subclasses.
     */
    public Collection<Criteria> getPredicates() {
        return this.predicates;
    }

    /**
     * Helper to quickly get the predicates from obj
     * @param obj Language object
     */
    public static final Collection<Criteria> getPredicates(LanguageObject obj) {
        PredicateCollectorVisitor visitor = new PredicateCollectorVisitor();
        if(obj != null) {
            PreOrderNavigator.doVisit(obj, visitor);
        }
        return visitor.getPredicates();
    }

}
