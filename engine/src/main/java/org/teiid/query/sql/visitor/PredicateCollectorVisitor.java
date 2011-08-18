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

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.PreOrderNavigator;


/**
 * <p>Walk a tree of language objects and collect any predicate criteria that are found.
 * A predicate criteria is of the following types: </p>
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
