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

package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * This class represents the criteria clause for a query, which defines
 * constraints on the data values to be retrieved for each parameter in the
 * select clause. <p>
 */
public abstract class Criteria implements Expression {

    /**
     * Constructs a default instance of this class.
     */
    public Criteria() {
    }

    /**
     * Abstract clone method
     * @return Deep clone of this criteria
     */
    public abstract Object clone();

    /**
     * Return the parser string.
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    /**
     * This utility method will pull apart a tree of criteria by breaking all
     * compound AND criteria apart.  For instance, ((A=1 AND B=2) AND C=3)
     * will be broken into A=1, B=2, C=3.
     * @param crit Criteria to break apart
     * @return List of Criteria, empty list if crit is null
     */
    public static List<Criteria> separateCriteriaByAnd(Criteria crit) {
        if(crit == null) {
            return Collections.emptyList();
        }

        List<Criteria> parts = new ArrayList<Criteria>();
        separateCriteria(crit, parts);
        return parts;
    }

    public static Criteria combineCriteria(List<Criteria> parts) {
        if(parts == null || parts.isEmpty()) {
            return null;
        }

        if (parts.size() == 1) {
            return parts.get(0);
        }

        return new CompoundCriteria(parts);
    }

    /**
     * Helper method for {@link #separateCriteriaByAnd(Criteria)} that
     * can be called recursively to collect parts.
     * @param crit Crit to break apart
     * @param parts Collection to add parts to
     */
    private static void separateCriteria(Criteria crit, Collection<Criteria> parts) {
        if(crit instanceof CompoundCriteria) {
            CompoundCriteria compCrit = (CompoundCriteria) crit;
            if(compCrit.getOperator() == CompoundCriteria.AND) {
                for (Criteria conjunct : compCrit.getCriteria()) {
                    separateCriteria(conjunct, parts);
                }
            } else {
                parts.add(crit);
            }
        } else {
            parts.add(crit);
        }
    }

    /**
     * This utility method can be used to combine two criteria using an AND.
     * If both criteria are null, then null will be returned.  If either is null,
     * then the other will be returned.  If neither is null and the primaryCrit is
     * a CompoundCriteria, then the additionalCrit will be added to the primaryCrit
     * and the primaryCrit will be returned.  If the primaryCrit is not compound, a new
     * CompoundCriteria will be created and both criteria will be added to it.
     * @param primaryCrit Primary criteria - may be modified
     * @param additionalCrit Won't be modified, but will likely be attached to the returned crit
     * @return Combined criteria
     */
    public static Criteria combineCriteria(Criteria primaryCrit, Criteria additionalCrit) {
        return combineCriteria(primaryCrit, additionalCrit, false);
    }

    public static Criteria combineCriteria(Criteria primaryCrit, Criteria additionalCrit, boolean disjunctively) {
        if(primaryCrit == null) {
            return additionalCrit;
        }
        if(additionalCrit == null) {
            return primaryCrit;
        }
        CompoundCriteria compCrit = new CompoundCriteria();
        compCrit.setOperator((disjunctively?CompoundCriteria.OR:CompoundCriteria.AND));
        if ((primaryCrit instanceof CompoundCriteria) && ((CompoundCriteria)primaryCrit).getOperator() == (disjunctively?CompoundCriteria.OR:CompoundCriteria.AND)) {
            compCrit.getCriteria().addAll(((CompoundCriteria)primaryCrit).getCriteria());
        } else {
            compCrit.addCriteria(primaryCrit);
        }
        if ((additionalCrit instanceof CompoundCriteria) && ((CompoundCriteria)additionalCrit).getOperator() == (disjunctively?CompoundCriteria.OR:CompoundCriteria.AND)) {
            compCrit.getCriteria().addAll(((CompoundCriteria)additionalCrit).getCriteria());
        } else {
            compCrit.addCriteria(additionalCrit);
        }
        return compCrit;
    }

    public static Criteria applyDemorgan(Criteria input) {

        if (input instanceof NotCriteria) {
            NotCriteria not = (NotCriteria)input;

            return not.getCriteria();
        }

        if (!(input instanceof CompoundCriteria)) {
            return new NotCriteria(input);
        }

        CompoundCriteria compCrit = (CompoundCriteria)input;

        int operator = (compCrit.getOperator()==CompoundCriteria.OR)?CompoundCriteria.AND:CompoundCriteria.OR;

        List<Criteria> criteria = new ArrayList<Criteria>(compCrit.getCriteria().size());

        for (Criteria crit : compCrit.getCriteria()) {

            crit = new NotCriteria(crit);

            criteria.add(crit);
        }

        return new CompoundCriteria(operator, criteria);
    }

    @Override
    public Class<?> getType() {
        return DataTypeManager.DefaultDataClasses.BOOLEAN;
    }

}  // END CLASS
