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

package org.teiid.language;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.language.AndOr.Operator;


/**
 * Helpful utility methods to work with language interfaces.
 */
public final class LanguageUtil {

    /**
     * Can't construct - this contains only static utility methods
     */
    private LanguageUtil() {
    }

    /**
     * Take a criteria, which may be null, a single IPredicateCriteria or a
     * complex criteria built using ICompoundCriteria and breaks it apart
     * at ANDs such that a List of ICriteria conjuncts are returned.  For
     * example, ((a=1 OR b=2) AND (c=3 AND d=4)) would return the list
     * (a=1 OR b=2), c=3, d=4.  If criteria is null, an empty list is
     * returned.
     * @param criteria Criteria to break, may be null
     * @return List of ICriteria, never null
     */
    public static final List<Condition> separateCriteriaByAnd(Condition criteria) {
        if(criteria == null) {
            return Collections.emptyList();
        }

        List<Condition> parts = new ArrayList<Condition>();
        separateCriteria(criteria, parts);
        return parts;
    }

    /**
     * Helper method for {@link #separateCriteriaByAnd(Condition)} that
     * can be called recursively to collect parts.
     * @param crit Crit to break apart
     * @param parts List to add parts to
     */
    private static void separateCriteria(Condition crit, List<Condition> parts) {
        if(crit instanceof AndOr) {
            AndOr compCrit = (AndOr) crit;
            if(compCrit.getOperator() == Operator.AND) {
                separateCriteria(compCrit.getLeftCondition(), parts);
                separateCriteria(compCrit.getRightCondition(), parts);
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
     * then the other will be returned.
     * @param primaryCrit Primary criteria - may be modified
     * @param additionalCrit Won't be modified, but will likely be attached to the returned crit
     * @param languageFactory Will be used to construct new ICompoundCriteria if necessary
     * @return Combined criteria
     */
    public static Condition combineCriteria(Condition primaryCrit, Condition additionalCrit, LanguageFactory languageFactory) {
        if(primaryCrit == null) {
            return additionalCrit;
        } else if(additionalCrit == null) {
            return primaryCrit;
        } else {
            return languageFactory.createAndOr(Operator.AND, primaryCrit, additionalCrit);
        }
    }

    /**
     * Combines a list of conditions under a single AndOr
     * @param crits
     * @return
     */
    public static Condition combineCriteria(List<Condition> crits) {
        if(crits == null || crits.isEmpty()) {
            return null;
        }
        if (crits.size() == 1) {
            return crits.get(0);
        }
        Condition result = null;
        for (Condition crit : crits) {
            if (result == null) {
                result = crit;
            } else {
                result = new AndOr(result, crit, Operator.AND);
            }
        }
        return result;
    }

}
