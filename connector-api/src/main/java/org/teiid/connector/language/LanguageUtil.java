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

package org.teiid.connector.language;

import java.util.*;

import org.teiid.connector.language.ICompoundCriteria.Operator;


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
    public static final List separateCriteriaByAnd(ICriteria criteria) {
        if(criteria == null) { 
            return Collections.EMPTY_LIST;
        }
        
        List parts = new ArrayList();
        separateCriteria(criteria, parts);
        return parts;           
    }
    
    /**
     * Helper method for {@link #separateCriteriaByAnd(ICriteria)} that 
     * can be called recursively to collect parts.
     * @param crit Crit to break apart
     * @param parts List to add parts to
     */
    private static void separateCriteria(ICriteria crit, List parts) {
        if(crit instanceof ICompoundCriteria) {
            ICompoundCriteria compCrit = (ICompoundCriteria) crit;
            if(compCrit.getOperator() == Operator.AND) {
                List subCrits = compCrit.getCriteria();
                Iterator iter = subCrits.iterator();
                while(iter.hasNext()) { 
                    separateCriteria((ICriteria) iter.next(), parts);
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
     * an ICompoundCriteria, then the additionalCrit will be added to the primaryCrit
     * and the primaryCrit will be returned.  If the primaryCrit is not compound, a new
     * ICompoundCriteria will be created and both criteria will be added to it.
     * @param primaryCrit Primary criteria - may be modified
     * @param additionalCrit Won't be modified, but will likely be attached to the returned crit
     * @param languageFactory Will be used to construct new ICompoundCriteria if necessary
     * @return Combined criteria
     */
    public static ICriteria combineCriteria(ICriteria primaryCrit, ICriteria additionalCrit, ILanguageFactory languageFactory) {
        if(primaryCrit == null) {
            return additionalCrit;
        } else if(additionalCrit == null) { 
            return primaryCrit;
        } else if((primaryCrit instanceof ICompoundCriteria) && ((ICompoundCriteria)primaryCrit).getOperator() == Operator.AND) {
            ICompoundCriteria primaryCompound = (ICompoundCriteria) primaryCrit;
            primaryCompound.getCriteria().add(additionalCrit);
            return primaryCrit;
        } else {
            List crits = new ArrayList(2);
            crits.add(primaryCrit);
            crits.add(additionalCrit);
            ICompoundCriteria compCrit = languageFactory.createCompoundCriteria(Operator.AND, crits);
            return compCrit;
        }               
    }   
    
}
