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

package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
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
    
    public static Criteria combineCriteria(List parts) {
        if(parts == null || parts.isEmpty()) { 
            return null;
        }
        
        if (parts.size() == 1) {
            return (Criteria)parts.get(0);
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
    
    public static Criteria toDisjunctiveNormalForm(Criteria input) {
    	Criteria result = normalize(input, true);
    	return cloneIfNeeded(input, result);
    }

	private static Criteria cloneIfNeeded(Criteria input, Criteria result) {
		if (result != null && !result.equals(input)) {
    		return (Criteria) result.clone();
    	}
    	return input;
	}
    
    public static Criteria toConjunctiveNormalForm(Criteria input) {
        Criteria result = normalize(input, false);
        return cloneIfNeeded(input, result);
    }
    
    /**
     * Returns a new criteria object in the equivalent normal form to the input.
     *  
     * @param input
     * @return
     */
    static Criteria normalize(Criteria input, boolean dnf) {

        boolean invert = false;
        
        if (input instanceof NotCriteria) {
            NotCriteria not = (NotCriteria)input;
            
            Criteria child = not.getCriteria();
            
            if (child instanceof NotCriteria) {
                return normalize(((NotCriteria)child).getCriteria(), dnf);
            }
            
            if (child instanceof CompoundCriteria) {
                invert = true;
                input = child;
            }
        }
        
        if (!(input instanceof CompoundCriteria)) {
            return input;
        }
        
        CompoundCriteria compCrit = (CompoundCriteria)input;
        
        int operator = compCrit.getOperator();
        
        if (invert) {
            operator = (operator==CompoundCriteria.OR)?CompoundCriteria.AND:CompoundCriteria.OR;
        }
        
        List<Criteria> criteria = new ArrayList<Criteria>(compCrit.getCriteria().size());
        List<CompoundCriteria> parts = new LinkedList<CompoundCriteria>();
        
        for (Criteria crit : compCrit.getCriteria()) {
            
            if (invert) {
                crit = new NotCriteria(crit);
            }
            
            crit = normalize(crit, dnf);
            
            if (crit instanceof CompoundCriteria) {
                CompoundCriteria child = (CompoundCriteria)crit;
                
                if (operator == child.getOperator()) {
                    criteria.addAll(child.getCriteria());
                    continue;
                } 
                
                if ((dnf && operator == CompoundCriteria.AND) || (!dnf && operator == CompoundCriteria.OR)) {
                    parts.add(child);
                    continue;
                }
            } 

            criteria.add(crit);
        }

        if (parts.isEmpty()) {
            //no expansion needed, just return
            return new CompoundCriteria(operator, criteria);
        }
        
        int total = 1;
        int[] divisors = new int[parts.size()];
        int i = 0;
        for (CompoundCriteria crit : parts) {
            divisors[i++] = total;
            total *= crit.getCriteriaCount();
        }
        
        List<Criteria> newCrits = new ArrayList<Criteria>(total);
        
        for (i = 0; i < total; i++) {
            CompoundCriteria crit = new CompoundCriteria(dnf?CompoundCriteria.AND:CompoundCriteria.OR, new ArrayList<Criteria>(parts.size() + criteria.size()));
            crit.getCriteria().addAll(criteria);
            for (int j = 0; j < parts.size(); j++) {
                CompoundCriteria disjunct = parts.get(j);
                
                Criteria part = disjunct.getCriteria().get((i/divisors[j])%disjunct.getCriteriaCount());
                crit.addCriteria(part);
            }
            newCrits.add(crit);
        }
        
        return new CompoundCriteria(!dnf?CompoundCriteria.AND:CompoundCriteria.OR, newCrits);
    }
    
    @Override
    public Class getType() {
    	return DataTypeManager.DefaultDataClasses.BOOLEAN;
    }
    
    @Override
    public boolean isResolved() {
    	return true;
    }
	
}  // END CLASS
