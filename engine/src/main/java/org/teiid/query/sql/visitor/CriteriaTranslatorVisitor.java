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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.teiid.core.util.Assertion;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.proc.CriteriaSelector;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Reference;


/**
 * <p> This class is used to translate criteria specified on the user's update command against
 * the virtual group, the elements on this criteria are replaced by elements on the query
 * transformation that defines the virtual group. Parts of the criteria are selectively translated
 * if a CriteriaSelector is specified, also if the user explicitly defines translations for some
 * of the elements those translations override any symbol mappings.</p>
 */
public class CriteriaTranslatorVisitor extends ExpressionMappingVisitor {
	
	// criteria selector specified on the TranslateCriteria obj
	private CriteriaSelector selector;

	// translation in for of CompareCriteria objs on the TranslateCriteria obj
	private Collection translations;

	private Map<ElementSymbol, Reference> implicitParams = new HashMap<ElementSymbol, Reference>();

    /**
     * <p> This constructor initializes this object by setting the symbolMap.</p>
     * @param symbolMap A map of virtual elements to their counterparts in transform
     * defining the virtual group
     */
    public CriteriaTranslatorVisitor(Map symbolMap) {
        super(symbolMap);
        Assertion.isNotNull(symbolMap);
    }

	/**
	 * <p>Set the criteria selector used to restrict the part of the criteria that needs to be
	 * translated.</p>
	 * @param selector The <code>CriteriaSelector</code> on the <code>TranslateCriteria</code>
	 * object
	 */
    public void setCriteriaSelector(CriteriaSelector selector) {
    	this.selector = selector;
    }

	/**
	 * <p> Set the translations to be used to replace elements on the user's command against
	 * the virtual group.</p>
     * @param translations Collection of <code>ComapreCriteria</code> objects used to
     * specify translations
     */
    public void setTranslations(Collection translations) {
    	this.translations = translations;
    }

    /* ############### Helper Methods ##################   */    
    
    private boolean selectorContainsCriteriaElements(Criteria criteria, int criteriaType) {
        int selectorType = selector.getSelectorType();
        if(selectorType!= CriteriaSelector.NO_TYPE && selectorType != criteriaType) {
            return false;
        } else if(selector.hasElements()) {                
            Iterator selectElmnIter = selector.getElements().iterator();
            Collection<ElementSymbol> critElmnts = ElementCollectorVisitor.getElements(criteria, true);
            while(selectElmnIter.hasNext()) {
                ElementSymbol selectElmnt = (ElementSymbol) selectElmnIter.next();
                if(critElmnts.contains(selectElmnt)) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }
    
    @Override
    public Expression replaceExpression(Expression obj) {
    	if (this.translations != null && obj instanceof ElementSymbol) {
			Iterator transIter = this.translations.iterator();
			while(transIter.hasNext()) {
				CompareCriteria compCrit = (CompareCriteria) transIter.next();
				Collection<ElementSymbol> leftElmnts = ElementCollectorVisitor.getElements(compCrit.getLeftExpression(), true);
				// there is always only one element
				ElementSymbol element = leftElmnts.iterator().next();
				if(obj.equals(element)) {
					return compCrit.getRightExpression();
				}
			}
     	}
    	/*
    	 * Special handling for references in translated criteria.
    	 * We need to create a locally valid reference name.
    	 */
    	if (obj instanceof Reference) {
    		Reference implicit = (Reference)obj;
    		ElementSymbol key = null;
    		if (implicit.isPositional()) {
    			key = new ElementSymbol("$INPUT." + implicit.getContextSymbol()); //$NON-NLS-1$
    		} else {
    			key = new ElementSymbol("$INPUT." + implicit.getExpression().getName()); //$NON-NLS-1$
    		}
    		key.setType(implicit.getType());
    		this.implicitParams.put(key, implicit);
    		return new Reference(key);
    	}
    	return super.replaceExpression(obj);
    }

    public Map<ElementSymbol, Reference> getImplicitParams() {
		return implicitParams;
	}
    
    public Criteria translate(Criteria crit) {
    	LinkedList<Criteria> crits = new LinkedList<Criteria>();
    	for (Criteria conjunct : Criteria.separateCriteriaByAnd(crit)) {
			if (conjunct instanceof BetweenCriteria) {
				if (!selectorContainsCriteriaElements(conjunct, CriteriaSelector.BETWEEN)) {
					continue;
		        }
			} else if (conjunct instanceof CompareCriteria) {
		        if (!selectorContainsCriteriaElements(conjunct, ((CompareCriteria)conjunct).getOperator())) {
		        	continue; 
		        }
			} else if (conjunct instanceof IsNullCriteria) {
		        if (!selectorContainsCriteriaElements(conjunct, CriteriaSelector.IS_NULL)) {
		        	continue; 
		        }
			} else if (conjunct instanceof MatchCriteria) {
		        if (!selectorContainsCriteriaElements(conjunct, CriteriaSelector.LIKE)) {
		        	continue; 
		        }
			} else if (conjunct instanceof AbstractSetCriteria) {
		        if (!selectorContainsCriteriaElements(conjunct, CriteriaSelector.IN)) {
		        	continue;
		        }
			} else if (!selectorContainsCriteriaElements(conjunct, CriteriaSelector.NO_TYPE)) {
	        	continue;
			}
			DeepPostOrderNavigator.doVisit(conjunct, this);
			crits.add(conjunct);
		}
    	if (crits.isEmpty()) {
    		return QueryRewriter.TRUE_CRITERIA;
    	}
    	return Criteria.combineCriteria(crits);
    }
    
}
