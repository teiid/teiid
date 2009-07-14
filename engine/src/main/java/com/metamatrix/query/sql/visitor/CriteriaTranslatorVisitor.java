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

package com.metamatrix.query.sql.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.proc.CriteriaSelector;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;

/**
 * <p> This class is used to translate criteria specified on the user's update command against
 * the virtual group, the elements on this criteria are replaced by elements on the query
 * transformation that defines the virtual group. Parts of the criteria are selectively translated
 * if a CriteriaSelector is specified, also if the user explicty defines translations for some
 * of the elements those translations override any symbol mappings.</p>
 */
public class CriteriaTranslatorVisitor extends ExpressionMappingVisitor {

	// criteria selector specified on the TranslateCriteria obj
	private CriteriaSelector selector;

	// translation in for of CompareCriteria objs on the TranslateCriteria obj
	private Collection translations;

	// list of translated criteria
	private List<Criteria> translatedCriteria = new ArrayList<Criteria>();
	
	private Map<ElementSymbol, Reference> implicitParams = new HashMap<ElementSymbol, Reference>();

    /**
     * <p> This constructor initialises the visitor</p>
     */
    public CriteriaTranslatorVisitor() {
    	this(null);
    }

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

    // ############### Visitor methods for language objects ##################

    /**
     * <p> This method updates the <code>BetweenCriteria</code> object it receives as an
     * argument by replacing the virtual elements present in the expressions in the
     * function with translated expressions.</p>
     * @param obj The BetweenCriteria object to be updated with translated expressions
     */
    public void visit(BetweenCriteria obj) {
        if (!selectorContainsCriteriaElements(obj, CriteriaSelector.BETWEEN)) {
            return;
        }
        super.visit(obj);
        translatedCriteria.add(obj);
    }
    
    /**
     * <p> This method updates the <code>CompareCriteria</code> object it receives as an
     * argument by replacing the virtual elements present in the expressions in the
     * function with translated expressions.</p>
     * @param obj The CompareCriteria object to be updated with translated expressions
     */
    public void visit(CompareCriteria obj) {
        
        if (!selectorContainsCriteriaElements(obj, obj.getOperator())) {
            return;
        }

        super.visit(obj);
    	translatedCriteria.add(obj);
    }

    /**
     * <p> This method updates the <code>IsNullCriteria</code> object it receives as an
     * argument by replacing the virtual elements present in the expressions in the
     * function with translated expressions.</p>
     * @param obj The IsNullCriteria object to be updated with translated expressions
     */
    public void visit(IsNullCriteria obj) {

        if (!selectorContainsCriteriaElements(obj, CriteriaSelector.IS_NULL)) {
            return;
        }
        super.visit(obj);
        translatedCriteria.add(obj);
    }

    /**
     * <p> This method updates the <code>MatchCriteria</code> object it receives as an
     * argument by replacing the virtual elements present in the expressions in the
     * function with translated expressions</p>
     * @param obj The SetCriteria object to be updated with translated expressions
     */
    public void visit(MatchCriteria obj) {
        
        if (!selectorContainsCriteriaElements(obj, CriteriaSelector.LIKE)) {
            return;
        }

        super.visit(obj);
    	translatedCriteria.add(obj);
    }
    
    /**
     * <p> This method updates the <code>SetCriteria</code> object it receives as an
     * argument by replacing the virtual elements present in the expressions in the
     * function with translated expressions</p>
     * @param obj The SetCriteria object to be updated with translated expressions
     */
    public void visit(SetCriteria obj) {
        
        if (!selectorContainsCriteriaElements(obj, CriteriaSelector.IN)) {
            return;
        }
        
        super.visit(obj);
    	translatedCriteria.add(obj);
    }

    /**
     * <p> This method updates the <code>SetCriteria</code> object it receives as an
     * argument by replacing the virtual elements present in the expressions in the
     * function with translated expressions</p>
     * @param obj The SetCriteria object to be updated with translated expressions
     */
    public void visit(DependentSetCriteria obj) {
        
        if (!selectorContainsCriteriaElements(obj, CriteriaSelector.IN)) {
            return;
        }
        
        super.visit(obj);
        translatedCriteria.add(obj);
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

    /**
     * <p>Gets the criteria translated by this visitor, differrent parts of the user's
     * criteria are translated and they are combined as a <code>CompoundCriteria</code>
     * using an AND operator. Returns a null if no part of the user's criteria could
     * be translated.</p>
     * @return The criteria after vistor completes translation of the criteria on the
     * virtual group
     */
    public Criteria getTranslatedCriteria() {
    	if(translatedCriteria.size() > 0) {
    		if(translatedCriteria.size() == 1) {
    			return translatedCriteria.get(0);
    		}
   			return new CompoundCriteria(CompoundCriteria.AND, translatedCriteria);
    	}
   		return null;
    }
    
    public Map<ElementSymbol, Reference> getImplicitParams() {
		return implicitParams;
	}
    
}
