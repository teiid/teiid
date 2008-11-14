/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.*;

import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.*;
import com.metamatrix.query.sql.symbol.*;
import com.metamatrix.query.sql.proc.*;

/**
 * <p> This class is used to translate criteria specified on the user's update command against
 * the virtual group, the elements on this criteria are replaced by elements on the query
 * transformation that defines the virtual group. Parts of the criteria are selectively translated
 * if a CriteriaSelector is specified, also if the user explicty defines translations for some
 * of the elements those translations override any symbol mappings.</p>
 */
public class CriteriaTranslatorVisitor extends LanguageVisitor {

	// map between virtual elements and the elements in its query transformation
	private Map symbolMap;

	// criteria selector specified on the TranslateCriteria obj
	private CriteriaSelector selector;

	// traslation in for of CompareCriteria objs on the TranslateCriteria obj
	private Collection translations;

	// list of translated criteria
	private List translatedCriteria;

    /**
     * <p> This constructor initialises the visitor</p>
     */
    public CriteriaTranslatorVisitor() {
        this.translatedCriteria = new ArrayList();
    }

    /**
     * <p> This constructor initialises this object by setting the symbolMap.</p>
     * @param symbolMap A map of virtual elements to their counterparts in transform
     * defining the virtual group
     */
    public CriteriaTranslatorVisitor(Map symbolMap) {
        this();
        Assertion.isNotNull(symbolMap);
        this.symbolMap = symbolMap;
    }

    /**
     * <p>Get the symbol map between virtual group elements and the expressions that define
     * them on the query transform of the virtual group.</p>
     * @param symbolMap A map of virtual elements to their counterparts in transform
     * defining the virtual group
     */
    protected Symbol getMappedSymbol(Symbol symbol) {
        return (Symbol) this.symbolMap.get(symbol);
    }

    /**
     * <p>Set the symbol map between virtual group elements and the expressions that define
     * them on the query transform of the virtual group.</p>
     * @param symbolMap A map of virtual elements to their counterparts in transform
     * defining the virtual group
     */
    public void setSymbolMap(Map symbolMap) {
    	this.symbolMap = symbolMap;
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
        
        obj.setExpression(replaceExpression(obj.getExpression()));
        obj.setLowerExpression(replaceExpression(obj.getLowerExpression()));
        obj.setUpperExpression(replaceExpression(obj.getUpperExpression()));

        translatedCriteria.add(obj);
    }

    /**
     * <p> This method updates the <code>CaseExpression</code> object it receives as an
     * argument by replacing the virtual elements present in the expressions in the
     * function with translated expressions.</p>
     * @param obj The CaseExpression object to be updated with translated expressions
     */
    public void visit(CaseExpression obj) {
        obj.setExpression(replaceExpression(obj.getExpression()));
        int whenCount = obj.getWhenCount();
        ArrayList whens = new ArrayList(whenCount);
        ArrayList thens = new ArrayList(whenCount);
        for(int i = 0; i < whenCount; i++) {
            whens.add(replaceExpression(obj.getWhenExpression(i)));
            thens.add(replaceExpression(obj.getThenExpression(i)));
        }
        obj.setWhen(whens, thens);
        if (obj.getElseExpression() != null) {
            obj.setElseExpression(replaceExpression(obj.getElseExpression()));
        }
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

        obj.setLeftExpression(replaceExpression(obj.getLeftExpression()));
        obj.setRightExpression(replaceExpression(obj.getRightExpression()));

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
		obj.setExpression(replaceExpression(obj.getExpression()));
        
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

        obj.setLeftExpression(replaceExpression(obj.getLeftExpression()));
        obj.setRightExpression(replaceExpression(obj.getRightExpression()));

    	translatedCriteria.add(obj);
    }

    /**
     * <p> This method updates the <code>SearchedCaseExpression</code> object it receives as an
     * argument by replacing the virtual elements present in the expressions in the
     * function with translated expressions.</p>
     * @param obj The SearchedCaseExpression object to be updated with translated expressions
     */
    public void visit(SearchedCaseExpression obj) {
        int whenCount = obj.getWhenCount();
        ArrayList thens = new ArrayList(whenCount);
        for(int i = 0; i < whenCount; i++) {
            thens.add(replaceExpression(obj.getThenExpression(i)));
        }
        obj.setWhen(obj.getWhen(), thens);
        if (obj.getElseExpression() != null) {
            obj.setElseExpression(replaceExpression(obj.getElseExpression()));
        }
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
        
        obj.setExpression(replaceExpression(obj.getExpression()));

    	// create a new list containing physical elements and constants
    	List valuesList = new ArrayList(obj.getNumberOfValues());

    	// iterate over the list containing virtual elements/constants
    	Iterator valuesIter = obj.getValues().iterator();
    	while(valuesIter.hasNext()) {
    	    Expression valueExpr = (Expression) valuesIter.next();
    	    valuesList.add(replaceExpression(valueExpr));
    	}
        obj.setValues(valuesList);

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
        
        obj.setExpression(replaceExpression(obj.getExpression()));

        translatedCriteria.add(obj);
    }

    /**
     * <p> This method updates the <code>Function</code> object it receives as an
     * argument by replacing the virtual elements present in the expressions in the
     * function with translated expressions</p>
     * @param obj The Function object to be updated with translated expressions
     */
    public void visit(Function obj) {

    	int argLength = obj.getArgs().length;
    	Expression args[] = new Expression [argLength];

    	for(int i=0; i < argLength; i++) {
            args[i] = replaceExpression(obj.getArg(i));
    	}
    	obj.setArgs(args);
    }

    /* ############### Helper Methods ##################   */    
    
    private boolean selectorContainsCriteriaElements(Criteria criteria, int criteriaType) {
        int selectorType = selector.getSelectorType();
        if(selectorType!= CriteriaSelector.NO_TYPE && selectorType != criteriaType) {
            return false;
        } else if(selector.hasElements()) {                
            Iterator selectElmnIter = selector.getElements().iterator();
            Collection critElmnts = ElementCollectorVisitor.getElements(criteria, true);
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
    
    /**
     * Utility method that implements a common pattern used throughout this class.
     * @param exp an expression
     * @return the translated expression, if the the expression needs to the
     *         translated. Otherwise, the same expression.
     */
    private Expression replaceExpression(Expression exp) {
        if(exp instanceof AliasSymbol) {
            exp = ((AliasSymbol)exp).getSymbol();
        }
        if(exp instanceof ElementSymbol) {
            exp = getTranslatedExpression((ElementSymbol)exp);
        }
        return exp;
    }

    /**
     * <p> This method looks up the symbol map between <code>ElementSymbol</code>s at the virtual
     * group level to the <code>Expression</code>s they map to in the query transformation
     * that defines the virtual group, if there is valid translation expresion in the map the
     * translated expression is returned else a the element symbol is
     * returned back as there is no mapping (physical elements).</p>
     * @param obj The virtual <code>ElementSymbol</code> object whose counterpart used in
     * the query transformation of the virtual group is returned
     * @return An <code>Expression</code> object or the elementSymbol if the
     * object could not be mapped
     */
    private Expression getMappedExpression(ElementSymbol obj) {
    	
    	Expression expr = (Expression) this.symbolMap.get(obj); 
		if(expr != null) {
			return expr;
		}
		return obj;
    }

    private Expression getTranslatedExpression(ElementSymbol obj) {
    	 if(this.translations != null) {
			Iterator transIter = this.translations.iterator();
			while(transIter.hasNext()) {
				CompareCriteria compCrit = (CompareCriteria) transIter.next();
				Collection leftElmnts = ElementCollectorVisitor.getElements(compCrit.getLeftExpression(), true);
				// there is always only one element
				ElementSymbol element = (ElementSymbol)leftElmnts.iterator().next();
				if(obj.equals(element)) {
					return compCrit.getRightExpression();
				}
			}
    	}

		return getMappedExpression(obj);
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
    			return (Criteria) translatedCriteria.get(0);
    		}
   			return new CompoundCriteria(CompoundCriteria.AND, translatedCriteria);
    	}
   		return null;
    }

}
