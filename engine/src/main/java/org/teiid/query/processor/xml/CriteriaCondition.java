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

package org.teiid.query.processor.xml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.ElementSymbol;


/**
 * A condition based on a sql Criteria Object.
 */
public class CriteriaCondition extends Condition {

    protected Criteria criteria;

    /**
     * The constructor takes the sql criteria object, the list of result set names which
     * the Criteria pertains to, and a sub Program which is the "then" clause if this
     * Condition {link #evaluate evaluates} to true.
     * @param criteria sql Criteria
     * @param resultSetNames zero or more String result set names which pertain to the
     * criteria
     * @param thenProgram Program which is the "then" clause
     */
    public CriteriaCondition(Criteria criteria, Program thenProgram){
        super( thenProgram );
        this.criteria = criteria;
    }

	/**
	 * @see org.teiid.query.processor.xml.Condition#evaluate(Map, List, ProcessorEnvironment)
	 */
	public boolean evaluate(XMLProcessorEnvironment env, XMLContext context) 
        throws TeiidComponentException, TeiidProcessingException{
        
        Map elementMap = new HashMap();
        List data = new ArrayList();

        // these are all the reference values; ElementSymbol ==> Symbol Current value
        // however since the CriteriaEvaluator needs ElementSybol ==> $index && value[$index]
        // we need to convert these values.
        Map referenceValues = context.getReferenceValues();
        int index = 0;
        for (final Iterator i = referenceValues.keySet().iterator(); i.hasNext();) {
            final ElementSymbol element = (ElementSymbol)i.next();
            elementMap.put(element, new Integer(index));
            data.add(referenceValues.get(element));
            index++;
        } 

        try {
			return new Evaluator(elementMap, env.getDataManager(), env.getProcessorContext()).evaluate(this.criteria, data);
		} catch (ExpressionEvaluationException e) {
            throw new TeiidComponentException(e);
		}
	}

    public String toString(){
        return criteria.toString();
    }
    
}
