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

package org.teiid.query.resolver.util;

import java.util.Iterator;
import java.util.List;

import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.util.ArgCheck;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Reference;


/**
 * <p>Given a LanguageObject containing References and the List of String binding
 * expressions from a query transformation, this visitor will parse and resolve
 * each binding and set the resolved expression on the appropriate Reference,
 * making sure to match up the correct binding with the correct Reference.
 * The Reference is fully resolved after this happens.</p>
 *
 * <p>Optionally, a Map can be built up which maps the String virtual group to
 * a List of Reference objects which have bindings to an element of the
 * virtual group key.  This may be useful to have on hand the Reference objects
 * which are dependent on the changing tuples of a virtual group during query
 * processing.</p>
 */
public class BindVariableVisitor extends LanguageVisitor {

    private List<ElementSymbol> bindings;

	/**
	 * Constructor
	 * @param bindings List of String binding expressions from query
	 * transformation node
	 */
	public BindVariableVisitor(List<ElementSymbol> bindings) {
		ArgCheck.isNotNull(bindings, QueryPlugin.Util.getString("ERR.015.008.0049")); //$NON-NLS-1$
        
		this.bindings = bindings;
	}

    /**
     * Visit a Reference object and bind it based on the bindings
     * @see org.teiid.query.sql.LanguageVisitor#visit(Reference)
     */
    public void visit(Reference obj) {
        bindReference(obj);
    }

    private void bindReference(Reference obj) {
        int index = obj.getIndex();
        
        ElementSymbol binding = bindings.get(index);
        obj.setExpression(binding);
    }

    public void visit(StoredProcedure storedProcedure){
        //collect reference for physical stored procedure
        Iterator<SPParameter> paramsIter = storedProcedure.getParameters().iterator();
        while(paramsIter.hasNext()){
            SPParameter param = paramsIter.next();
            if(param.getParameterType() == ParameterInfo.IN || param.getParameterType() == ParameterInfo.INOUT){
                if(param.getExpression() instanceof Reference){
                    bindReference((Reference)param.getExpression()); 
                }                   
            }
        }       
    }

	/**
	 * Convenient static method for using this visitor
	 * @param obj LanguageObject which has References to be bound
	 * @param bindings List of String binding expressions from query
	 * transformation node
	 * @param boundReferencesMap Map to be filled with String group name to List of References
	 */
    public static void bindReferences(LanguageObject obj, List<ElementSymbol> bindings) {

        BindVariableVisitor visitor = new BindVariableVisitor(bindings);
        DeepPreOrderNavigator.doVisit(obj, visitor);
    }

}
