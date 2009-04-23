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

package com.metamatrix.query.resolver.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.util.ErrorMessageKeys;

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

    private List bindings;
    private QueryMetadataInterface metadata;

    private MetaMatrixComponentException componentException;
    private QueryResolverException resolverException;

	/**
	 * Constructor
	 * @param bindings List of String binding expressions from query
	 * transformation node
	 * @param metadata source of metadata
	 */
	public BindVariableVisitor(List bindings, QueryMetadataInterface metadata) {
		ArgCheck.isNotNull(bindings, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0049));
        
		this.bindings = bindings;
		this.metadata = metadata;
	}

    public MetaMatrixComponentException getComponentException() {
        return this.componentException;
    }

    public QueryResolverException getResolverException() {
        return this.resolverException;
    }

    private void handleException(MetaMatrixComponentException e) {
        this.componentException = e;

        // Abort the validation process
        setAbort(true);
    }

    private void handleException(QueryResolverException e) {
        this.resolverException = e;

        // Abort the validation process
        setAbort(true);
    }

    /**
     * Visit a Reference object and bind it based on the bindings
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(Reference)
     */
    public void visit(Reference obj) {
        bindReference(obj);
    }

    private void bindReference(Reference obj) {
        int index = obj.getIndex();
        
        String binding = (String) bindings.get(index);
        try { 
            bindReference(obj, binding);
        } catch(QueryParserException e) {
            handleException(new QueryResolverException(QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0022), e.getMessage()));
        } catch(QueryMetadataException e) {
            handleException(new MetaMatrixComponentException(e, e.getMessage()));    
        } catch(QueryResolverException e) {
            handleException(e);
        } catch(MetaMatrixComponentException e) {
            handleException(e);
        }
    }

    public void visit(StoredProcedure storedProcedure){
        //collect reference for physical stored procedure
        Iterator paramsIter = storedProcedure.getParameters().iterator();
        while(paramsIter.hasNext()){
            SPParameter param = (SPParameter)paramsIter.next();
            if(param.getParameterType() == ParameterInfo.IN || param.getParameterType() == ParameterInfo.INOUT){
                if(param.getExpression() instanceof Reference){
                    bindReference((Reference)param.getExpression()); 
                }                   
            }
        }       
    }

    private void bindReference(Reference reference, String binding)
        throws  QueryParserException, QueryResolverException,
                QueryMetadataException, MetaMatrixComponentException {

        // Parse and resolve ref
        Expression expr = QueryParser.getQueryParser().parseExpression(binding);

        if(!(expr instanceof ElementSymbol)) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0025, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0025, expr));
        }
        
        ElementSymbol element = (ElementSymbol) expr;

        GroupSymbol groupSymbol = new GroupSymbol(metadata.getGroupName(element.getName()));
        ResolverUtil.resolveGroup(groupSymbol, metadata);

        ResolverVisitor.resolveLanguageObject(element, Arrays.asList(groupSymbol), metadata);

        reference.setExpression(element);
    }

	/**
	 * Convenient static method for using this visitor
	 * @param obj LanguageObject which has References to be bound
	 * @param bindings List of String binding expressions from query
	 * transformation node
	 * @param metadata source of metadata
	 * @param boundReferencesMap Map to be filled with String group name to List of References
	 */
    public static void bindReferences(LanguageObject obj, List bindings, QueryMetadataInterface metadata)
        throws QueryResolverException, MetaMatrixComponentException {

        BindVariableVisitor visitor = new BindVariableVisitor(bindings, metadata);
        DeepPreOrderNavigator.doVisit(obj, visitor);

        if(visitor.getComponentException() != null) {
            throw visitor.getComponentException();
        }

        if(visitor.getResolverException() != null) {
            throw visitor.getResolverException();
        }
    }

}
