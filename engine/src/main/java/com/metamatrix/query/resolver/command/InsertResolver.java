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

package com.metamatrix.query.resolver.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.resolver.ProcedureContainerResolver;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.VariableResolver;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.GroupContext;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * This class knows how to expand and resolve INSERT commands.
 */
public class InsertResolver extends ProcedureContainerResolver implements VariableResolver {

    /**
     * Resolve an INSERT.  Need to resolve elements, constants, types, etc.
     * @see com.metamatrix.query.resolver.ProcedureContainerResolver#resolveProceduralCommand(com.metamatrix.query.sql.lang.Command, boolean, com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.analysis.AnalysisRecord)
     */
    public void resolveProceduralCommand(Command command, boolean useMetadataCommands, TempMetadataAdapter metadata, AnalysisRecord analysis) 
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {


        // Cast to known type
        Insert insert = (Insert) command;
        
        //variables and values must be resolved separately to account for implicitly defined temp groups
        resolveList(insert.getValues(), metadata, insert.getExternalGroupContexts(), null);

        //resolve subquery if there
        if(insert.getQueryExpression() != null) {
        	QueryResolver.setChildMetadata(insert.getQueryExpression(), command);
            
            QueryResolver.resolveCommand(insert.getQueryExpression(), Collections.EMPTY_MAP, useMetadataCommands, metadata.getMetadata(), analysis, false);
        }

        Set groups = new HashSet();
        groups.add(insert.getGroup());
        
        if (insert.getVariables().isEmpty()) {
            if (insert.getGroup().isResolved()) {
                List variables = ResolverUtil.resolveElementsInGroup(insert.getGroup(), metadata);
                for (Iterator i = variables.iterator(); i.hasNext();) {
                    insert.addVariable((ElementSymbol)((ElementSymbol)i.next()).clone());
                }
            } else {
                for (int i = 0; i < insert.getValues().size(); i++) {
                    insert.addVariable(new ElementSymbol("expr" + i)); //$NON-NLS-1$
                }
            }
        } else if (insert.getGroup().isResolved()) {
            resolveVariables(metadata, insert, groups);
        }

        resolveTypes(insert);
        
        if (!insert.getGroup().isResolved()) { //define the implicit temp group
            if(insert.getQueryExpression() != null) {
                ResolverUtil.resolveImplicitTempGroup(metadata, insert.getGroup(), insert.getQueryExpression().getProjectedSymbols());
            }else {
                ResolverUtil.resolveImplicitTempGroup(metadata, insert.getGroup(), insert.getVariables());
            }
            resolveVariables(metadata, insert, groups);
            
            //ensure that the types match
            resolveTypes(insert);
        }
    }

    private void resolveVariables(TempMetadataAdapter metadata,
                                  Insert insert,
                                  Set groups) throws MetaMatrixComponentException,
                                             QueryResolverException {
        try {
            resolveList(insert.getVariables(), metadata, null, groups);
        } catch (QueryResolverException e) {
            throw new QueryResolverException(e, QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0054, insert.getGroup(), e.getUnresolvedSymbols()));
        }
    }

    private void resolveList(Collection elements, TempMetadataAdapter metadata,
                                  GroupContext externalGroups, Set groups) throws MetaMatrixComponentException,
                                             QueryResolverException {
        for (Iterator i = elements.iterator(); i.hasNext();) {
            Expression expr = (Expression)i.next();
            ResolverVisitor.resolveLanguageObject(expr, groups, externalGroups, metadata);
        }
    }
    
    /** 
     * @param insert
     * @throws QueryResolverException
     */
    public void resolveTypes(Insert insert) throws QueryResolverException {
        
        boolean usingQuery = insert.getQueryExpression() != null;
        
        // resolve any functions in the values
        List values = insert.getValues();
        
        if (usingQuery) {
            values = insert.getQueryExpression().getProjectedSymbols();
        }
        
        List newValues = new ArrayList(values.size());
        
        // check that # of variables == # of values
        if(values.size() != insert.getVariables().size()) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0010, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0010, new Object[] {new Integer(insert.getVariables().size()), new Integer(insert.getValues().size())}));
        }
        
        Iterator valueIter = values.iterator();
        Iterator varIter = insert.getVariables().iterator();
        while(valueIter.hasNext()) {
            // Walk through both elements and expressions, which should match up
			Expression expression = (Expression) valueIter.next();
			ElementSymbol element = (ElementSymbol) varIter.next();
			
			if (!usingQuery) {
				ResolverUtil.setDesiredType(expression, element.getType(), insert);
			}

            if(element.getType() != null && expression.getType() != null) {
                String elementTypeName = DataTypeManager.getDataTypeName(element.getType());
                if (!usingQuery) {
                    newValues.add(ResolverUtil.convertExpression(expression, elementTypeName));
                } else if (element.getType() != expression.getType()
                           && !DataTypeManager.isImplicitConversion(DataTypeManager.getDataTypeName(expression.getType()),
                                                                    DataTypeManager.getDataTypeName(element.getType()))) {
                    //TODO: a special case here is a projected literal
                    throw new QueryResolverException(QueryPlugin.Util.getString("InsertResolver.cant_convert_query_type", new Object[] {expression, expression.getType().getName(), element, element.getType().getName()})); //$NON-NLS-1$
                }
            } else if (element.getType() == null && expression.getType() != null && !usingQuery)  {
                element.setType(expression.getType());
                newValues.add(expression);
            } else {
                Assertion.failed("Cannot determine element or expression type"); //$NON-NLS-1$
            }
        }

        if (!usingQuery) {
            insert.setValues(newValues);
        }
    }
    
    /** 
     * @param metadata
     * @param group
     * @return
     * @throws MetaMatrixComponentException
     * @throws QueryMetadataException
     */
    protected String getPlan(QueryMetadataInterface metadata,
                           GroupSymbol group) throws MetaMatrixComponentException,
                                             QueryMetadataException {
        return metadata.getInsertPlan(group.getMetadataID());
    }
    
    /** 
     * @see com.metamatrix.query.resolver.ProcedureContainerResolver#resolveGroup(com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.sql.lang.ProcedureContainer)
     */
    protected void resolveGroup(TempMetadataAdapter metadata,
                                ProcedureContainer procCommand) throws MetaMatrixComponentException,
                                                              QueryResolverException {
        if (!procCommand.getGroup().isImplicitTempGroupSymbol() || metadata.getMetadataStore().getTempGroupID(procCommand.getGroup().getName()) != null) {
            super.resolveGroup(metadata, procCommand);
        }
    }

    /** 
     * @throws MetaMatrixComponentException 
     * @throws QueryResolverException 
     * @throws QueryMetadataException 
     * @see com.metamatrix.query.resolver.CommandResolver#getVariableValues(com.metamatrix.query.sql.lang.Command, com.metamatrix.query.metadata.QueryMetadataInterface)
     */
    public Map getVariableValues(Command command,
                                 QueryMetadataInterface metadata) throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {
        
        Insert insert = (Insert) command;
        
        Map result = new HashMap();
        
        // iterate over the variables and values they should be the same number
        Iterator varIter = insert.getVariables().iterator();
        Iterator valIter = insert.getValues().iterator();
        while (varIter.hasNext()) {
            ElementSymbol varSymbol = (ElementSymbol) varIter.next();
            
            String varName = varSymbol.getShortCanonicalName();
            String changingKey = ProcedureReservedWords.CHANGING + ElementSymbol.SEPARATOR + varName;
            String inputKey = ProcedureReservedWords.INPUT + ElementSymbol.SEPARATOR + varName;
            
            result.put(changingKey, new Constant(Boolean.TRUE));
            result.put(inputKey, valIter.next());
        }
        
        Collection insertElmnts = ResolverUtil.resolveElementsInGroup(insert.getGroup(), metadata);

        insertElmnts.removeAll(insert.getVariables());

        Iterator defaultIter = insertElmnts.iterator();
        while(defaultIter.hasNext()) {
            ElementSymbol varSymbol = (ElementSymbol) defaultIter.next();

            Expression value = ResolverUtil.getDefault(varSymbol, metadata);
            
            String varName = varSymbol.getShortCanonicalName();
            String changingKey = ProcedureReservedWords.CHANGING + ElementSymbol.SEPARATOR + varName;
            String inputKey = ProcedureReservedWords.INPUT + ElementSymbol.SEPARATOR + varName;
            
            result.put(changingKey, new Constant(Boolean.FALSE));
            result.put(inputKey, value);
        }
        
        return result;
    }
    
}
