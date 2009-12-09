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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.resolver.ProcedureContainerResolver;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.VariableResolver;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.GroupContext;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.lang.XQuery;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 */
public class ExecResolver extends ProcedureContainerResolver implements VariableResolver {
	
	@Override
	public Command expandCommand(ProcedureContainer procCommand,
			QueryMetadataInterface metadata, AnalysisRecord analysis)
			throws QueryMetadataException, QueryResolverException,
			MetaMatrixComponentException {
		Command command = super.expandCommand(procCommand, metadata, analysis);
		if (command instanceof XQuery) {
			XQuery query = (XQuery)command;
			query.setProcedureGroup(procCommand.getGroup().getCanonicalName());
			query.setVariables(getVariableValues(procCommand, metadata));
		}
		return command;
	}

    /**
     * @see com.metamatrix.query.resolver.CommandResolver#findCommandMetadata(com.metamatrix.query.sql.lang.Command,
     * com.metamatrix.query.metadata.QueryMetadataInterface)
     */
    private void findCommandMetadata(Command command, TempMetadataStore discoveredMetadata, QueryMetadataInterface metadata)
    throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        StoredProcedure storedProcedureCommand = (StoredProcedure) command;
        
        StoredProcedureInfo storedProcedureInfo = metadata.getStoredProcedureInfoForProcedure(storedProcedureCommand.getProcedureName());

        storedProcedureCommand.setUpdateCount(storedProcedureInfo.getUpdateCount());
        storedProcedureCommand.setModelID(storedProcedureInfo.getModelID());
        storedProcedureCommand.setProcedureID(storedProcedureInfo.getProcedureID());
        storedProcedureCommand.setProcedureCallableName(storedProcedureInfo.getProcedureCallableName());

        // Get old parameters as they may have expressions set on them - collect
        // those expressions to copy later into the resolved parameters
        List oldParams = storedProcedureCommand.getParameters();

        boolean namedParameters = storedProcedureCommand.displayNamedParameters();
        
        // If parameter count is zero, then for the purposes of this method treat that
        // as if named parameters were used.  Even though the StoredProcedure was not
        // parsed that way, the user may have entered no parameters with the intention
        // of relying on all default values of all optional parameters.
        if (oldParams.size() == 0) {
        	storedProcedureCommand.setDisplayNamedParameters(true);
            namedParameters = true;
        }
        
        // Cache original input parameter expressions.  Depending on whether
        // the procedure was parsed with named or unnamed parameters, the keys
        // for this map will either be the String names of the parameters or
        // the Integer indices, as entered in the user query
        Map inputExpressions = new HashMap();
        Iterator oldParamIter = oldParams.iterator();
        while(oldParamIter.hasNext()) {
            SPParameter param = (SPParameter) oldParamIter.next();
            if(param.getExpression() != null) {
                if (namedParameters) {
                    if (inputExpressions.put(param.getName().toUpperCase(), param.getExpression()) != null) {
                    	throw new QueryResolverException(QueryPlugin.Util.getString("ExecResolver.duplicate_named_params", param.getName().toUpperCase())); //$NON-NLS-1$
                    }
                } else {
                    inputExpressions.put(new Integer(param.getIndex()), param.getExpression());
                }
            }
        }

        storedProcedureCommand.clearParameters();
        
        /*
         * Take the values set from the stored procedure implementation, and match up with the
         * types of parameter it is from the metadata and then reset the newly joined parameters
         * into the stored procedure command.  If it is a result set get those columns and place
         * them into the stored procedure command as well.
         */
        List metadataParams = storedProcedureInfo.getParameters();
        List clonedMetadataParams = new ArrayList();
        int inputParams = 0;
        Iterator paramIter = metadataParams.iterator();
        while(paramIter.hasNext()){
            SPParameter metadataParameter  = (SPParameter)paramIter.next();
            if( (metadataParameter.getParameterType()==ParameterInfo.IN) ||
                (metadataParameter.getParameterType()==ParameterInfo.INOUT)){

                inputParams++;
            }
            SPParameter clonedParam = (SPParameter)metadataParameter.clone();
            clonedMetadataParams.add(clonedParam);
            storedProcedureCommand.setParameter(clonedParam);
        }

        if(!namedParameters && (inputParams != inputExpressions.size())) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0007, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0007, new Object[] {new Integer(inputParams), new Integer(inputExpressions.size()), storedProcedureCommand.getGroup().toString()}));
        }

        // Walk through the resolved parameters and set the expressions from the
        // input parameters
        paramIter = clonedMetadataParams.iterator();
        int exprIndex = 1;
        HashSet<String> expected = new HashSet<String>();
        while(paramIter.hasNext()) {
            SPParameter param = (SPParameter) paramIter.next();
            if(param.getParameterType() == ParameterInfo.IN || param.getParameterType() == ParameterInfo.INOUT) {
                
                if (namedParameters) {
                    String nameKey = param.getName();
                    nameKey = metadata.getShortElementName(nameKey);
                    nameKey = nameKey.toUpperCase();
                    Expression expr = (Expression)inputExpressions.remove(nameKey);
                    // With named parameters, have to check on optional params and default values
                    if (expr == null) {
                    	expr = ResolverUtil.getDefault(param.getParameterSymbol(), metadata);
                    	param.setUsingDefault(true);
                    	expected.add(nameKey);
                    } 
                    param.setExpression(expr);                    
                } else {
                    Expression expr = (Expression)inputExpressions.remove(new Integer(exprIndex));
                    param.setExpression(expr);
                }
                exprIndex++;
            }
        }
        
        // Check for leftovers, i.e. params entered by user w/ wrong/unknown names
        if (namedParameters && !inputExpressions.isEmpty()) {
            throw new QueryResolverException(QueryPlugin.Util.getString("ExecResolver.invalid_named_params", inputExpressions.keySet(), expected)); //$NON-NLS-1$
        }
        
        // Create temporary metadata that defines a group based on either the stored proc
        // name or the stored query name - this will be used later during planning
        String procName = storedProcedureCommand.getProcedureName();
        List tempElements = storedProcedureCommand.getProjectedSymbols();
        boolean isVirtual = storedProcedureInfo.getQueryPlan() != null;
        discoveredMetadata.addTempGroup(procName, tempElements, isVirtual);

        // Resolve tempElements against new metadata
        GroupSymbol procGroup = new GroupSymbol(procName);
        procGroup.setProcedure(true);
        TempMetadataID tid = discoveredMetadata.getTempGroupID(procName);
        tid.setOriginalMetadataID(storedProcedureCommand.getProcedureID());
        procGroup.setMetadataID(tid);
        storedProcedureCommand.setGroup(procGroup);
    }
    
    @Override
    public GroupContext findChildCommandMetadata(ProcedureContainer container,
    		Command subCommand, TempMetadataStore discoveredMetadata,
    		QueryMetadataInterface metadata) throws QueryMetadataException,
    		QueryResolverException, MetaMatrixComponentException {

        StoredProcedure storedProcedureCommand = (StoredProcedure) container;

        // Create temporary metadata that defines a group based on either the stored proc
        // name or the stored query name - this will be used later during planning
        String procName = storedProcedureCommand.getProcedureName();
        
        GroupContext context = new GroupContext();

        // Look through parameters to find input elements - these become child metadata
        List<ElementSymbol> tempElements = new ArrayList<ElementSymbol>();
        Iterator iter = storedProcedureCommand.getParameters().iterator();
        while(iter.hasNext()) {
            SPParameter param = (SPParameter) iter.next();
            if(param.getParameterType() == ParameterInfo.IN || param.getParameterType() == ParameterInfo.INOUT) {
                ElementSymbol symbol = param.getParameterSymbol();
                tempElements.add(symbol);
            }
        }

        ProcedureContainerResolver.addScalarGroup(procName, discoveredMetadata, context, tempElements);
        
        return context;
    }

    /** 
     * @see com.metamatrix.query.resolver.ProcedureContainerResolver#resolveProceduralCommand(com.metamatrix.query.sql.lang.Command, com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.analysis.AnalysisRecord)
     */
    public void resolveProceduralCommand(Command command, TempMetadataAdapter metadata, AnalysisRecord analysis) 
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        findCommandMetadata(command, metadata.getMetadataStore(), metadata);
        
        //Resolve expressions on input parameters
        StoredProcedure storedProcedureCommand = (StoredProcedure) command;
        List params = storedProcedureCommand.getParameters();
        if(params.size() > 0) {
            GroupContext externalGroups = storedProcedureCommand.getExternalGroupContexts();
            Iterator paramIter = params.iterator();
            while(paramIter.hasNext()) {
                SPParameter param = (SPParameter) paramIter.next();
                Expression expr = param.getExpression();
                if(expr != null) {
                    for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(expr)) {
                        QueryResolver.setChildMetadata(container.getCommand(), command);
                        
                        QueryResolver.resolveCommand(container.getCommand(), Collections.EMPTY_MAP, metadata.getMetadata(), analysis);
                    }
                    ResolverVisitor.resolveLanguageObject(expr, null, externalGroups, metadata);
                    Class paramType = param.getClassType();

                    ResolverUtil.setDesiredType(expr, paramType, storedProcedureCommand);
                    
                    // Compare type of parameter expression against parameter type
                    // and add implicit conversion if necessary
                    Class exprType = expr.getType();
                    if(paramType == null || exprType == null) {
                        throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0061, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0061, storedProcedureCommand.getProcedureName(), param.getName()));
                    }
                    String tgtType = DataTypeManager.getDataTypeName(paramType);
                    String srcType = DataTypeManager.getDataTypeName(exprType);
                    Expression result = null;
                    
                    try {
                        result = ResolverUtil.convertExpression(expr, tgtType);
                    } catch (QueryResolverException e) {
                        throw new QueryResolverException(e, QueryPlugin.Util.getString("ExecResolver.Param_convert_fail", new Object[] { srcType, tgtType}));                                     //$NON-NLS-1$
                    }                                                       
                    param.setExpression(result);
                }
            }
        }
    }
    
    protected void resolveGroup(TempMetadataAdapter metadata,
                                ProcedureContainer procCommand) throws MetaMatrixComponentException,
                                                              QueryResolverException {
        //Do nothing
    }
    
    /**
     * Collect input expressions from a procedure and map them to the parameters they came from
     * @param command Procedure to collect input expressions from
     * @return Map of param name (full upper case) to Expression
     */
    public Map getVariableValues(Command command, QueryMetadataInterface metadata) {
        StoredProcedure proc = (StoredProcedure)command;

        List oldParams = proc.getInputParameters();
        Map inputMap = new HashMap();

        Iterator oldParamIter = oldParams.iterator();
        while(oldParamIter.hasNext()) {
            SPParameter param = (SPParameter) oldParamIter.next();
            String paramName = proc.getParamFullName(param).toUpperCase();
            Expression expr = param.getExpression();
            inputMap.put(paramName, expr);
        }

        return inputMap;
    }

    /** 
     * @see com.metamatrix.query.resolver.ProcedureContainerResolver#getPlan(com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.sql.symbol.GroupSymbol)
     */
    protected String getPlan(QueryMetadataInterface metadata,
                             GroupSymbol group) throws MetaMatrixComponentException,
                                               QueryMetadataException {
        StoredProcedureInfo storedProcedureInfo = metadata.getStoredProcedureInfoForProcedure(group.getCanonicalName());
        
        //if there is a query plan associated with the procedure, get it.
        QueryNode plan = storedProcedureInfo.getQueryPlan();
        
        return plan.getQuery();
    }
}
