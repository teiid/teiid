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

package org.teiid.query.resolver.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.resolver.ProcedureContainerResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.VariableResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.SingleElementSymbol;


/**
 * This class knows how to expand and resolve INSERT commands.
 */
public class InsertResolver extends ProcedureContainerResolver implements VariableResolver {

    /**
     * Resolve an INSERT.  Need to resolve elements, constants, types, etc.
     * @see org.teiid.query.resolver.ProcedureContainerResolver#resolveProceduralCommand(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.TempMetadataAdapter)
     */
    public void resolveProceduralCommand(Command command, TempMetadataAdapter metadata) 
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {

        // Cast to known type
        Insert insert = (Insert) command;
        
        if (insert.getValues() != null) {
        	QueryResolver.resolveSubqueries(command, metadata, null);
	        //variables and values must be resolved separately to account for implicitly defined temp groups
	        resolveList(insert.getValues(), metadata, insert.getExternalGroupContexts(), null);
    	}
        //resolve subquery if there
        if(insert.getQueryExpression() != null) {
        	QueryResolver.setChildMetadata(insert.getQueryExpression(), command);
            
            QueryResolver.resolveCommand(insert.getQueryExpression(), metadata.getMetadata(), false);
        }

        Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        groups.add(insert.getGroup());
        
     // resolve any functions in the values
        List values = insert.getValues();
        boolean usingQuery = insert.getQueryExpression() != null;
        
        if (usingQuery) {
            values = insert.getQueryExpression().getProjectedSymbols();
        }
        
        if (insert.getVariables().isEmpty()) {
            if (insert.getGroup().isResolved()) {
                List<ElementSymbol> variables = ResolverUtil.resolveElementsInGroup(insert.getGroup(), metadata);
                for (Iterator<ElementSymbol> i = variables.iterator(); i.hasNext();) {
                    insert.addVariable(i.next().clone());
                }
            } else {
                for (int i = 0; i < values.size(); i++) {
                	if (usingQuery) {
                		SingleElementSymbol ses = (SingleElementSymbol)values.get(i);
                    	ElementSymbol es = new ElementSymbol(ses.getShortName()); 
                    	es.setType(ses.getType());
                    	insert.addVariable(es);
                    } else {
                    	insert.addVariable(new ElementSymbol("expr" + i)); //$NON-NLS-1$
                    }
                }
            }
        } else if (insert.getGroup().isResolved()) {
            resolveVariables(metadata, insert, groups);
        }

        resolveTypes(insert, metadata, values, usingQuery);
        
        if (!insert.getGroup().isResolved()) { //define the implicit temp group
            if(insert.getQueryExpression() != null) {
                ResolverUtil.resolveImplicitTempGroup(metadata, insert.getGroup(), insert.getQueryExpression().getProjectedSymbols());
            }else {
                ResolverUtil.resolveImplicitTempGroup(metadata, insert.getGroup(), insert.getVariables());
            }
            resolveVariables(metadata, insert, groups);
            
            //ensure that the types match
            resolveTypes(insert, metadata, values, usingQuery);
        }
        
        if (insert.getQueryExpression() != null && metadata.isVirtualGroup(insert.getGroup().getMetadataID())) {
        	List<Reference> references = new ArrayList<Reference>(insert.getVariables().size());
        	for (int i = 0; i < insert.getVariables().size(); i++) {
        		Reference ref = new Reference(i);
        		ref.setType(insert.getVariables().get(i).getType());
				references.add(ref);
			}
        	insert.setValues(references);
        }
    }

    private void resolveVariables(TempMetadataAdapter metadata,
                                  Insert insert,
                                  Set<GroupSymbol> groups) throws TeiidComponentException,
                                             QueryResolverException {
        try {
            resolveList(insert.getVariables(), metadata, null, groups);
        } catch (QueryResolverException e) {
            throw new QueryResolverException(e, QueryPlugin.Util.getString("ERR.015.012.0054", insert.getGroup(), e.getUnresolvedSymbols())); //$NON-NLS-1$
        }
    }

    private void resolveList(Collection elements, TempMetadataAdapter metadata,
                                  GroupContext externalGroups, Set<GroupSymbol> groups) throws TeiidComponentException,
                                             QueryResolverException {
        for (Iterator i = elements.iterator(); i.hasNext();) {
            Expression expr = (Expression)i.next();
            ResolverVisitor.resolveLanguageObject(expr, groups, externalGroups, metadata);
        }
    }
    
    /** 
     * @param insert
     * @param values 
     * @param usingQuery 
     * @throws QueryResolverException
     */
    public void resolveTypes(Insert insert, TempMetadataAdapter metadata, List values, boolean usingQuery) throws QueryResolverException {
        
        List newValues = new ArrayList(values.size());
        
        // check that # of variables == # of values
        if(values.size() != insert.getVariables().size()) {
            throw new QueryResolverException("ERR.015.008.0010", QueryPlugin.Util.getString("ERR.015.008.0010", insert.getVariables().size(), values.size())); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        Iterator valueIter = values.iterator();
        Iterator<ElementSymbol> varIter = insert.getVariables().iterator();
        while(valueIter.hasNext()) {
            // Walk through both elements and expressions, which should match up
			Expression expression = (Expression) valueIter.next();
			ElementSymbol element = varIter.next();
			
			if (!usingQuery) {
				ResolverUtil.setDesiredType(expression, element.getType(), insert);
			}

            if(element.getType() != null && expression.getType() != null) {
                String elementTypeName = DataTypeManager.getDataTypeName(element.getType());
                if (!usingQuery) {
                    newValues.add(ResolverUtil.convertExpression(expression, elementTypeName, metadata));
                } else if (element.getType() != expression.getType()
                           && !DataTypeManager.isImplicitConversion(DataTypeManager.getDataTypeName(expression.getType()),
                                                                    DataTypeManager.getDataTypeName(element.getType()))) {
                    //TODO: a special case here is a projected literal
                    throw new QueryResolverException(QueryPlugin.Util.getString("InsertResolver.cant_convert_query_type", new Object[] {expression, expression.getType().getName(), element, element.getType().getName()})); //$NON-NLS-1$
                }
            } else if (element.getType() == null && expression.getType() != null)  {
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
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    protected String getPlan(QueryMetadataInterface metadata,
                           GroupSymbol group) throws TeiidComponentException,
                                             QueryMetadataException {
        return metadata.getInsertPlan(group.getMetadataID());
    }
    
    /** 
     * @see org.teiid.query.resolver.ProcedureContainerResolver#resolveGroup(org.teiid.query.metadata.TempMetadataAdapter, org.teiid.query.sql.lang.ProcedureContainer)
     */
    protected void resolveGroup(TempMetadataAdapter metadata,
                                ProcedureContainer procCommand) throws TeiidComponentException,
                                                              QueryResolverException {
        if (!procCommand.getGroup().isImplicitTempGroupSymbol() || metadata.getMetadataStore().getTempGroupID(procCommand.getGroup().getName()) != null) {
            super.resolveGroup(metadata, procCommand);
        }
    }

    /** 
     * @throws TeiidComponentException 
     * @throws QueryResolverException 
     * @throws QueryMetadataException 
     * @see org.teiid.query.resolver.CommandResolver#getVariableValues(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.QueryMetadataInterface)
     */
    public Map<ElementSymbol, Expression> getVariableValues(Command command, boolean changingOnly,
                                 QueryMetadataInterface metadata) throws QueryMetadataException, QueryResolverException, TeiidComponentException {
        
        Insert insert = (Insert) command;
        
        Map<ElementSymbol, Expression> result = new HashMap<ElementSymbol, Expression>();
        
        // iterate over the variables and values they should be the same number
        Iterator<ElementSymbol> varIter = insert.getVariables().iterator();
        Iterator valIter = null;
        if (insert.getQueryExpression() != null) {
        	valIter = insert.getQueryExpression().getProjectedSymbols().iterator();
        } else {
            valIter = insert.getValues().iterator();
        }
        while (varIter.hasNext()) {
            ElementSymbol varSymbol = varIter.next().clone();
            
            varSymbol.getGroupSymbol().setName(ProcedureReservedWords.CHANGING);
            result.put(varSymbol, new Constant(Boolean.TRUE));
            if (!changingOnly) {
            	varSymbol = varSymbol.clone();
            	varSymbol.getGroupSymbol().setName(ProcedureReservedWords.INPUTS);
            	result.put(varSymbol, (Expression)valIter.next());
            }
        }
        
        Collection<ElementSymbol> insertElmnts = ResolverUtil.resolveElementsInGroup(insert.getGroup(), metadata);

        insertElmnts.removeAll(insert.getVariables());

        Iterator<ElementSymbol> defaultIter = insertElmnts.iterator();
        while(defaultIter.hasNext()) {
            ElementSymbol varSymbol = defaultIter.next().clone();
            varSymbol.getGroupSymbol().setName(ProcedureReservedWords.CHANGING);
            result.put(varSymbol, new Constant(Boolean.FALSE));
            
            if (!changingOnly) {
                Expression value = ResolverUtil.getDefault(varSymbol, metadata);
            	varSymbol = varSymbol.clone();
            	varSymbol.getGroupSymbol().setName(ProcedureReservedWords.INPUTS);
            	result.put(varSymbol, value);
            }
        }
        
        return result;
    }
    
}
