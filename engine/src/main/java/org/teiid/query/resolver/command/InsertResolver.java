/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.teiid.language.SQLConstants;
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
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.util.SymbolMap;


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
        boolean usingQuery = insert.getQueryExpression() != null;
        QueryResolverException resolveQueryException = null;
        //resolve subquery if there
        if(usingQuery) {
        	QueryResolver.setChildMetadata(insert.getQueryExpression(), command);
            
            QueryResolver.resolveCommand(insert.getQueryExpression(), metadata.getMetadata(), false);
        }

        Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        groups.add(insert.getGroup());
        
     // resolve any functions in the values
        List values = insert.getValues();
        
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
                		Expression ses = (Expression)values.get(i);
                    	ElementSymbol es = new ElementSymbol(Symbol.getShortName(ses)); 
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
        
        if (usingQuery && insert.getQueryExpression() instanceof SetQuery) {
            //now that the first branch is set, we need to make sure that all branches conform
            QueryResolver.resolveCommand(insert.getQueryExpression(), metadata.getMetadata(), false);
            resolveTypes(insert, metadata, values, usingQuery);
        }
        
        if (!insert.getGroup().isResolved()) { //define the implicit temp group
            ResolverUtil.resolveImplicitTempGroup(metadata, insert.getGroup(), insert.getVariables());
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
             throw new QueryResolverException(QueryPlugin.Event.TEIID30126, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30126, insert.getGroup(), e.getUnresolvedSymbols()));
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
             throw new QueryResolverException(QueryPlugin.Event.TEIID30127, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30127, insert.getVariables().size(), values.size()));
        }
        
        Iterator valueIter = values.iterator();
        Iterator<ElementSymbol> varIter = insert.getVariables().iterator();
        while(valueIter.hasNext()) {
            // Walk through both elements and expressions, which should match up
			Expression expression = (Expression) valueIter.next();
			ElementSymbol element = varIter.next();
			
			if (expression.getType() == null) {
				ResolverUtil.setDesiredType(SymbolMap.getExpression(expression), element.getType(), insert);
			}

            if(element.getType() != null && expression.getType() != null) {
                String elementTypeName = DataTypeManager.getDataTypeName(element.getType());
                if (!usingQuery) {
                    newValues.add(ResolverUtil.convertExpression(expression, elementTypeName, metadata));
                } else if (element.getType() != expression.getType()
                           && !DataTypeManager.isImplicitConversion(DataTypeManager.getDataTypeName(expression.getType()),
                                                                    DataTypeManager.getDataTypeName(element.getType()))) {
                    //TODO: a special case here is a projected literal
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30128, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30128, new Object[] {expression, expression.getType().getName(), element, element.getType().getName()}));
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
    	try { 
    		super.resolveGroup(metadata, procCommand);
    	} catch (QueryResolverException e) {
            if (!procCommand.getGroup().isImplicitTempGroupSymbol() || metadata.getMetadataStore().getTempGroupID(procCommand.getGroup().getName()) != null) {
                throw e;
            }
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
            ElementSymbol next = varIter.next();
			ElementSymbol varSymbol = next.clone();
            varSymbol.getGroupSymbol().setName(ProcedureReservedWords.CHANGING);
            varSymbol.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
            result.put(varSymbol, new Constant(Boolean.TRUE));
            if (!changingOnly) {
            	varSymbol = next.clone();
            	varSymbol.getGroupSymbol().setName(SQLConstants.Reserved.NEW);
            	result.put(varSymbol, SymbolMap.getExpression((Expression)valIter.next()));
            }
        }
        
        Collection<ElementSymbol> insertElmnts = ResolverUtil.resolveElementsInGroup(insert.getGroup(), metadata);

        insertElmnts.removeAll(insert.getVariables());

        Iterator<ElementSymbol> defaultIter = insertElmnts.iterator();
        while(defaultIter.hasNext()) {
        	ElementSymbol next = defaultIter.next();
 			ElementSymbol varSymbol = next.clone();
            varSymbol.getGroupSymbol().setName(ProcedureReservedWords.CHANGING);
            varSymbol.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
            result.put(varSymbol, new Constant(Boolean.FALSE));
            if (!changingOnly) {
            	varSymbol = next.clone();
            	Expression value = ResolverUtil.getDefault(varSymbol, metadata);
            	varSymbol.getGroupSymbol().setName(SQLConstants.Reserved.NEW);
            	result.put(varSymbol, value);
            }
        }
        
        return result;
    }
    
}
