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

package org.teiid.query.resolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.internal.process.Request;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.command.BatchedUpdateResolver;
import org.teiid.query.resolver.command.DeleteResolver;
import org.teiid.query.resolver.command.DynamicCommandResolver;
import org.teiid.query.resolver.command.ExecResolver;
import org.teiid.query.resolver.command.InsertResolver;
import org.teiid.query.resolver.command.SetQueryResolver;
import org.teiid.query.resolver.command.SimpleQueryResolver;
import org.teiid.query.resolver.command.TempTableResolver;
import org.teiid.query.resolver.command.UpdateProcedureResolver;
import org.teiid.query.resolver.command.UpdateResolver;
import org.teiid.query.resolver.command.XMLQueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.validator.ValidationVisitor;


/**
 * <P>The QueryResolver is used between Parsing and QueryValidation. The SQL queries,
 * inserts, updates and deletes are parsed and converted into objects. The language
 * objects have variable names which resolved to fully qualified names using metadata
 * information. The resolver is also used in transforming the values in language
 * objects to their variable types defined in metadata.
 */
public class QueryResolver {

    private static final CommandResolver SIMPLE_QUERY_RESOLVER = new SimpleQueryResolver();
    private static final CommandResolver SET_QUERY_RESOLVER = new SetQueryResolver();
    private static final CommandResolver XML_QUERY_RESOLVER = new XMLQueryResolver();
    private static final CommandResolver EXEC_RESOLVER = new ExecResolver();
    private static final CommandResolver INSERT_RESOLVER = new InsertResolver();
    private static final CommandResolver UPDATE_RESOLVER = new UpdateResolver();
    private static final CommandResolver DELETE_RESOLVER = new DeleteResolver();
    private static final CommandResolver UPDATE_PROCEDURE_RESOLVER = new UpdateProcedureResolver();
    private static final CommandResolver BATCHED_UPDATE_RESOLVER = new BatchedUpdateResolver();
    private static final CommandResolver DYNAMIC_COMMAND_RESOLVER = new DynamicCommandResolver();
    private static final CommandResolver TEMP_TABLE_RESOLVER = new TempTableResolver();
    
    public static Command expandCommand(ProcedureContainer proc, QueryMetadataInterface metadata, AnalysisRecord analysisRecord) throws QueryResolverException, QueryMetadataException, TeiidComponentException {
    	ProcedureContainerResolver cr = (ProcedureContainerResolver)chooseResolver(proc, metadata);
    	Command command = cr.expandCommand(proc, metadata, analysisRecord);
    	if (command == null) {
    		return null;
    	}
		if (command instanceof CreateUpdateProcedureCommand) {
    		CreateUpdateProcedureCommand cupCommand = (CreateUpdateProcedureCommand)command;
		    cupCommand.setUserCommand(proc);
    		//if the subcommand is virtual stored procedure, it must have the same
            //projected symbol as its parent.
            if(!cupCommand.isUpdateProcedure()){
                cupCommand.setProjectedSymbols(proc.getProjectedSymbols());
            } 
    	}
    	resolveCommand(command, proc.getGroup(), proc.getType(), metadata);
    	return command;
    }

	/**
	 * This implements an algorithm to resolve all the symbols created by the
	 * parser into real metadata IDs
	 * 
	 * @param command
	 *            Command the SQL command we are running (Select, Update,
	 *            Insert, Delete)
	 * @param metadata
	 *            QueryMetadataInterface the metadata
	 * @return 
	 */
	public static TempMetadataStore resolveCommand(Command command,
			QueryMetadataInterface metadata) throws QueryResolverException,
			TeiidComponentException {

		return resolveCommand(command, metadata, true);
	}

	/**
	 * Resolve a command in a given type container and type context.
	 * @param type The {@link Command} type
	 */
    public static TempMetadataStore resolveCommand(Command currentCommand, GroupSymbol container, int type, QueryMetadataInterface metadata) throws QueryResolverException, TeiidComponentException {
    	ResolverUtil.resolveGroup(container, metadata);
    	switch (type) {
	    case Command.TYPE_QUERY:
	        QueryNode queryNode = metadata.getVirtualPlan(metadata.getGroupID(container.getCanonicalName()));
            
	        return resolveWithBindingMetadata(currentCommand, metadata, queryNode, false);
    	case Command.TYPE_INSERT:
    	case Command.TYPE_UPDATE:
    	case Command.TYPE_DELETE:
    	case Command.TYPE_STORED_PROCEDURE:
    		ProcedureContainerResolver.findChildCommandMetadata(currentCommand, container, type, metadata);
    	}
    	return resolveCommand(currentCommand, metadata, false);
    }

	/**
	 * Bindings are a poor mans input parameters.  They are represented in legacy metadata
	 * by ElementSymbols and placed positionally into the command or by alias symbols
	 * and matched by names.  After resolving bindings will be replaced with their
	 * referenced symbols (input names will not be used) and those symbols will
	 * be marked as external references.
	 */
	public static TempMetadataStore resolveWithBindingMetadata(Command currentCommand,
			QueryMetadataInterface metadata, QueryNode queryNode, boolean replaceBindings)
			throws TeiidComponentException, QueryResolverException {
		Map<ElementSymbol, ElementSymbol> symbolMap = null;
		if (queryNode.getBindings() != null && queryNode.getBindings().size() > 0) {
			symbolMap = new HashMap<ElementSymbol, ElementSymbol>();

		    // Create ElementSymbols for each InputParameter
		    final List<ElementSymbol> elements = new ArrayList<ElementSymbol>(queryNode.getBindings().size());
		    boolean positional = true;
		    for (SingleElementSymbol ses : parseBindings(queryNode)) {
		    	String name = ses.getName();
		    	if (ses instanceof AliasSymbol) {
		    		ses = ((AliasSymbol)ses).getSymbol();
		    		positional = false;
		    	}
		    	ElementSymbol elementSymbol = (ElementSymbol)ses;
		    	ResolverVisitor.resolveLanguageObject(elementSymbol, metadata);
		    	elementSymbol.setIsExternalReference(true);
		    	if (!positional) {
		    		symbolMap.put(new ElementSymbol(ProcedureReservedWords.INPUT + ElementSymbol.SEPARATOR + name), (ElementSymbol)elementSymbol.clone());
		    		symbolMap.put(new ElementSymbol(ProcedureReservedWords.INPUTS + ElementSymbol.SEPARATOR + name), (ElementSymbol)elementSymbol.clone());
		    		elementSymbol.setName(name);
		    	}
		        elements.add(elementSymbol);
		    }
		    if (positional) {
		    	ExpressionMappingVisitor emv = new ExpressionMappingVisitor(null) {
		    		@Override
		    		public Expression replaceExpression(Expression element) {
			    		if (!(element instanceof Reference)) {
			    			return element;
			    		}
			    		Reference ref = (Reference)element;
			    		if (!ref.isPositional()) {
			    			return ref;
			    		}
			    		return (ElementSymbol)elements.get(ref.getIndex()).clone();
		    		}
		    	};
		    	DeepPostOrderNavigator.doVisit(currentCommand, emv);
		    } else {
		        TempMetadataStore rootExternalStore = new TempMetadataStore();
		        
		        GroupContext externalGroups = new GroupContext();
		        
		        ProcedureContainerResolver.addScalarGroup(ProcedureReservedWords.INPUT, rootExternalStore, externalGroups, elements);
		        ProcedureContainerResolver.addScalarGroup(ProcedureReservedWords.INPUTS, rootExternalStore, externalGroups, elements);
		        QueryResolver.setChildMetadata(currentCommand, rootExternalStore.getData(), externalGroups);
		    }
		}
		TempMetadataStore result = resolveCommand(currentCommand, metadata, false);
		if (replaceBindings && symbolMap != null && !symbolMap.isEmpty()) {
			ExpressionMappingVisitor emv = new ExpressionMappingVisitor(symbolMap);
			emv.setClone(true);
			DeepPostOrderNavigator.doVisit(currentCommand, emv);
		}
		return result;
	}

	/**
	 * Bindings are a poor mans input parameters.  They are represented in legacy metadata
	 * by ElementSymbols and placed positionally into the command or by alias symbols
	 * and matched by names.
	 * @param planNode
	 * @return
	 * @throws TeiidComponentException
	 */
    public static List<SingleElementSymbol> parseBindings(QueryNode planNode) throws TeiidComponentException {
        Collection<String> bindingsCol = planNode.getBindings();
        if (bindingsCol == null) {
            return Collections.emptyList();
        }
        
        List<SingleElementSymbol> parsedBindings = new ArrayList<SingleElementSymbol>(bindingsCol.size());
        for (Iterator<String> bindings=bindingsCol.iterator(); bindings.hasNext();) {
            try {
                SingleElementSymbol binding = QueryParser.getQueryParser().parseSelectExpression(bindings.next());
                parsedBindings.add(binding);
            } catch (QueryParserException err) {
                throw new TeiidComponentException(err);
            }
        }
        return parsedBindings;
    }

    public static TempMetadataStore resolveCommand(Command currentCommand, QueryMetadataInterface metadata, boolean resolveNullLiterals)
        throws QueryResolverException, TeiidComponentException {

		LogManager.logTrace(org.teiid.logging.LogConstants.CTX_QUERY_RESOLVER, new Object[]{"Resolving command", currentCommand}); //$NON-NLS-1$
        
        TempMetadataAdapter resolverMetadata = null;
        try {
            Map tempMetadata = currentCommand.getTemporaryMetadata();
            if(tempMetadata == null) {
                currentCommand.setTemporaryMetadata(new HashMap());
            }
            
            TempMetadataStore discoveredMetadata = new TempMetadataStore(currentCommand.getTemporaryMetadata());
            
            resolverMetadata = new TempMetadataAdapter(metadata, discoveredMetadata);
            
            // Resolve external groups for command
            Collection<GroupSymbol> externalGroups = currentCommand.getAllExternalGroups();
            for (GroupSymbol extGroup : externalGroups) {
                Object metadataID = extGroup.getMetadataID();
                //make sure that the group is resolved and that it is pointing to the appropriate temp group
                //TODO: this is mainly for XML resolving since it sends external groups in unresolved
                if (metadataID == null || (!(extGroup.getMetadataID() instanceof TempMetadataID) && discoveredMetadata.getTempGroupID(extGroup.getName()) != null)) {
                    metadataID = resolverMetadata.getGroupID(extGroup.getName());
                    extGroup.setMetadataID(metadataID);
                }
            }

            CommandResolver resolver = chooseResolver(currentCommand, resolverMetadata);

            // Resolve this command
            resolver.resolveCommand(currentCommand, resolverMetadata, resolveNullLiterals);            
        } catch(QueryMetadataException e) {
            throw new QueryResolverException(e, e.getMessage());
        }

        // Flag that this command has been resolved.
        currentCommand.setIsResolved(true);
        
        return resolverMetadata.getMetadataStore();
    }

    /**
     * Method chooseResolver.
     * @param command
     * @param metadata
     * @return CommandResolver
     */
    private static CommandResolver chooseResolver(Command command, QueryMetadataInterface metadata)
        throws QueryResolverException, QueryMetadataException, TeiidComponentException {

        switch(command.getType()) {
            case Command.TYPE_QUERY:
                if(command instanceof Query) {
                    if(isXMLQuery((Query)command, metadata)) {
                        return XML_QUERY_RESOLVER;
                    }
                    return SIMPLE_QUERY_RESOLVER;
                }
                return SET_QUERY_RESOLVER;
            case Command.TYPE_INSERT:               return INSERT_RESOLVER;
            case Command.TYPE_UPDATE:               return UPDATE_RESOLVER;
            case Command.TYPE_DELETE:               return DELETE_RESOLVER;
            case Command.TYPE_STORED_PROCEDURE:     return EXEC_RESOLVER;
            case Command.TYPE_TRIGGER_ACTION:		return UPDATE_PROCEDURE_RESOLVER;
            case Command.TYPE_UPDATE_PROCEDURE:     return UPDATE_PROCEDURE_RESOLVER;
            case Command.TYPE_BATCHED_UPDATE:       return BATCHED_UPDATE_RESOLVER;
            case Command.TYPE_DYNAMIC:              return DYNAMIC_COMMAND_RESOLVER;
            case Command.TYPE_CREATE:               return TEMP_TABLE_RESOLVER;
            case Command.TYPE_DROP:                 return TEMP_TABLE_RESOLVER;
            default:
                throw new AssertionError("Unknown command type"); //$NON-NLS-1$
        }
    }

    /**
     * Check to verify if the query would return XML results.
     * @param query the query to check
     * @param metadata QueryMetadataInterface the metadata
     */
    public static boolean isXMLQuery(Query query, QueryMetadataInterface metadata)
     throws TeiidComponentException, QueryMetadataException, QueryResolverException {

        if (query.getWith() != null) {
        	return false;
        }

        // Check first group
        From from = query.getFrom();
        if(from == null){
            //select with no from
            return false;
        }
                
        if (from.getClauses().size() != 1) {
            return false;
        }
        
        FromClause clause = (FromClause)from.getClauses().get(0);
        
        if (!(clause instanceof UnaryFromClause)) {
            return false;
        }
        
        GroupSymbol symbol = ((UnaryFromClause)clause).getGroup();
        
        ResolverUtil.resolveGroup(symbol, metadata);
                
        if (symbol.isProcedure()) {
            return false;
        }
        
        Object groupID = ((UnaryFromClause)clause).getGroup().getMetadataID();

        return metadata.isXMLGroup(groupID);
    }
    
    /**
     * Resolve just a criteria.  The criteria will be modified so nothing is returned.
     * @param criteria Criteria to resolve
     * @param metadata Metadata implementation
     */
    public static void resolveCriteria(Criteria criteria, QueryMetadataInterface metadata)
        throws QueryResolverException, QueryMetadataException, TeiidComponentException {

        ResolverVisitor.resolveLanguageObject(criteria, metadata);
    }

    public static void setChildMetadata(Command subCommand, Command parent) {
        Map childMetadataMap = parent.getTemporaryMetadata();
        GroupContext parentContext = parent.getExternalGroupContexts();
        
        setChildMetadata(subCommand, childMetadataMap, parentContext);
    }
    
    public static void setChildMetadata(Command subCommand, Map parentTempMetadata, GroupContext parentContext) {
        Map tempMetadata = subCommand.getTemporaryMetadata();
        if(tempMetadata == null) {
            subCommand.setTemporaryMetadata(new HashMap(parentTempMetadata));
        } else {
            tempMetadata.putAll(parentTempMetadata);
        }
    
        subCommand.setExternalGroupContexts(parentContext);
    }
    
    public static Map<String, Expression> getVariableValues(Command command, boolean changingOnly, QueryMetadataInterface metadata) throws QueryMetadataException, QueryResolverException, TeiidComponentException {
        
        CommandResolver resolver = chooseResolver(command, metadata);
        
        if (resolver instanceof VariableResolver) {
            return ((VariableResolver)resolver).getVariableValues(command, changingOnly, metadata);
        }
        
        return Collections.EMPTY_MAP;
    }
    
	public static void resolveSubqueries(Command command,
			TempMetadataAdapter metadata, Collection<GroupSymbol> externalGroups)
			throws QueryResolverException, TeiidComponentException {
		for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(command)) {
            QueryResolver.setChildMetadata(container.getCommand(), command);
            if (externalGroups != null) {
            	container.getCommand().pushNewResolvingContext(externalGroups);
            }
            QueryResolver.resolveCommand(container.getCommand(), metadata.getMetadata(), false);
        }
	}
	
	public static Command resolveView(GroupSymbol virtualGroup, QueryNode qnode,
			String cacheString, QueryMetadataInterface qmi) throws TeiidComponentException,
			QueryMetadataException, QueryResolverException,
			QueryValidatorException {
		Command result = (Command)qmi.getFromMetadataCache(virtualGroup.getMetadataID(), "transformation/" + cacheString); //$NON-NLS-1$
        if (result != null) {
        	result = (Command)result.clone();
        } else {
        	result = qnode.getCommand();
        	List bindings = null;
            if (result == null) {
                try {
                	result = QueryParser.getQueryParser().parseCommand(qnode.getQuery());
                } catch(QueryParserException e) {
                    throw new QueryResolverException(e, "ERR.015.008.0011", QueryPlugin.Util.getString("ERR.015.008.0011", qnode.getGroupName())); //$NON-NLS-1$ //$NON-NLS-2$
                }
                
                bindings = qnode.getBindings();
            }
            if (bindings != null && !bindings.isEmpty()) {
            	QueryResolver.resolveWithBindingMetadata(result, qmi, qnode, true);
            } else {
            	QueryResolver.resolveCommand(result, qmi, false);
            }
	        Request.validateWithVisitor(new ValidationVisitor(), qmi, result);
	        qmi.addToMetadataCache(virtualGroup.getMetadataID(), "transformation/" + cacheString, result.clone()); //$NON-NLS-1$
        }
		return result;
	}
	
}
