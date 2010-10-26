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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.logging.LogManager;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
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
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


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
    	return cr.expandCommand(proc, metadata, analysisRecord);
    }

    /**
     * This implements an algorithm to resolve all the symbols created by the parser into real metadata IDs
     * @param command Command the SQL command we are running (Select, Update, Insert, Delete)
     * @param metadata QueryMetadataInterface the metadata
     * @param analysis The analysis record which can be used to add anotations and debug information.
     */
     public static void resolveCommand(Command command, QueryMetadataInterface metadata, AnalysisRecord analysis)
         throws QueryResolverException, TeiidComponentException {

         resolveCommand(command, Collections.EMPTY_MAP, metadata, analysis);
     }

     /**
      * This implements an algorithm to resolve all the symbols created by the parser into real metadata IDs
      * @param command Command the SQL command we are running (Select, Update, Insert, Delete)
      * @param metadata QueryMetadataInterface the metadata
      */
      public static void resolveCommand(Command command, QueryMetadataInterface metadata)
          throws QueryResolverException, TeiidComponentException {

          resolveCommand(command, Collections.EMPTY_MAP, metadata, AnalysisRecord.createNonRecordingRecord());
      }

   /**
    * This implements an algorithm to resolve all the symbols created by the parser into real metadata IDs
 * @param externalMetadata Map of GroupSymbol to a List of ElementSymbol that identifies
    * valid external groups that can be resolved against. Any elements resolved against external
    * groups will be treated as variables
 * @param metadata QueryMetadataInterface the metadata
 * @param analysis The analysis record which can be used to add anotations and debug information.
 * @param command Command the SQL command we are running (Select, Update, Insert, Delete)
    */
    public static TempMetadataStore resolveCommand(Command currentCommand, Map externalMetadata, QueryMetadataInterface metadata, 
                                                     AnalysisRecord analysis)
                       throws QueryResolverException, TeiidComponentException {
        return resolveCommand(currentCommand, externalMetadata, metadata, analysis, true);
    }
      
    public static TempMetadataStore resolveCommand(Command currentCommand, Map externalMetadata, QueryMetadataInterface metadata, 
                                      AnalysisRecord analysis, boolean resolveNullLiterals)
        throws QueryResolverException, TeiidComponentException {

		LogManager.logTrace(org.teiid.logging.LogConstants.CTX_QUERY_RESOLVER, new Object[]{"Resolving command", currentCommand}); //$NON-NLS-1$
        
        TempMetadataAdapter resolverMetadata = null;
        try {
            TempMetadataStore rootExternalStore = new TempMetadataStore();
            if(externalMetadata != null) {
                for(Iterator iter = externalMetadata.entrySet().iterator(); iter.hasNext();) {
                    Map.Entry entry = (Map.Entry)iter.next();
                    GroupSymbol group = (GroupSymbol) entry.getKey();
                    List elements = (List) entry.getValue();
                    rootExternalStore.addTempGroup(group.getName(), elements);
                    currentCommand.addExternalGroupToContext(group);
                }
            } 
            Map tempMetadata = currentCommand.getTemporaryMetadata();
            if(tempMetadata == null) {
                currentCommand.setTemporaryMetadata(new HashMap(rootExternalStore.getData()));
            } else {
                tempMetadata.putAll(rootExternalStore.getData());
            }
            
            TempMetadataStore discoveredMetadata = new TempMetadataStore(currentCommand.getTemporaryMetadata());
            
            resolverMetadata = new TempMetadataAdapter(metadata, discoveredMetadata);
            
            // Resolve external groups for command
            Collection externalGroups = currentCommand.getAllExternalGroups();
            Iterator extIter = externalGroups.iterator();
            while(extIter.hasNext()) {
                GroupSymbol extGroup = (GroupSymbol) extIter.next();
                Object metadataID = extGroup.getMetadataID();
                //make sure that the group is resolved and that it is pointing to the appropriate temp group
                if (metadataID == null || (!(extGroup.getMetadataID() instanceof TempMetadataID) && discoveredMetadata.getTempGroupID(extGroup.getName()) != null)) {
                    metadataID = resolverMetadata.getGroupID(extGroup.getName());
                    extGroup.setMetadataID(metadataID);
                }
            }

            CommandResolver resolver = chooseResolver(currentCommand, resolverMetadata);

            // Resolve this command
            resolver.resolveCommand(currentCommand, resolverMetadata, analysis, resolveNullLiterals);            
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

        if (query.getWith() != null && !query.getWith().isEmpty()) {
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
    
    public static Map getVariableValues(Command command, QueryMetadataInterface metadata) throws QueryMetadataException, QueryResolverException, TeiidComponentException {
        
        CommandResolver resolver = chooseResolver(command, metadata);
        
        if (resolver instanceof VariableResolver) {
            return ((VariableResolver)resolver).getVariableValues(command, metadata);
        }
        
        return Collections.EMPTY_MAP;
    }
    
	public static void resolveSubqueries(Command command,
			TempMetadataAdapter metadata, AnalysisRecord analysis)
			throws QueryResolverException, TeiidComponentException {
		for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(command)) {
            QueryResolver.setChildMetadata(container.getCommand(), command);
            
            QueryResolver.resolveCommand(container.getCommand(), Collections.EMPTY_MAP, metadata.getMetadata(), analysis);
        }
	}

}
