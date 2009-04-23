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

package com.metamatrix.query.resolver;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.resolver.command.BatchedUpdateResolver;
import com.metamatrix.query.resolver.command.DeleteResolver;
import com.metamatrix.query.resolver.command.DynamicCommandResolver;
import com.metamatrix.query.resolver.command.ExecResolver;
import com.metamatrix.query.resolver.command.InsertResolver;
import com.metamatrix.query.resolver.command.SetQueryResolver;
import com.metamatrix.query.resolver.command.SimpleQueryResolver;
import com.metamatrix.query.resolver.command.TempTableResolver;
import com.metamatrix.query.resolver.command.UpdateProcedureResolver;
import com.metamatrix.query.resolver.command.UpdateResolver;
import com.metamatrix.query.resolver.command.XMLQueryResolver;
import com.metamatrix.query.resolver.command.XQueryResolver;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.FromClause;
import com.metamatrix.query.sql.lang.GroupContext;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.util.LogConstants;

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
    private static final CommandResolver X_QUERY_RESOLVER = new XQueryResolver();
    private static final CommandResolver BATCHED_UPDATE_RESOLVER = new BatchedUpdateResolver();
    private static final CommandResolver DYNAMIC_COMMAND_RESOLVER = new DynamicCommandResolver();
    private static final CommandResolver TEMP_TABLE_RESOLVER = new TempTableResolver();

    /**
     * This implements an algorithm to resolve all the symbols created by the parser into real metadata IDs
     * @param command Command the SQL command we are running (Select, Update, Insert, Delete)
     * @param metadata QueryMetadataInterface the metadata
     * @param analysis The analysis record which can be used to add anotations and debug information.
     */
     public static void resolveCommand(Command command, QueryMetadataInterface metadata, AnalysisRecord analysis)
         throws QueryResolverException, MetaMatrixComponentException {

         resolveCommand(command, Collections.EMPTY_MAP, true, metadata, analysis);
     }

     /**
      * This implements an algorithm to resolve all the symbols created by the parser into real metadata IDs
      * @param command Command the SQL command we are running (Select, Update, Insert, Delete)
      * @param metadata QueryMetadataInterface the metadata
      */
      public static void resolveCommand(Command command, QueryMetadataInterface metadata)
          throws QueryResolverException, MetaMatrixComponentException {

          resolveCommand(command, Collections.EMPTY_MAP, true, metadata, AnalysisRecord.createNonRecordingRecord());
      }

   /**
    * This implements an algorithm to resolve all the symbols created by the parser into real metadata IDs
    * @param command Command the SQL command we are running (Select, Update, Insert, Delete)
    * @param externalMetadata Map of GroupSymbol to a List of ElementSymbol that identifies
    * valid external groups that can be resolved against. Any elements resolved against external
    * groups will be treated as variables
    * @param useMetadataCommands True if resolver should use metadata commands to completely resolve
    * the command tree all the way to physical.  False if resolver should resolve only the visible command
    * @param metadata QueryMetadataInterface the metadata
    * @param analysis The analysis record which can be used to add anotations and debug information.
    */
    public static TempMetadataStore resolveCommand(Command currentCommand, Map externalMetadata, boolean useMetadataCommands, 
                                                     QueryMetadataInterface metadata, AnalysisRecord analysis)
                       throws QueryResolverException, MetaMatrixComponentException {
        return resolveCommand(currentCommand, externalMetadata, useMetadataCommands, metadata, analysis, true);
    }
      
    public static TempMetadataStore resolveCommand(Command currentCommand, Map externalMetadata, boolean useMetadataCommands, 
                                      QueryMetadataInterface metadata, AnalysisRecord analysis, boolean resolveNullLiterals)
        throws QueryResolverException, MetaMatrixComponentException {

		LogManager.logTrace(LogConstants.CTX_QUERY_RESOLVER, new Object[]{"Resolving command", currentCommand}); //$NON-NLS-1$
        
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
            resolver.resolveCommand(currentCommand, useMetadataCommands, resolverMetadata, analysis, resolveNullLiterals);            
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
        throws QueryResolverException, QueryMetadataException, MetaMatrixComponentException {

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
            case Command.TYPE_XQUERY:               return X_QUERY_RESOLVER;
            case Command.TYPE_BATCHED_UPDATE:       return BATCHED_UPDATE_RESOLVER;
            case Command.TYPE_DYNAMIC:              return DYNAMIC_COMMAND_RESOLVER;
            case Command.TYPE_CREATE:               return TEMP_TABLE_RESOLVER;
            case Command.TYPE_DROP:                 return TEMP_TABLE_RESOLVER;
            default:
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0002, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0002, command.getType()));
        }
    }

    /**
     * Check to verify if the query would return XML results.
     * @param query the query to check
     * @param metadata QueryMetadataInterface the metadata
     */
    static boolean isXMLQuery(Query query, QueryMetadataInterface metadata)
     throws MetaMatrixComponentException, QueryMetadataException, QueryResolverException {

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
        throws QueryResolverException, QueryMetadataException, MetaMatrixComponentException {

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
    
    public static Map getVariableValues(Command command, QueryMetadataInterface metadata) throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {
        
        CommandResolver resolver = chooseResolver(command, metadata);
        
        if (resolver instanceof VariableResolver) {
            return ((VariableResolver)resolver).getVariableValues(command, metadata);
        }
        
        return Collections.EMPTY_MAP;
    }

}
