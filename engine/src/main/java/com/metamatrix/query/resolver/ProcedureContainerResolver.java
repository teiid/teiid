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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.GroupContext;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;

public abstract class ProcedureContainerResolver implements CommandResolver {

    public abstract void resolveProceduralCommand(Command command,
                                                  TempMetadataAdapter metadata,
                                                  AnalysisRecord analysis) throws QueryMetadataException,
                                                                          QueryResolverException,
                                                                          MetaMatrixComponentException;
    
    /**
     * Expand a command by finding and attaching all subcommands to the command.  If
     * some initial resolution must be done for this to be accomplished, that is ok, 
     * but it should be kept to a minimum.
     * @param command The command to expand
     * @param useMetadataCommands True if resolver should use metadata commands to completely resolve
     * @param metadata Metadata access
     * @param analysis The analysis record that will be filled in if doing annotation.
     * 
     * @throws QueryMetadataException If there is a metadata problem
     * @throws QueryResolverException If the query cannot be resolved
     * @throws MetaMatrixComponentException If there is an internal error
     */
    public Command expandCommand(ProcedureContainer procCommand, QueryMetadataInterface metadata, AnalysisRecord analysis)
    throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {
    	
        // Resolve group so we can tell whether it is an update procedure
        GroupSymbol group = procCommand.getGroup();

        Command subCommand = null;
        
        String plan = getPlan(metadata, procCommand);
        
        if (plan == null) {
            return null;
        }
        
        QueryParser parser = QueryParser.getQueryParser();
        try {
            subCommand = parser.parseCommand(plan);
        } catch(QueryParserException e) {
            throw new QueryResolverException(e, ErrorMessageKeys.RESOLVER_0045, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0045, group));
        }
        
        if(subCommand instanceof CreateUpdateProcedureCommand){
            CreateUpdateProcedureCommand cupCommand = (CreateUpdateProcedureCommand)subCommand;
            //if the subcommand is virtual stored procedure, it must have the same
            //projected symbol as its parent.
            if(!cupCommand.isUpdateProcedure()){
                cupCommand.setProjectedSymbols(procCommand.getProjectedSymbols());
            } 
            
            cupCommand.setVirtualGroup(procCommand.getGroup());
            cupCommand.setUserCommand(procCommand);
        } 
        
        //find the childMetadata using a clean metadata store
        TempMetadataStore childMetadata = new TempMetadataStore();
        QueryMetadataInterface resolveMetadata = new TempMetadataAdapter(metadata, childMetadata);

        GroupContext externalGroups = findChildCommandMetadata(procCommand, childMetadata, resolveMetadata);
        
        QueryResolver.setChildMetadata(subCommand, childMetadata.getData(), externalGroups);
        
        QueryResolver.resolveCommand(subCommand, Collections.EMPTY_MAP, metadata, analysis);
        
        return subCommand;
    }

    /** 
     * For a given resolver, this returns the unparsed command.
     * 
     * @param metadata
     * @param group
     * @return
     * @throws MetaMatrixComponentException
     * @throws QueryMetadataException
     */
    protected abstract String getPlan(QueryMetadataInterface metadata,
                           GroupSymbol group) throws MetaMatrixComponentException,
                                             QueryMetadataException;
        
    /**
     * Find all metadata defined by this command for it's children.  This metadata should be collected 
     * in the childMetadata object.  Typical uses of this are for stored queries that define parameter
     * variables valid in subcommands. only used for inserts, updates, and deletes
     * @param metadata Metadata access
     * @param command The command to find metadata on
     * @param childMetadata The store to collect child metadata in 
     * @throws QueryMetadataException If there is a metadata problem
     * @throws QueryResolverException If the query cannot be resolved
     * @throws MetaMatrixComponentException If there is an internal error    
     */ 
    public GroupContext findChildCommandMetadata(ProcedureContainer container, TempMetadataStore discoveredMetadata, QueryMetadataInterface metadata)
    throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {
        // get the group on the delete statement
        GroupSymbol group = container.getGroup();
        // proceed further if it is a virtual group
            
        return createChildMetadata(discoveredMetadata, metadata, group);
    }

	static GroupContext createChildMetadata(
			TempMetadataStore discoveredMetadata,
			QueryMetadataInterface metadata, GroupSymbol group)
			throws QueryMetadataException, MetaMatrixComponentException {
		GroupContext externalGroups = new GroupContext();
        
        //Look up elements for the virtual group
        List<ElementSymbol> elements = ResolverUtil.resolveElementsInGroup(group, metadata);

        // Create the INPUT variables
        List<ElementSymbol> inputElments = new ArrayList<ElementSymbol>(elements.size());
        for(int i=0; i<elements.size(); i++) {
            ElementSymbol virtualElmnt = elements.get(i);
            ElementSymbol inputElement = (ElementSymbol)virtualElmnt.clone();
            inputElments.add(inputElement);
        }

        addScalarGroup(ProcedureReservedWords.INPUT, discoveredMetadata, externalGroups, inputElments);
        addScalarGroup(ProcedureReservedWords.INPUTS, discoveredMetadata, externalGroups, inputElments);

        // Switch type to be boolean for all CHANGING variables
        List<ElementSymbol> changingElements = new ArrayList<ElementSymbol>(elements.size());
        for(int i=0; i<elements.size(); i++) {
            ElementSymbol virtualElmnt = elements.get(i);
            ElementSymbol changeElement = (ElementSymbol)virtualElmnt.clone();
            changeElement.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
            changingElements.add(changeElement);
        }

        addScalarGroup(ProcedureReservedWords.CHANGING, discoveredMetadata, externalGroups, changingElements);
		return externalGroups;
	}
        
    /** 
     * @see com.metamatrix.query.resolver.CommandResolver#resolveCommand(com.metamatrix.query.sql.lang.Command, com.metamatrix.query.metadata.TempMetadataAdapter, com.metamatrix.query.analysis.AnalysisRecord, boolean)
     */
    public void resolveCommand(Command command, TempMetadataAdapter metadata, AnalysisRecord analysis, boolean resolveNullLiterals) 
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {
        
        ProcedureContainer procCommand = (ProcedureContainer)command;
        
        resolveGroup(metadata, procCommand);
        
        resolveProceduralCommand(procCommand, metadata, analysis);
        
        getPlan(metadata, procCommand);
    }

	private String getPlan(QueryMetadataInterface metadata, ProcedureContainer procCommand)
			throws MetaMatrixComponentException, QueryMetadataException,
			QueryResolverException {
		if(!procCommand.getGroup().isTempGroupSymbol() && metadata.isVirtualGroup(procCommand.getGroup().getMetadataID())) {
            String plan = getPlan(metadata, procCommand.getGroup());
            
            if(plan == null) {
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0009, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0009, procCommand.getGroup(), procCommand.getClass().getSimpleName()));
            }
            return plan;
        }
		return null;
	}
    
    /** 
     * @param metadata
     * @param procCommand
     * @throws MetaMatrixComponentException
     * @throws QueryResolverException
     */
    protected void resolveGroup(TempMetadataAdapter metadata,
                              ProcedureContainer procCommand) throws MetaMatrixComponentException,
                                                            QueryResolverException {
        // Resolve group so we can tell whether it is an update procedure
        GroupSymbol group = procCommand.getGroup();
        ResolverUtil.resolveGroup(group, metadata);
    }

	public static GroupSymbol addScalarGroup(String name, TempMetadataStore metadata, GroupContext externalGroups, List symbols) {
		GroupSymbol variables = new GroupSymbol(name);
	    externalGroups.addGroup(variables);
	    TempMetadataID tid = metadata.addTempGroup(name, symbols);
	    tid.setScalarGroup(true);
	    variables.setMetadataID(tid);
	    return variables;
	}
        
}
