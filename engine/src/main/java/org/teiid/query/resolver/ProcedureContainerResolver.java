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
import java.util.Collections;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.metadata.TempMetadataID.Type;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.command.UpdateProcedureResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.validator.UpdateValidator;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;


public abstract class ProcedureContainerResolver implements CommandResolver {

    public abstract void resolveProceduralCommand(Command command,
                                                  TempMetadataAdapter metadata,
                                                  AnalysisRecord analysis) throws QueryMetadataException,
                                                                          QueryResolverException,
                                                                          TeiidComponentException;
    
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
     * @throws TeiidComponentException If there is an internal error
     */
    public Command expandCommand(ProcedureContainer procCommand, QueryMetadataInterface metadata, AnalysisRecord analysis)
    throws QueryMetadataException, QueryResolverException, TeiidComponentException {
    	
        // Resolve group so we can tell whether it is an update procedure
        GroupSymbol group = procCommand.getGroup();

        Command subCommand = null;
        
        String plan = getPlan(metadata, procCommand);
        
        if (plan == null) {
            return null;
        }
        
        QueryParser parser = QueryParser.getQueryParser();
        try {
            subCommand = parser.parseUpdateProcedure(plan);
        } catch(QueryParserException e) {
            throw new QueryResolverException(e, "ERR.015.008.0045", QueryPlugin.Util.getString("ERR.015.008.0045", group)); //$NON-NLS-1$ //$NON-NLS-2$
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
        } else if (subCommand instanceof TriggerAction) {
        	TriggerAction ta = (TriggerAction)subCommand;
        	ta.setView(procCommand.getGroup());
        	TempMetadataAdapter tma = new TempMetadataAdapter(metadata, new TempMetadataStore());
        	ta.setTemporaryMetadata(tma.getMetadataStore().getData());
            GroupContext externalGroups = procCommand.getExternalGroupContexts();
            
            List<ElementSymbol> viewElements = ResolverUtil.resolveElementsInGroup(ta.getView(), metadata);
            if (procCommand instanceof Update || procCommand instanceof Insert) {
            	addChanging(tma.getMetadataStore(), externalGroups, viewElements);
            	ProcedureContainerResolver.addScalarGroup(SQLConstants.Reserved.NEW, tma.getMetadataStore(), externalGroups, viewElements);
            }
            if (procCommand instanceof Update || procCommand instanceof Delete) {
            	ProcedureContainerResolver.addScalarGroup(SQLConstants.Reserved.OLD, tma.getMetadataStore(), externalGroups, viewElements);
            }
            
            new UpdateProcedureResolver().resolveBlock(new CreateUpdateProcedureCommand(), ta.getBlock(), externalGroups, tma, analysis);

            return subCommand;
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
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    protected abstract String getPlan(QueryMetadataInterface metadata,
                           GroupSymbol group) throws TeiidComponentException,
                                             QueryMetadataException, QueryResolverException;
        
    /**
     * Find all metadata defined by this command for it's children.  This metadata should be collected 
     * in the childMetadata object.  Typical uses of this are for stored queries that define parameter
     * variables valid in subcommands. only used for inserts, updates, and deletes
     * @param metadata Metadata access
     * @param command The command to find metadata on
     * @param childMetadata The store to collect child metadata in 
     * @throws QueryMetadataException If there is a metadata problem
     * @throws QueryResolverException If the query cannot be resolved
     * @throws TeiidComponentException If there is an internal error    
     */ 
    public GroupContext findChildCommandMetadata(ProcedureContainer container, TempMetadataStore discoveredMetadata, QueryMetadataInterface metadata)
    throws QueryMetadataException, QueryResolverException, TeiidComponentException {
        // get the group on the delete statement
        GroupSymbol group = container.getGroup();
        // proceed further if it is a virtual group
            
        return createChildMetadata(discoveredMetadata, metadata, group);
    }

	static GroupContext createChildMetadata(
			TempMetadataStore discoveredMetadata,
			QueryMetadataInterface metadata, GroupSymbol group)
			throws QueryMetadataException, TeiidComponentException {
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
        addChanging(discoveredMetadata, externalGroups, elements);
		return externalGroups;
	}

	private static void addChanging(TempMetadataStore discoveredMetadata,
			GroupContext externalGroups, List<ElementSymbol> elements) {
		List<ElementSymbol> changingElements = new ArrayList<ElementSymbol>(elements.size());
        for(int i=0; i<elements.size(); i++) {
            ElementSymbol virtualElmnt = elements.get(i);
            ElementSymbol changeElement = (ElementSymbol)virtualElmnt.clone();
            changeElement.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
            changingElements.add(changeElement);
        }

        addScalarGroup(ProcedureReservedWords.CHANGING, discoveredMetadata, externalGroups, changingElements);
	}
        
    /** 
     * @see org.teiid.query.resolver.CommandResolver#resolveCommand(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.TempMetadataAdapter, org.teiid.query.analysis.AnalysisRecord, boolean)
     */
    public void resolveCommand(Command command, TempMetadataAdapter metadata, AnalysisRecord analysis, boolean resolveNullLiterals) 
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {
        
        ProcedureContainer procCommand = (ProcedureContainer)command;
        
        resolveGroup(metadata, procCommand);
        
        resolveProceduralCommand(procCommand, metadata, analysis);
        
        getPlan(metadata, procCommand);
    }

	private String getPlan(QueryMetadataInterface metadata, ProcedureContainer procCommand)
			throws TeiidComponentException, QueryMetadataException,
			QueryResolverException {
		if(!procCommand.getGroup().isTempGroupSymbol() && metadata.isVirtualGroup(procCommand.getGroup().getMetadataID())) {
            String plan = getPlan(metadata, procCommand.getGroup());
            if (plan == null && !metadata.isProcedure(procCommand.getGroup().getMetadataID())) {
            	int type = procCommand.getType();
            	//force validation
            	getUpdateInfo(procCommand.getGroup(), metadata, type);
            }
            return plan;
        }
		return null;
	}
	
	public static UpdateInfo getUpdateInfo(GroupSymbol group, QueryMetadataInterface metadata, int type) throws QueryMetadataException, TeiidComponentException, QueryResolverException {
		UpdateInfo info = getUpdateInfo(group, metadata);
		
		if (info == null) {
			return null;
		}
    	
    	if ((info.isDeleteValidationError() && type == Command.TYPE_DELETE) 
				|| (info.isUpdateValidationError() && type == Command.TYPE_UPDATE) 
				|| (info.isInsertValidationError() && type == Command.TYPE_INSERT)) {
    		String name = "Delete"; //$NON-NLS-1$
    		if (type == Command.TYPE_UPDATE) {
    			name = "Update"; //$NON-NLS-1$
    		} else if (type == Command.TYPE_INSERT) {
    			name = "Insert"; //$NON-NLS-1$
    		}
			throw new QueryResolverException("ERR.015.008.0009", QueryPlugin.Util.getString("ERR.015.008.0009", group, name)); //$NON-NLS-1$ //$NON-NLS-2$
		}
    	return info;
	}

	public static UpdateInfo getUpdateInfo(GroupSymbol group,
			QueryMetadataInterface metadata) throws TeiidComponentException,
			QueryMetadataException, QueryResolverException {
		//if this is not a view, just return null
		if(group.isTempGroupSymbol() || !metadata.isVirtualGroup(group.getMetadataID()) || !metadata.isVirtualModel(metadata.getModelID(group.getMetadataID()))) {
			return null;
		}
		String updatePlan = metadata.getUpdatePlan(group.getMetadataID());
		String deletePlan = metadata.getDeletePlan(group.getMetadataID());
		String insertPlan = metadata.getInsertPlan(group.getMetadataID());

    	UpdateInfo info = (UpdateInfo)metadata.getFromMetadataCache(group.getMetadataID(), "UpdateInfo"); //$NON-NLS-1$
    	if (info == null) {
            List<ElementSymbol> elements = ResolverUtil.resolveElementsInGroup(group, metadata);
    		UpdateValidator validator = new UpdateValidator(metadata, updatePlan, deletePlan, insertPlan);
    		info = validator.getUpdateInfo();
    		if (info.isInherentDelete() || info.isInherentInsert() || info.isInherentUpdate()) {
    			validator.validate(UpdateProcedureResolver.getQueryTransformCmd(group, metadata), elements);
    		}
    		metadata.addToMetadataCache(group.getMetadataID(), "UpdateInfo", info); //$NON-NLS-1$
    	}
		return info;
	}
    
    /** 
     * @param metadata
     * @param procCommand
     * @throws TeiidComponentException
     * @throws QueryResolverException
     */
    protected void resolveGroup(TempMetadataAdapter metadata,
                              ProcedureContainer procCommand) throws TeiidComponentException,
                                                            QueryResolverException {
        // Resolve group so we can tell whether it is an update procedure
        GroupSymbol group = procCommand.getGroup();
        ResolverUtil.resolveGroup(group, metadata);
        procCommand.setUpdateInfo(ProcedureContainerResolver.getUpdateInfo(group, metadata, procCommand.getType()));
    }

	public static GroupSymbol addScalarGroup(String name, TempMetadataStore metadata, GroupContext externalGroups, List symbols) {
		GroupSymbol variables = new GroupSymbol(name);
	    externalGroups.addGroup(variables);
	    TempMetadataID tid = metadata.addTempGroup(name, symbols);
	    tid.setMetadataType(Type.SCALAR);
	    variables.setMetadataID(tid);
	    return variables;
	}
        
}
