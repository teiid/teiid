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
import java.util.Arrays;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.SQLConstants;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.StoredProcedureInfo;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.metadata.TempMetadataID.Type;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.command.UpdateProcedureResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.validator.UpdateValidator;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;
import org.teiid.query.validator.UpdateValidator.UpdateType;


public abstract class ProcedureContainerResolver implements CommandResolver {

    public abstract void resolveProceduralCommand(Command command,
                                                  TempMetadataAdapter metadata) throws QueryMetadataException,
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
            throw new QueryResolverException(e, "ERR.015.008.0045", QueryPlugin.Util.getString("ERR.015.008.0045", group, procCommand.getClass().getSimpleName())); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
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
        
	public static void addChanging(TempMetadataStore discoveredMetadata,
			GroupContext externalGroups, List<ElementSymbol> elements) {
		List<ElementSymbol> changingElements = new ArrayList<ElementSymbol>(elements.size());
        for(int i=0; i<elements.size(); i++) {
            ElementSymbol virtualElmnt = elements.get(i);
            ElementSymbol changeElement = (ElementSymbol)virtualElmnt.clone();
            changeElement.setType(DataTypeManager.DefaultDataClasses.BOOLEAN);
            changingElements.add(changeElement);
        }

        addScalarGroup(ProcedureReservedWords.CHANGING, discoveredMetadata, externalGroups, changingElements, false);
	}
        
    /** 
     * @see org.teiid.query.resolver.CommandResolver#resolveCommand(org.teiid.query.sql.lang.Command, org.teiid.query.metadata.TempMetadataAdapter, boolean)
     */
    public void resolveCommand(Command command, TempMetadataAdapter metadata, boolean resolveNullLiterals) 
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {
        
        ProcedureContainer procCommand = (ProcedureContainer)command;
        
        resolveGroup(metadata, procCommand);
        
        resolveProceduralCommand(procCommand, metadata);
        
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
    		UpdateValidator validator = new UpdateValidator(metadata, determineType(insertPlan), determineType(updatePlan), determineType(deletePlan));
    		info = validator.getUpdateInfo();
			validator.validate(UpdateProcedureResolver.getQueryTransformCmd(group, metadata), elements);
    		metadata.addToMetadataCache(group.getMetadataID(), "UpdateInfo", info); //$NON-NLS-1$
    	}
		return info;
	}
	
	private static UpdateType determineType(String plan) {
		UpdateType type = UpdateType.INHERENT;
		if (plan != null) {
			if (StringUtil.startsWithIgnoreCase(plan, SQLConstants.Reserved.CREATE)) {
				type = UpdateType.UPDATE_PROCEDURE;
			} else {
				type = UpdateType.INSTEAD_OF;
			}
		}
		return type;
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

    public static GroupSymbol addScalarGroup(String name, TempMetadataStore metadata, GroupContext externalGroups, List<? extends SingleElementSymbol> symbols) {
    	return addScalarGroup(name, metadata, externalGroups, symbols, true);
    }
    
	public static GroupSymbol addScalarGroup(String name, TempMetadataStore metadata, GroupContext externalGroups, List<? extends SingleElementSymbol> symbols, boolean updatable) {
		boolean[] updateArray = new boolean[symbols.size()];
		if (updatable) {
			Arrays.fill(updateArray, true);
		}
		return addScalarGroup(name, metadata, externalGroups, symbols, updateArray);
	}
	
	public static GroupSymbol addScalarGroup(String name, TempMetadataStore metadata, GroupContext externalGroups, List<? extends SingleElementSymbol> symbols, boolean[] updatable) {
		GroupSymbol variables = new GroupSymbol(name);
	    externalGroups.addGroup(variables);
	    TempMetadataID tid = metadata.addTempGroup(name, symbols);
	    tid.setMetadataType(Type.SCALAR);
	    int i = 0;
	    for (TempMetadataID cid : tid.getElements()) {
			cid.setMetadataType(Type.SCALAR);
			cid.setUpdatable(updatable[i++]);
		}
	    variables.setMetadataID(tid);
	    return variables;
	}
	
	/**
	 * Set the appropriate "external" metadata for the given command
	 */
	public static void findChildCommandMetadata(Command currentCommand,
			GroupSymbol container, int type, QueryMetadataInterface metadata)
			throws QueryMetadataException, TeiidComponentException {
		//find the childMetadata using a clean metadata store
	    TempMetadataStore childMetadata = new TempMetadataStore();
	    TempMetadataAdapter tma = new TempMetadataAdapter(metadata, childMetadata);
	    GroupContext externalGroups = new GroupContext();

		if (currentCommand instanceof TriggerAction) {
			TriggerAction ta = (TriggerAction)currentCommand;
			ta.setView(container);
		    //TODO: it seems easier to just inline the handling here rather than have each of the resolvers check for trigger actions
		    List<ElementSymbol> viewElements = ResolverUtil.resolveElementsInGroup(ta.getView(), metadata);
		    if (type == Command.TYPE_UPDATE || type == Command.TYPE_INSERT) {
		    	ProcedureContainerResolver.addChanging(tma.getMetadataStore(), externalGroups, viewElements);
		    	ProcedureContainerResolver.addScalarGroup(SQLConstants.Reserved.NEW, tma.getMetadataStore(), externalGroups, viewElements, false);
		    }
		    if (type == Command.TYPE_UPDATE || type == Command.TYPE_DELETE) {
		    	ProcedureContainerResolver.addScalarGroup(SQLConstants.Reserved.OLD, tma.getMetadataStore(), externalGroups, viewElements, false);
		    }
		} else if (currentCommand instanceof CreateUpdateProcedureCommand) {
			CreateUpdateProcedureCommand cupc = (CreateUpdateProcedureCommand)currentCommand;
			cupc.setVirtualGroup(container);

			if (type == Command.TYPE_STORED_PROCEDURE) {
				StoredProcedureInfo info = metadata.getStoredProcedureInfoForProcedure(container.getCanonicalName());
		        // Create temporary metadata that defines a group based on either the stored proc
		        // name or the stored query name - this will be used later during planning
		        String procName = info.getProcedureCallableName();
		        
		        // Look through parameters to find input elements - these become child metadata
		        List<ElementSymbol> tempElements = new ArrayList<ElementSymbol>(info.getParameters().size());
		        boolean[] updatable = new boolean[info.getParameters().size()];
		        int i = 0;
		        for (SPParameter param : info.getParameters()) {
		            if(param.getParameterType() != ParameterInfo.RESULT_SET) {
		                ElementSymbol symbol = param.getParameterSymbol();
		                tempElements.add(symbol);
		                updatable[i++] = param.getParameterType() != ParameterInfo.IN;  
		            }
		        }

		        ProcedureContainerResolver.addScalarGroup(procName, childMetadata, externalGroups, tempElements, updatable);
			} else if (type != Command.TYPE_DELETE) {
				createInputChangingMetadata(childMetadata, tma, container, externalGroups);
			}
		}
		
	    QueryResolver.setChildMetadata(currentCommand, childMetadata.getData(), externalGroups);
	}

	static void createInputChangingMetadata(
			TempMetadataStore discoveredMetadata,
			QueryMetadataInterface metadata, GroupSymbol group, GroupContext externalGroups)
			throws QueryMetadataException, TeiidComponentException {
        //Look up elements for the virtual group
        List<ElementSymbol> elements = ResolverUtil.resolveElementsInGroup(group, metadata);

        // Create the INPUT variables
        List<ElementSymbol> inputElments = new ArrayList<ElementSymbol>(elements.size());
        for(int i=0; i<elements.size(); i++) {
            ElementSymbol virtualElmnt = elements.get(i);
            ElementSymbol inputElement = (ElementSymbol)virtualElmnt.clone();
            inputElments.add(inputElement);
        }

        ProcedureContainerResolver.addScalarGroup(ProcedureReservedWords.INPUT, discoveredMetadata, externalGroups, inputElments, false);
        ProcedureContainerResolver.addScalarGroup(ProcedureReservedWords.INPUTS, discoveredMetadata, externalGroups, inputElments, false);

        // Switch type to be boolean for all CHANGING variables
        ProcedureContainerResolver.addChanging(discoveredMetadata, externalGroups, elements);
	}
        
}
