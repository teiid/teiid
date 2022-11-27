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

package org.teiid.query.resolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.StoredProcedureInfo;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataID.Type;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.command.InsertResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;


public abstract class ProcedureContainerResolver implements CommandResolver {

    public abstract void resolveProceduralCommand(Command command,
                                                  TempMetadataAdapter metadata) throws QueryMetadataException,
                                                                          QueryResolverException,
                                                                          TeiidComponentException;

    /**
     * Expand a command by finding and attaching all subcommands to the command.  If
     * some initial resolution must be done for this to be accomplished, that is ok,
     * but it should be kept to a minimum.
     * @param procCommand The command to expand
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
            subCommand = parser.parseProcedure(plan, !(procCommand instanceof StoredProcedure));
        } catch(QueryParserException e) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30060, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30060, group, procCommand.getClass().getSimpleName()));
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
            ElementSymbol changeElement = virtualElmnt.clone();
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

        //getPlan(metadata, procCommand);
    }

    private String getPlan(QueryMetadataInterface metadata, ProcedureContainer procCommand)
            throws TeiidComponentException, QueryMetadataException,
            QueryResolverException {
        if(!procCommand.getGroup().isTempTable() && metadata.isVirtualGroup(procCommand.getGroup().getMetadataID())) {
            String plan = getPlan(metadata, procCommand.getGroup());
            if (plan == null && !metadata.isProcedure(procCommand.getGroup().getMetadataID())) {
                int type = procCommand.getType();
                //force validation
                getUpdateInfo(procCommand.getGroup(), metadata, type, true);
            }
            return plan;
        }
        return null;
    }

    public static UpdateInfo getUpdateInfo(GroupSymbol group, QueryMetadataInterface metadata, int type, boolean validate) throws QueryMetadataException, TeiidComponentException, QueryResolverException {
        UpdateInfo info = getUpdateInfo(group, metadata);

        if (info == null) {
            return null;
        }
        if (validate) {
            String error = validateUpdateInfo(group, type, info);
            if (error != null) {
                throw new QueryResolverException(QueryPlugin.Event.TEIID30061, error);
            }
        }
        return info;
    }

    public static String validateUpdateInfo(GroupSymbol group, int type,
            UpdateInfo info) {
        String error = info.getDeleteValidationError();
        String name = "Delete"; //$NON-NLS-1$
        if (type == Command.TYPE_UPDATE) {
            error = info.getUpdateValidationError();
            name = "Update"; //$NON-NLS-1$
        } else if (type == Command.TYPE_INSERT) {
            error = info.getInsertValidationError();
            name = "Insert"; //$NON-NLS-1$
        }
        if (error != null) {
            return QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30061, group, name, error);
        }
        return null;
    }

    public static UpdateInfo getUpdateInfo(GroupSymbol group,
            QueryMetadataInterface metadata) throws TeiidComponentException,
            QueryMetadataException, QueryResolverException {
        if (!QueryResolver.isView(group, metadata)) {
            return null;
        }
        try {
            return QueryResolver.resolveView(group, metadata.getVirtualPlan(group.getMetadataID()), SQLConstants.Reserved.SELECT, metadata, false).getUpdateInfo();
        } catch (QueryValidatorException e) {
             throw new QueryResolverException(e);
        }
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
        if (!group.isTempTable()) {
            procCommand.setUpdateInfo(ProcedureContainerResolver.getUpdateInfo(group, metadata, procCommand.getType(), false));
        }
    }

    public static GroupSymbol addScalarGroup(String name, TempMetadataStore metadata, GroupContext externalGroups, List<? extends Expression> symbols) {
        return addScalarGroup(name, metadata, externalGroups, symbols, true);
    }

    public static GroupSymbol addScalarGroup(String name, TempMetadataStore metadata, GroupContext externalGroups, List<? extends Expression> symbols, boolean updatable) {
        boolean[] updateArray = new boolean[symbols.size()];
        if (updatable) {
            Arrays.fill(updateArray, true);
        }
        return addScalarGroup(name, metadata, externalGroups, symbols, updateArray);
    }

    public static GroupSymbol addScalarGroup(String name, TempMetadataStore metadata, GroupContext externalGroups, List<? extends Expression> symbols, boolean[] updatable) {
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
     * @param inferProcedureResultSetColumns
     * @throws QueryResolverException
     */
    public static void findChildCommandMetadata(Command currentCommand,
            GroupSymbol container, int type, QueryMetadataInterface metadata, boolean inferProcedureResultSetColumns)
            throws QueryMetadataException, TeiidComponentException, QueryResolverException {
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
                if (type == Command.TYPE_INSERT) {
                    List<ElementSymbol> key = InsertResolver.getAutoIncrementKey(ta.getView().getMetadataID(), viewElements, metadata);
                    if (key != null) {
                        ProcedureContainerResolver.addScalarGroup(SQLConstants.NonReserved.KEY, tma.getMetadataStore(), externalGroups, key, true);
                    }
                }
            }
            if (type == Command.TYPE_UPDATE || type == Command.TYPE_DELETE) {
                ProcedureContainerResolver.addScalarGroup(SQLConstants.Reserved.OLD, tma.getMetadataStore(), externalGroups, viewElements, false);
            }
        } else if (currentCommand instanceof CreateProcedureCommand) {
            CreateProcedureCommand cupc = (CreateProcedureCommand)currentCommand;
            cupc.setVirtualGroup(container);
            if (type == Command.TYPE_STORED_PROCEDURE) {
                StoredProcedureInfo info = metadata.getStoredProcedureInfoForProcedure(container.getName());
                // Create temporary metadata that defines a group based on either the stored proc
                // name or the stored query name - this will be used later during planning
                String procName = info.getProcedureCallableName();

                // Look through parameters to find input elements - these become child metadata
                List<ElementSymbol> tempElements = new ArrayList<ElementSymbol>(info.getParameters().size());
                boolean[] updatable = new boolean[info.getParameters().size()];
                int i = 0;
                List<ElementSymbol> rsColumns = Collections.emptyList();
                for (SPParameter param : info.getParameters()) {
                    if(param.getParameterType() != ParameterInfo.RESULT_SET) {
                        ElementSymbol symbol = param.getParameterSymbol();
                        tempElements.add(symbol);
                        updatable[i++] = param.getParameterType() != ParameterInfo.IN;
                        if (param.getParameterType() == ParameterInfo.RETURN_VALUE) {
                            cupc.setReturnVariable(symbol);
                        }
                    } else {
                        rsColumns = param.getResultSetColumns();
                    }
                }
                if (inferProcedureResultSetColumns) {
                    rsColumns = null;
                }
                GroupSymbol gs = ProcedureContainerResolver.addScalarGroup(procName, childMetadata, externalGroups, tempElements, updatable);
                if (cupc.getReturnVariable() != null) {
                    ResolverVisitor.resolveLanguageObject(cupc.getReturnVariable(), Arrays.asList(gs), metadata);
                }
                cupc.setResultSetColumns(rsColumns);
                //the relational planner will override this with the appropriate value
                cupc.setProjectedSymbols(rsColumns);
            } else {
                cupc.setUpdateType(type);
            }
        }

        QueryResolver.setChildMetadata(currentCommand, childMetadata, externalGroups);
    }

}
