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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.Request;
import org.teiid.language.SQLConstants;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.command.*;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.ExplainCommand;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.validator.UpdateValidator;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;
import org.teiid.query.validator.UpdateValidator.UpdateType;
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
    private static final CommandResolver EXEC_RESOLVER = new ExecResolver();
    private static final CommandResolver INSERT_RESOLVER = new InsertResolver();
    private static final CommandResolver UPDATE_RESOLVER = new UpdateResolver();
    private static final CommandResolver DELETE_RESOLVER = new DeleteResolver();
    private static final CommandResolver UPDATE_PROCEDURE_RESOLVER = new UpdateProcedureResolver();
    private static final CommandResolver BATCHED_UPDATE_RESOLVER = new BatchedUpdateResolver();
    private static final CommandResolver DYNAMIC_COMMAND_RESOLVER = new DynamicCommandResolver();
    private static final CommandResolver TEMP_TABLE_RESOLVER = new TempTableResolver();
    private static final CommandResolver ALTER_RESOLVER = new AlterResolver();
    private static final CommandResolver DUMMY_RESOLVER = new CommandResolver() {

        @Override
        public void resolveCommand(Command command, TempMetadataAdapter metadata,
                boolean resolveNullLiterals) throws QueryMetadataException,
                QueryResolverException, TeiidComponentException {
        }
    };
    private static final CommandResolver EXPLAIN_RESOLVER = new CommandResolver() {

        @Override
        public void resolveCommand(Command command, TempMetadataAdapter metadata,
                boolean resolveNullLiterals) throws QueryMetadataException,
                QueryResolverException, TeiidComponentException {
            Command actual = ((ExplainCommand)command).getCommand();
            CommandResolver resolver = chooseResolver(actual, metadata);
            resolver.resolveCommand(actual, metadata, resolveNullLiterals);
        }
    };

    public static Command expandCommand(ProcedureContainer proc, QueryMetadataInterface metadata, AnalysisRecord analysisRecord) throws QueryResolverException, QueryMetadataException, TeiidComponentException {
        ProcedureContainerResolver cr = (ProcedureContainerResolver)chooseResolver(proc, metadata);
        Command command = cr.expandCommand(proc, metadata, analysisRecord);
        if (command == null) {
            return null;
        }
        resolveCommand(command, proc.getGroup(), proc.getType(), metadata.getDesignTimeMetadata(), false);
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
     * @param inferProcedureResultSetColumns if true and the currentCommand is a procedure definition, then resolving will set the getResultSetColumns on the command to what is discoverable in the procedure body.
     */
    public static TempMetadataStore resolveCommand(Command currentCommand, GroupSymbol container, int type, QueryMetadataInterface metadata, boolean inferProcedureResultSetColumns) throws QueryResolverException, TeiidComponentException {
        ResolverUtil.resolveGroup(container, metadata);
        switch (type) {
        case Command.TYPE_INSERT:
        case Command.TYPE_UPDATE:
        case Command.TYPE_DELETE:
        case Command.TYPE_STORED_PROCEDURE:
            ProcedureContainerResolver.findChildCommandMetadata(currentCommand, container, type, metadata, inferProcedureResultSetColumns);
        }
        return resolveCommand(currentCommand, metadata, false);
    }

    public static TempMetadataStore resolveCommand(Command currentCommand, QueryMetadataInterface metadata, boolean resolveNullLiterals)
        throws QueryResolverException, TeiidComponentException {

        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_QUERY_RESOLVER, new Object[]{"Resolving command", currentCommand}); //$NON-NLS-1$

        TempMetadataAdapter resolverMetadata = null;
        try {
            TempMetadataStore discoveredMetadata = currentCommand.getTemporaryMetadata();
            if(discoveredMetadata == null) {
                discoveredMetadata = new TempMetadataStore();
                currentCommand.setTemporaryMetadata(discoveredMetadata);
            }

            resolverMetadata = new TempMetadataAdapter(metadata, discoveredMetadata);

            // Resolve external groups for command
            Collection<GroupSymbol> externalGroups = currentCommand.getAllExternalGroups();
            for (GroupSymbol extGroup : externalGroups) {
                Object metadataID = extGroup.getMetadataID();
                //make sure that the group is resolved and that it is pointing to the appropriate temp group
                //TODO: this is mainly for XML resolving since it sends external groups in unresolved
                if (metadataID == null || (!(extGroup.getMetadataID() instanceof TempMetadataID) && discoveredMetadata.getTempGroupID(extGroup.getName()) != null)) {
                    boolean missing = metadataID == null;
                    metadataID = resolverMetadata.getGroupID(extGroup.getName());
                    if (missing) {
                        extGroup.setMetadataID(metadataID);
                    } else {
                        //we shouldn't modify the existing, just add a shadow group
                        GroupSymbol gs = extGroup.clone();
                        gs.setMetadataID(metadataID);
                        currentCommand.getExternalGroupContexts().addGroup(gs);
                    }
                }
            }

            CommandResolver resolver = chooseResolver(currentCommand, resolverMetadata);

            // Resolve this command
            resolver.resolveCommand(currentCommand, resolverMetadata, resolveNullLiterals);
        } catch(QueryMetadataException e) {
             throw new QueryResolverException(e);
        }

        return resolverMetadata.getMetadataStore();
    }

    /**
     * Method chooseResolver.
     * @param command
     * @param metadata
     * @return CommandResolver
     */
    private static CommandResolver chooseResolver(Command command, QueryMetadataInterface metadata) {

        switch(command.getType()) {
            case Command.TYPE_EXPLAIN:
                return EXPLAIN_RESOLVER;
            case Command.TYPE_QUERY:
                if(command instanceof Query) {
                    return SIMPLE_QUERY_RESOLVER;
                }
                return SET_QUERY_RESOLVER;
            case Command.TYPE_INSERT:               return INSERT_RESOLVER;
            case Command.TYPE_UPDATE:               return UPDATE_RESOLVER;
            case Command.TYPE_DELETE:               return DELETE_RESOLVER;
            case Command.TYPE_STORED_PROCEDURE:     return EXEC_RESOLVER;
            case Command.TYPE_TRIGGER_ACTION:        return UPDATE_PROCEDURE_RESOLVER;
            case Command.TYPE_UPDATE_PROCEDURE:     return UPDATE_PROCEDURE_RESOLVER;
            case Command.TYPE_BATCHED_UPDATE:       return BATCHED_UPDATE_RESOLVER;
            case Command.TYPE_DYNAMIC:              return DYNAMIC_COMMAND_RESOLVER;
            case Command.TYPE_CREATE:               return TEMP_TABLE_RESOLVER;
            case Command.TYPE_DROP:                 return TEMP_TABLE_RESOLVER;
            case Command.TYPE_ALTER_PROC:
            case Command.TYPE_ALTER_TRIGGER:
            case Command.TYPE_ALTER_VIEW:           return ALTER_RESOLVER;
            case Command.TYPE_SOURCE_EVENT:         return DUMMY_RESOLVER;
            default:
                throw new AssertionError("Unknown command type"); //$NON-NLS-1$
        }
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
        TempMetadataStore childMetadata = parent.getTemporaryMetadata();
        GroupContext parentContext = parent.getExternalGroupContexts();

        setChildMetadata(subCommand, childMetadata, parentContext);
    }

    public static void setChildMetadata(Command subCommand, TempMetadataStore parentTempMetadata, GroupContext parentContext) {
        TempMetadataStore tempMetadata = subCommand.getTemporaryMetadata();
        if(tempMetadata == null) {
            subCommand.setTemporaryMetadata(parentTempMetadata.clone());
        } else {
            tempMetadata.getData().putAll(parentTempMetadata.getData());
        }

        subCommand.setExternalGroupContexts(parentContext);
    }

    public static Map<ElementSymbol, Expression> getVariableValues(Command command, boolean changingOnly, QueryMetadataInterface metadata) throws QueryMetadataException, QueryResolverException, TeiidComponentException {

        CommandResolver resolver = chooseResolver(command, metadata);

        if (resolver instanceof VariableResolver) {
            return ((VariableResolver)resolver).getVariableValues(command, changingOnly, metadata);
        }

        return Collections.emptyMap();
    }

    public static void resolveSubqueries(Command command,
            TempMetadataAdapter metadata, Collection<GroupSymbol> externalGroups)
            throws QueryResolverException, TeiidComponentException {
        for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(command)) {
            QueryResolver.setChildMetadata(container.getCommand(), command);
            if (externalGroups != null) {
                container.getCommand().pushNewResolvingContext(externalGroups);
            }
            QueryResolver.resolveCommand(container.getCommand(), metadata.getMetadata(), false);
        }
    }

    public static QueryNode resolveView(GroupSymbol virtualGroup, QueryNode qnode,
            String cacheString, QueryMetadataInterface qmi, boolean logValidation) throws TeiidComponentException,
            QueryMetadataException, QueryResolverException,
            QueryValidatorException {
        qmi = qmi.getDesignTimeMetadata();
        cacheString = "transformation/" + cacheString; //$NON-NLS-1$
        QueryNode cachedNode = (QueryNode)qmi.getFromMetadataCache(virtualGroup.getMetadataID(), cacheString);
        if (cachedNode == null) {
            Command result = qnode.getCommand();
            if (result == null) {
                try {
                    result = QueryParser.getQueryParser().parseCommand(qnode.getQuery());
                } catch(QueryParserException e) {
                     throw new QueryResolverException(QueryPlugin.Event.TEIID30065, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30065, virtualGroup));
                }

            } else {
                result = (Command) result.clone();
            }
            QueryResolver.resolveCommand(result, qmi, false);
            Request.validateWithVisitor(new ValidationVisitor(), qmi, result);

            validateProjectedSymbols(virtualGroup, qmi, result);
            cachedNode = new QueryNode(qnode.getQuery());
            cachedNode.setCommand(result);

            if(isView(virtualGroup, qmi)) {
                String updatePlan = qmi.getUpdatePlan(virtualGroup.getMetadataID());
                String deletePlan = qmi.getDeletePlan(virtualGroup.getMetadataID());
                String insertPlan = qmi.getInsertPlan(virtualGroup.getMetadataID());
                //the elements must be against the view and not the alias
                if (virtualGroup.getDefinition() != null) {
                    GroupSymbol group = new GroupSymbol(virtualGroup.getNonCorrelationName());
                    group.setMetadataID(virtualGroup.getMetadataID());
                    virtualGroup = group;
                }
                List<ElementSymbol> elements = ResolverUtil.resolveElementsInGroup(virtualGroup, qmi);
                UpdateValidator validator = new UpdateValidator(qmi, determineType(insertPlan), determineType(updatePlan), determineType(deletePlan));
                validator.validate(result, elements);
                UpdateInfo info = validator.getUpdateInfo();
                if (logValidation && qmi.groupSupports(virtualGroup.getMetadataID(), SupportConstants.Group.UPDATE)) {
                    if (info.isInherentInsert() && validator.getInsertReport().hasItems()) {
                        LogManager.logDetail(LogConstants.CTX_QUERY_RESOLVER, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31173, validator.getInsertReport().getFailureMessage(), SQLConstants.Reserved.INSERT, qmi.getFullName(virtualGroup.getMetadataID())));
                    }
                    if (info.isInherentUpdate() && validator.getUpdateReport().hasItems()) {
                        LogManager.logDetail(LogConstants.CTX_QUERY_RESOLVER, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31173, validator.getUpdateReport().getFailureMessage(), SQLConstants.Reserved.UPDATE, qmi.getFullName(virtualGroup.getMetadataID())));
                    }
                    if (info.isInherentDelete() && validator.getDeleteReport().hasItems()) {
                        LogManager.logDetail(LogConstants.CTX_QUERY_RESOLVER, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31173, validator.getDeleteReport().getFailureMessage(), SQLConstants.Reserved.DELETE, qmi.getFullName(virtualGroup.getMetadataID())));
                    }
                }
                cachedNode.setUpdateInfo(info);
            }
            qmi.addToMetadataCache(virtualGroup.getMetadataID(), cacheString, cachedNode);
        }
        return cachedNode;
    }

    public static void validateProjectedSymbols(GroupSymbol virtualGroup,
            QueryMetadataInterface qmi, Command result)
            throws QueryMetadataException, TeiidComponentException, QueryValidatorException {
        //ensure that null types match the view
        List<ElementSymbol> symbols = ResolverUtil.resolveElementsInGroup(virtualGroup, qmi);
        List<Expression> projectedSymbols = result.getProjectedSymbols();
        validateProjectedSymbols(virtualGroup, symbols, projectedSymbols);
        //setqueries store the projected types separately
        if (result instanceof SetQuery) {
            List<Class<?>> types = new ArrayList<Class<?>>();
            for (ElementSymbol es : symbols) {
                types.add(es.getType());
            }
            ((SetQuery)result).setProjectedTypes(types, qmi.getDesignTimeMetadata());
        }
    }

    public static void validateProjectedSymbols(GroupSymbol virtualGroup,
            List<? extends Expression> symbols,
            List<? extends Expression> projectedSymbols)
            throws QueryValidatorException {
        if (symbols.size() != projectedSymbols.size()) {
             throw new QueryValidatorException(QueryPlugin.Event.TEIID30066, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30066, virtualGroup, symbols.size(), projectedSymbols.size()));
        }
        for (int i = 0; i < projectedSymbols.size(); i++) {
            Expression projectedSymbol = projectedSymbols.get(i);

            ResolverUtil.setTypeIfNull(projectedSymbol, symbols.get(i).getType());

            if (projectedSymbol.getType() != symbols.get(i).getType()) {
                throw new QueryValidatorException(QueryPlugin.Util.getString("QueryResolver.wrong_view_symbol_type", virtualGroup, i+1,  //$NON-NLS-1$
                        DataTypeManager.getDataTypeName(symbols.get(i).getType()), DataTypeManager.getDataTypeName(projectedSymbol.getType())));
            }
        }
    }

    public static boolean isView(GroupSymbol virtualGroup,
            QueryMetadataInterface qmi) throws TeiidComponentException,
            QueryMetadataException {
        return !(virtualGroup.getMetadataID() instanceof TempMetadataID) && qmi.isVirtualGroup(virtualGroup.getMetadataID());// && qmi.isVirtualModel(qmi.getModelID(virtualGroup.getMetadataID()));
    }

    private static UpdateType determineType(String plan) {
        UpdateType type = UpdateType.INHERENT;
        if (plan != null) {
            type = UpdateType.INSTEAD_OF;
        }
        return type;
    }

}
