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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.UnresolvedSymbolDescription;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.resolver.CommandResolver;
import org.teiid.query.resolver.ProcedureContainerResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DynamicCommand;
import org.teiid.query.sql.lang.GroupContext;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.DeclareStatement;
import org.teiid.query.sql.proc.ExpressionStatement;
import org.teiid.query.sql.proc.IfStatement;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.proc.ReturnStatement;
import org.teiid.query.sql.proc.Statement;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.proc.WhileStatement;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


/**
 */
public class UpdateProcedureResolver implements CommandResolver {

    public static final List<ElementSymbol> exceptionGroup;
    static {
        ElementSymbol es1 = new ElementSymbol("STATE"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.STRING);
        ElementSymbol es2 = new ElementSymbol("ERRORCODE"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        ElementSymbol es3 = new ElementSymbol("TEIIDCODE"); //$NON-NLS-1$
        es3.setType(DataTypeManager.DefaultDataClasses.STRING);
        ElementSymbol es4 = new ElementSymbol(NonReserved.EXCEPTION);
        es4.setType(Exception.class);
        ElementSymbol es5 = new ElementSymbol(NonReserved.CHAIN);
        es5.setType(Exception.class);
        exceptionGroup = Arrays.asList(es1, es2, es3, es4, es5);
    }

    /**
     * @see org.teiid.query.resolver.CommandResolver#resolveCommand(org.teiid.query.sql.lang.Command, TempMetadataAdapter, boolean)
     */
    public void resolveCommand(Command command, TempMetadataAdapter metadata, boolean resolveNullLiterals)
        throws QueryMetadataException, QueryResolverException, TeiidComponentException {

        //by creating a new group context here it means that variables will resolve with a higher precedence than input/changing
        GroupContext externalGroups = command.getExternalGroupContexts();

        List<ElementSymbol> symbols = new LinkedList<ElementSymbol>();

        String countVar = ProcedureReservedWords.VARIABLES + Symbol.SEPARATOR + ProcedureReservedWords.ROWCOUNT;
        ElementSymbol updateCount = new ElementSymbol(countVar);
        updateCount.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        symbols.add(updateCount);

        ProcedureContainerResolver.addScalarGroup(ProcedureReservedWords.VARIABLES, metadata.getMetadataStore(), externalGroups, symbols);

        if (command instanceof TriggerAction) {
            TriggerAction ta = (TriggerAction)command;
            CreateProcedureCommand cmd = new CreateProcedureCommand(ta.getBlock());
            cmd.setVirtualGroup(ta.getView());
            //TODO: this is not generally correct - we should update the api to set the appropriate type
            cmd.setUpdateType(Command.TYPE_INSERT);
            resolveBlock(cmd, ta.getBlock(), ta.getExternalGroupContexts(), metadata);
            return;
        }

        CreateProcedureCommand procCommand = (CreateProcedureCommand) command;

        resolveBlock(procCommand, procCommand.getBlock(), externalGroups, metadata);
    }

    public void resolveBlock(CreateProcedureCommand command, Block block, GroupContext originalExternalGroups,
                              TempMetadataAdapter original)
        throws QueryResolverException, QueryMetadataException, TeiidComponentException {
        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_QUERY_RESOLVER, new Object[]{"Resolving block", block}); //$NON-NLS-1$

        //create a new variable and metadata context for this block so that discovered metadata is not visible else where
        TempMetadataStore store = original.getMetadataStore().clone();
        TempMetadataAdapter metadata = new TempMetadataAdapter(original.getMetadata(), store);
        GroupContext externalGroups = new GroupContext(originalExternalGroups, null);

        //create a new variables group for this block
        GroupSymbol variables = ProcedureContainerResolver.addScalarGroup(ProcedureReservedWords.VARIABLES, store, externalGroups, new LinkedList<Expression>());

        for (Statement statement : block.getStatements()) {
            resolveStatement(command, statement, externalGroups, variables, metadata);
        }

        if (block.getExceptionGroup() != null) {
            //create a new variable and metadata context for this block so that discovered metadata is not visible else where
            store = original.getMetadataStore().clone();
            metadata = new TempMetadataAdapter(original.getMetadata(), store);
            externalGroups = new GroupContext(originalExternalGroups, null);

            //create a new variables group for this block
            variables = ProcedureContainerResolver.addScalarGroup(ProcedureReservedWords.VARIABLES, store, externalGroups, new LinkedList<Expression>());
            isValidGroup(metadata, block.getExceptionGroup());

            if (block.getExceptionStatements() != null) {
                ProcedureContainerResolver.addScalarGroup(block.getExceptionGroup(), store, externalGroups, exceptionGroup, false);
                for (Statement statement : block.getExceptionStatements()) {
                    resolveStatement(command, statement, externalGroups, variables, metadata);
                }
            }
        }
    }

    private void resolveStatement(CreateProcedureCommand command, Statement statement, GroupContext externalGroups, GroupSymbol variables, TempMetadataAdapter metadata)
        throws QueryResolverException, QueryMetadataException, TeiidComponentException {
        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_QUERY_RESOLVER, new Object[]{"Resolving statement", statement}); //$NON-NLS-1$

        switch(statement.getType()) {
            case Statement.TYPE_IF:
                IfStatement ifStmt = (IfStatement) statement;
                Criteria ifCrit = ifStmt.getCondition();
                for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(ifCrit)) {
                    resolveEmbeddedCommand(metadata, externalGroups, container.getCommand());
                }
                ResolverVisitor.resolveLanguageObject(ifCrit, null, externalGroups, metadata);
                resolveBlock(command, ifStmt.getIfBlock(), externalGroups, metadata);
                if(ifStmt.hasElseBlock()) {
                    resolveBlock(command, ifStmt.getElseBlock(), externalGroups, metadata);
                }
                break;
            case Statement.TYPE_COMMAND:
                CommandStatement cmdStmt = (CommandStatement) statement;
                Command subCommand = cmdStmt.getCommand();

                TempMetadataStore discoveredMetadata = resolveEmbeddedCommand(metadata, externalGroups, subCommand);

                if (subCommand instanceof StoredProcedure) {
                    StoredProcedure sp = (StoredProcedure)subCommand;
                    for (SPParameter param : sp.getParameters()) {
                        switch (param.getParameterType()) {
                        case ParameterInfo.OUT:
                        case ParameterInfo.RETURN_VALUE:
                            if (param.getExpression() != null) {
                                if (!isAssignable(metadata, param)) {
                                    throw new QueryResolverException(QueryPlugin.Event.TEIID30121, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30121, param.getExpression()));
                                }
                                sp.setCallableStatement(true);
                            }
                            break;
                        case ParameterInfo.INOUT:
                            if (!isAssignable(metadata, param)) {
                                continue;
                            }
                            sp.setCallableStatement(true);
                            break;
                        }
                    }
                }

                if (discoveredMetadata != null) {
                    NavigableMap<String, TempMetadataID> data = metadata.getMetadataStore().getData();
                    data.clear();
                    data.putAll(discoveredMetadata.getData());
                }

                //dynamic commands need to be updated as to their implicitly expected projected symbols
                if (subCommand instanceof DynamicCommand) {
                    DynamicCommand dynCommand = (DynamicCommand)subCommand;

                    if(dynCommand.getIntoGroup() == null
                            && !dynCommand.isAsClauseSet()) {
                        if ((command.getResultSetColumns() != null && command.getResultSetColumns().isEmpty()) || !cmdStmt.isReturnable() || command.getResultSetColumns() == null) {
                            //we're not interested in the resultset
                            dynCommand.setAsColumns(Collections.EMPTY_LIST);
                        } else {
                            //should match the procedure
                            dynCommand.setAsColumns(command.getResultSetColumns());
                        }
                    }
                }

                if (command.getResultSetColumns() == null && cmdStmt.isReturnable() && subCommand.returnsResultSet() && subCommand.getResultSetColumns() != null && !subCommand.getResultSetColumns().isEmpty()) {
                    command.setResultSetColumns(subCommand.getResultSetColumns());
                    if (command.getProjectedSymbols().isEmpty()) {
                        command.setProjectedSymbols(subCommand.getResultSetColumns());
                    }
                }

                break;
            case Statement.TYPE_ERROR:
            case Statement.TYPE_ASSIGNMENT:
            case Statement.TYPE_DECLARE:
            case Statement.TYPE_RETURN:
                ExpressionStatement exprStmt = (ExpressionStatement) statement;
                //first resolve the value.  this ensures the value cannot use the variable being defined
                if (exprStmt.getExpression() != null) {
                    Expression expr = exprStmt.getExpression();
                    for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(expr)) {
                        resolveEmbeddedCommand(metadata, externalGroups, container.getCommand());
                    }
                    ResolverVisitor.resolveLanguageObject(expr, null, externalGroups, metadata);
                }

                //second resolve the variable
                switch (statement.getType()) {
                case Statement.TYPE_DECLARE:
                    collectDeclareVariable((DeclareStatement)statement, variables, metadata, externalGroups);
                    break;
                case Statement.TYPE_ASSIGNMENT:
                    AssignmentStatement assStmt = (AssignmentStatement)statement;
                    ResolverVisitor.resolveLanguageObject(assStmt.getVariable(), null, externalGroups, metadata);
                    if (!metadata.elementSupports(assStmt.getVariable().getMetadataID(), SupportConstants.Element.UPDATE)) {
                         throw new QueryResolverException(QueryPlugin.Event.TEIID30121, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30121, assStmt.getVariable()));
                    }
                    //don't allow variable assignments to be external
                    assStmt.getVariable().setIsExternalReference(false);
                    break;
                case Statement.TYPE_RETURN:
                    ReturnStatement rs = (ReturnStatement)statement;
                    if (rs.getExpression() != null) {
                        if (command.getReturnVariable() == null) {
                            throw new QueryResolverException(QueryPlugin.Event.TEIID31125, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31125, rs));
                        }
                        rs.setVariable(command.getReturnVariable().clone());
                    }
                    //else - we don't currently require the use of return for backwards compatibility
                    break;
                }

                //third ensure the type matches
                if (exprStmt.getExpression() != null) {
                    Class<?> varType = exprStmt.getExpectedType();
                    Class<?> exprType = exprStmt.getExpression().getType();
                    if (exprType == null) {
                         throw new QueryResolverException(QueryPlugin.Event.TEIID30123, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30123));
                    }
                    String varTypeName = DataTypeManager.getDataTypeName(varType);
                    exprStmt.setExpression(ResolverUtil.convertExpression(exprStmt.getExpression(), varTypeName, metadata));
                    if (statement.getType() == Statement.TYPE_ERROR) {
                        ResolverVisitor.checkException(exprStmt.getExpression());
                    }
                }
                break;
            case Statement.TYPE_WHILE:
                WhileStatement whileStmt = (WhileStatement) statement;
                Criteria whileCrit = whileStmt.getCondition();
                for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(whileCrit)) {
                    resolveEmbeddedCommand(metadata, externalGroups, container.getCommand());
                }
                ResolverVisitor.resolveLanguageObject(whileCrit, null, externalGroups, metadata);
                resolveBlock(command, whileStmt.getBlock(), externalGroups, metadata);
                break;
            case Statement.TYPE_LOOP:
                LoopStatement loopStmt = (LoopStatement) statement;
                String groupName = loopStmt.getCursorName();

                isValidGroup(metadata, groupName);
                Command cmd = loopStmt.getCommand();
                resolveEmbeddedCommand(metadata, externalGroups, cmd);
                List<Expression> symbols = cmd.getProjectedSymbols();

                //add the loop cursor group into its own context
                TempMetadataStore store = metadata.getMetadataStore().clone();
                metadata = new TempMetadataAdapter(metadata.getMetadata(), store);
                externalGroups = new GroupContext(externalGroups, null);

                ProcedureContainerResolver.addScalarGroup(groupName, store, externalGroups, symbols, false);

                resolveBlock(command, loopStmt.getBlock(), externalGroups, metadata);
                break;
            case Statement.TYPE_COMPOUND:
                resolveBlock(command, (Block)statement, externalGroups, metadata);
                break;
        }
    }

    private void isValidGroup(TempMetadataAdapter metadata, String groupName)
            throws QueryResolverException {
        if (metadata.getMetadataStore().getTempGroupID(groupName) != null) {
             throw new QueryResolverException(QueryPlugin.Event.TEIID30124, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30124, groupName));
        }

        //check - cursor name should not start with #
        if(GroupSymbol.isTempGroupName(groupName)){
             throw new QueryResolverException(QueryPlugin.Event.TEIID30125, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30125, groupName));
        }
    }

    private boolean isAssignable(TempMetadataAdapter metadata, SPParameter param)
            throws TeiidComponentException, QueryMetadataException {
        if (!(param.getExpression() instanceof ElementSymbol)) {
            return false;
        }
        ElementSymbol symbol = (ElementSymbol)param.getExpression();

        return metadata.elementSupports(symbol.getMetadataID(), SupportConstants.Element.UPDATE);
    }

    private TempMetadataStore resolveEmbeddedCommand(TempMetadataAdapter metadata, GroupContext groupContext,
                                Command cmd) throws TeiidComponentException,
                                            QueryResolverException {
        QueryResolver.setChildMetadata(cmd, metadata.getMetadataStore(), groupContext);

        return QueryResolver.resolveCommand(cmd, metadata.getMetadata());
    }

    private void collectDeclareVariable(DeclareStatement obj, GroupSymbol variables, TempMetadataAdapter metadata, GroupContext externalGroups) throws QueryResolverException, TeiidComponentException {
        ElementSymbol variable = obj.getVariable();
        String typeName = obj.getVariableType();
        GroupSymbol gs = variable.getGroupSymbol();
        if(gs == null) {
            String outputName = variable.getShortName();
            variable.setGroupSymbol(new GroupSymbol(ProcedureReservedWords.VARIABLES));
            variable.setOutputName(outputName);
        } else {
            if (!gs.getName().equalsIgnoreCase(ProcedureReservedWords.VARIABLES)) {
                handleUnresolvableDeclaration(variable, QueryPlugin.Util.getString("ERR.015.010.0031", new Object[]{ProcedureReservedWords.VARIABLES, variable})); //$NON-NLS-1$
            }
        }
        boolean exists = false;
        try {
            ResolverVisitor.resolveLanguageObject(variable, null, externalGroups, metadata);
            exists = true;
        } catch (QueryResolverException e) {
            //ignore, not already defined
        }
        if (exists) {
            handleUnresolvableDeclaration(variable, QueryPlugin.Util.getString("ERR.015.010.0032", variable.getOutputName())); //$NON-NLS-1$
        }
        boolean exception = typeName.equalsIgnoreCase(SQLConstants.NonReserved.EXCEPTION);
        variable.setType(exception?DataTypeManager.DefaultDataClasses.OBJECT:DataTypeManager.getDataTypeClass(typeName));
        variable.setGroupSymbol(variables);
        TempMetadataID id = new TempMetadataID(variable.getName(), exception?Exception.class:variable.getType());
        id.setUpdatable(true);
        variable.setMetadataID(id);
        //TODO: this will cause the variables group to loose it's cache of resolved symbols
        metadata.getMetadataStore().addElementToTempGroup(ProcedureReservedWords.VARIABLES, variable.clone());
    }

    private void handleUnresolvableDeclaration(ElementSymbol variable, String description) throws QueryResolverException {
        UnresolvedSymbolDescription symbol = new UnresolvedSymbolDescription(variable.toString(), description);
        QueryResolverException e = new QueryResolverException(symbol.getDescription());
        e.setUnresolvedSymbols(Arrays.asList(symbol));
        throw e;
    }

}
