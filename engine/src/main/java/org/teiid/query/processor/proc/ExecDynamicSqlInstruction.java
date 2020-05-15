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

package org.teiid.query.processor.proc;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.AuthorizationValidator.CommandType;
import org.teiid.dqp.internal.process.Request;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.proc.CreateCursorResultSetInstruction.Mode;
import org.teiid.query.processor.relational.SubqueryAwareRelationalNode;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.DynamicCommand;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.ValidationVisitor;


/**
 * <p>
 * Executes a SQL statement, and remove its results from the buffer manager.
 * Executing this instruction does not modify the values of any of the
 * variables, hence it's results are not important so they are removed
 * immediately.
 *
 */
public class ExecDynamicSqlInstruction extends ProgramInstruction {

    private static final int MAX_SQL_LENGTH = 1 << 18; //based roughly on what could be the default max over JDBC

    // the DynamicCommand
    private DynamicCommand dynamicCommand;

    // the idGenerator
    IDGenerator idGenerator;

    // the CapabilitiesFinder
    CapabilitiesFinder capFinder;

    // the metadata for this plan
    private QueryMetadataInterface metadata;
    private boolean returnable;

    // The parent command
    CreateProcedureCommand parentProcCommand;

    private Program dynamicProgram;

    public ExecDynamicSqlInstruction(
            CreateProcedureCommand parentProcCommand,
            DynamicCommand command, QueryMetadataInterface metadata,
            IDGenerator idGenerator, CapabilitiesFinder capFinder, boolean returnable) {
        this.parentProcCommand = parentProcCommand;
        this.dynamicCommand = command;
        this.metadata = metadata;
        this.capFinder = capFinder;
        this.idGenerator = idGenerator;
        this.returnable = returnable;
    }

    /**
     * <p>
     * Processing this instruction executes the ProcessorPlan for the command on
     * the CommandStatement of the update procedure language. Executing this
     * plan does not effect the values of any of the variables defined as part
     * of the update procedure and hence the results of the ProcessPlan
     * execution need not be stored for further processing. The results are
     * removed from the buffer manager immediately after execution. The program
     * counter is incremented after execution of the plan.
     *
     *
     * @throws BlockedException
     *             if this processing the plan throws a currentVarContext
     */
    public void process(ProcedurePlan procEnv) throws BlockedException,
            TeiidComponentException, TeiidProcessingException {

        VariableContext localContext = procEnv.getCurrentVariableContext();

        String query = null;

        try {
            Clob value = (Clob)procEnv.evaluateExpression(dynamicCommand.getSql());

            if (value == null) {
                throw new QueryProcessingException(QueryPlugin.Util
                        .getString("ExecDynamicSqlInstruction.0")); //$NON-NLS-1$
            }

            if (value.length() > MAX_SQL_LENGTH) {
                throw new QueryProcessingException(QueryPlugin.Util
                        .gs(QueryPlugin.Event.TEIID31204, MAX_SQL_LENGTH));
            }

            query = value.getSubString(1, MAX_SQL_LENGTH);

            LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP,
                    new Object[] { "Executing dynamic sql ", query }); //$NON-NLS-1$


            Command command = QueryParser.getQueryParser().parseCommand(query);

            //special handling for dynamic anon blocks
            if (command instanceof CreateProcedureCommand) {
                if (dynamicCommand.getIntoGroup() != null || returnable) {
                    //won't work unless we use a different approach than the insert into ...
                    //and the creation of an inline view
                    throw new QueryProcessingException(QueryPlugin.Event.TEIID31250, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31250));
                }
                ((CreateProcedureCommand)command).setResultSetColumns(Collections.EMPTY_LIST);
            }

            command.setExternalGroupContexts(dynamicCommand.getExternalGroupContexts());
            command.setTemporaryMetadata(dynamicCommand.getTemporaryMetadata().clone());
            updateContextWithUsingValues(procEnv, localContext);

            TempMetadataStore metadataStore = command.getTemporaryMetadata();

            if (dynamicCommand.getUsing() != null
                            && !dynamicCommand.getUsing().isEmpty()) {
                metadataStore.addTempGroup(Reserved.USING, new LinkedList<ElementSymbol>(dynamicCommand.getUsing().getClauseMap().keySet()));
                GroupSymbol using = new GroupSymbol(Reserved.USING);
                using.setMetadataID(metadataStore.getTempGroupID(Reserved.USING));
                command.addExternalGroupToContext(using);
                metadataStore.addTempGroup(ProcedureReservedWords.DVARS, new LinkedList<ElementSymbol>(dynamicCommand.getUsing().getClauseMap().keySet()));
                using = new GroupSymbol(ProcedureReservedWords.DVARS);
                using.setMetadataID(metadataStore.getTempGroupID(ProcedureReservedWords.DVARS));
                command.addExternalGroupToContext(using);
            }

            QueryMetadataInterface metadataToUse = metadata;
            if (!procEnv.isValidateAccess()) {
                //if we are not a user query, then use the design time metadata
                metadataToUse = metadata.getDesignTimeMetadata();
            }
            QueryResolver.resolveCommand(command, metadataToUse);

            validateDynamicCommand(procEnv, command, value.toString());

            // create a new set of variables including vars
            Map<ElementSymbol, Expression> nameValueMap = createVariableValuesMap(localContext);
            ValidationVisitor visitor = new ValidationVisitor();
            Request.validateWithVisitor(visitor, metadata, command);
            boolean insertInto = false;
            boolean updateCommand = false;
            if (!command.returnsResultSet() && !(command instanceof StoredProcedure)) {
                if (dynamicCommand.isAsClauseSet()) {
                    if (dynamicCommand.getProjectedSymbols().size() != 1 || ((Expression)dynamicCommand.getProjectedSymbols().get(0)).getType() != DataTypeManager.DefaultDataClasses.INTEGER) {
                        throw new QueryProcessingException(QueryPlugin.Event.TEIID31157, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31157));
                    }
                }
                updateCommand = true;
            } else if (dynamicCommand.getAsColumns() != null
                    && !dynamicCommand.getAsColumns().isEmpty()) {
                command = QueryRewriter.createInlineViewQuery(new GroupSymbol("X"), command, metadata, dynamicCommand.getAsColumns()); //$NON-NLS-1$
                if (dynamicCommand.getIntoGroup() != null) {
                    //we use the into group because we expect to positionally match all of the columns
                    Insert insert = new Insert(dynamicCommand.getIntoGroup(), ResolverUtil.resolveElementsInGroup(dynamicCommand.getIntoGroup(),  metadata), Collections.emptyList());
                    insert.setQueryExpression((Query)command);
                    command = insert;
                    insertInto = true;
                }
            }

            //if this is an update procedure, it could reassign variables
            command = QueryRewriter.rewrite(command, metadata, procEnv.getContext(),
                    command instanceof CreateProcedureCommand?Collections.EMPTY_MAP:nameValueMap);

            ProcessorPlan commandPlan = QueryOptimizer.optimizePlan(command, metadata,
                    idGenerator, capFinder, AnalysisRecord
                            .createNonRecordingRecord(), procEnv
                            .getContext());

            if (command instanceof CreateProcedureCommand && commandPlan instanceof ProcedurePlan) {
                ((ProcedurePlan)commandPlan).setValidateAccess(procEnv.isValidateAccess());
            }

            CreateCursorResultSetInstruction inst = new CreateCursorResultSetInstruction(null, commandPlan, (insertInto||updateCommand)?Mode.UPDATE:returnable?Mode.HOLD:Mode.NOHOLD) {
                @Override
                public void process(ProcedurePlan procEnv)
                        throws BlockedException, TeiidComponentException,
                        TeiidProcessingException {
                    boolean done = true;
                    try {
                        super.process(procEnv);
                    } catch (BlockedException e) {
                        done = false;
                        throw e;
                    } finally {
                        if (done) {
                            procEnv.getContext().popCall();
                        }
                    }
                }
            };

            dynamicProgram = new Program(false);
            dynamicProgram.addInstruction(inst);

            if (dynamicCommand.getIntoGroup() != null) {
                String groupName = dynamicCommand.getIntoGroup().getName();
                if (!procEnv.getTempTableStore().hasTempTable(groupName, true)) {
                    //create the temp table in the parent scope
                    Create create = new Create();
                    create.setTable(new GroupSymbol(groupName));
                    for (ElementSymbol es : (List<ElementSymbol>)dynamicCommand.getAsColumns()) {
                        Column c = new Column();
                        c.setName(es.getShortName());
                        c.setRuntimeType(DataTypeManager.getDataTypeName(es.getType()));
                        create.getColumns().add(c);
                    }
                    procEnv.getDataManager().registerRequest(procEnv.getContext(), create, TempMetadataAdapter.TEMP_MODEL.getName(), new RegisterRequestParameter());
                }
                //backwards compatibility to support into with a rowcount
                if (updateCommand) {
                    Insert insert = new Insert();
                    insert.setGroup(new GroupSymbol(groupName));
                    for (ElementSymbol es : (List<ElementSymbol>)dynamicCommand.getAsColumns()) {
                        ElementSymbol col = new ElementSymbol(es.getShortName(), insert.getGroup());
                        col.setType(es.getType());
                        insert.addVariable(col);
                    }
                    insert.addValue(new Constant(procEnv.getCurrentVariableContext().getValue(ProcedurePlan.ROWCOUNT)));
                    QueryResolver.resolveCommand(insert, metadataToUse);
                    TupleSource ts = procEnv.getDataManager().registerRequest(procEnv.getContext(), insert, TempMetadataAdapter.TEMP_MODEL.getName(), new RegisterRequestParameter());
                    ts.nextTuple();
                    ts.closeSource();
                }
            }

            // do a recursion check
            // Add group to recursion stack
            if (parentProcCommand.getUpdateType() != Command.TYPE_UNKNOWN) {
                procEnv.getContext().pushCall(Command.getCommandToken(parentProcCommand.getUpdateType()) + " " + parentProcCommand.getVirtualGroup()); //$NON-NLS-1$
            } else {
                if (parentProcCommand.getVirtualGroup() != null) {
                    procEnv.getContext().pushCall(parentProcCommand.getVirtualGroup().toString());
                }
            }

            procEnv.push(dynamicProgram);
        } catch (SQLException e) {
            Object[] params = {dynamicCommand, dynamicCommand.getSql(), e.getMessage()};
            throw new QueryProcessingException(QueryPlugin.Event.TEIID30168, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30168, params));
        } catch (TeiidProcessingException e) {
            Object[] params = {dynamicCommand, query == null?dynamicCommand.getSql():query, e.getMessage()};
             throw new QueryProcessingException(QueryPlugin.Event.TEIID30168, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30168, params));
        }
    }

    /**
     * @param procEnv
     * @param localContext
     * @throws TeiidComponentException
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    private void updateContextWithUsingValues(ProcedurePlan procEnv,
            VariableContext localContext) throws TeiidComponentException, TeiidProcessingException {
        if (dynamicCommand.getUsing() != null
                && !dynamicCommand.getUsing().isEmpty()) {
            for (SetClause setClause : dynamicCommand.getUsing().getClauses()) {
                Object assignment = procEnv.evaluateExpression(setClause.getValue());

                LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP,
                        new Object[] { this, " The using variable ", //$NON-NLS-1$
                        setClause.getSymbol(), " has value :", assignment }); //$NON-NLS-1$
                localContext.setValue(setClause.getSymbol(), assignment);
                ElementSymbol es = setClause.getSymbol().clone();
                es.getGroupSymbol().setShortName(Reserved.USING);
                localContext.setValue(es, assignment);
            }
        }
    }

    /**
     * @param localContext
     * @return
     */
    private Map<ElementSymbol, Expression> createVariableValuesMap(VariableContext localContext) {
        Map<ElementSymbol, Object> variableMap = new HashMap<ElementSymbol, Object>();
        localContext.getFlattenedContextMap(variableMap);
        Map<ElementSymbol, Expression> nameValueMap = new HashMap<ElementSymbol, Expression>(variableMap.size());
        for (Map.Entry<ElementSymbol, Object> entry : variableMap.entrySet()) {
            if (entry.getKey() instanceof ElementSymbol) {
                if (entry.getValue() instanceof Expression) {
                    nameValueMap.put(entry.getKey(), (Expression) entry.getValue());
                } else {
                    nameValueMap.put(entry.getKey(), new Constant(entry.getValue(), entry.getKey().getType()));
                }
            }
        }
        return nameValueMap;
    }

    /**
     * @param procEnv
     * @param command
     * @throws TeiidComponentException
     * @throws QueryProcessingException
     */
    private void validateDynamicCommand(ProcedurePlan procEnv,
            Command command, String commandString) throws TeiidComponentException,
            QueryProcessingException {
        // validate project symbols
        List dynamicExpectedColumns = dynamicCommand.getAsColumns();
        List<Expression> sourceProjectedSymbolList = command.getProjectedSymbols();

        if (dynamicExpectedColumns != null && !dynamicExpectedColumns.isEmpty()) {
            if (dynamicExpectedColumns.size() != sourceProjectedSymbolList.size()) {
                throw new QueryProcessingException(QueryPlugin.Util
                        .getString("ExecDynamicSqlInstruction.4")); //$NON-NLS-1$
            }
            // If there is only one project symbol, we won't validate the name.

            Iterator dynamicIter = dynamicExpectedColumns.iterator();
            Iterator<Expression> sourceIter = sourceProjectedSymbolList.iterator();
            // Check for proper element name and datatype definition in the
            // dynamic SQL
            // If the projected symbol list equal to 1, we won't bother checking
            // the name.
            while (dynamicIter.hasNext()) {
                Expression dynamicSymbol = (Expression) dynamicIter.next();
                Expression sourceExpr = sourceIter.next();
                Class<?> sourceSymbolDatatype = sourceExpr.getType();

                // Check if the the dynamic sql element types are equal or
                // implicitly convertible to the source types
                Class<?> dynamicType = dynamicSymbol.getType();
                String dynamicTypeName = DataTypeManager
                        .getDataTypeName(dynamicType);
                String sourceTypeName = DataTypeManager
                        .getDataTypeName(sourceSymbolDatatype);
                if (!dynamicTypeName.equals(sourceTypeName)
                        && // If the types aren't the same, and...
                        !DataTypeManager.isImplicitConversion(sourceTypeName,
                                dynamicTypeName)) { // if there's no implicit
                    // conversion between the two
                    throw new QueryProcessingException(QueryPlugin.Util
                            .getString("ExecDynamicSqlInstruction.6", sourceTypeName, sourceExpr, dynamicTypeName)); //$NON-NLS-1$
                }
            }
        }

        CommandContext context = procEnv.getContext();

        if (procEnv.isValidateAccess() && !context.getDQPWorkContext().isAdmin() && context.getAuthorizationValidator() != null) {
            context.getAuthorizationValidator().validate(new String[] {commandString}, command, metadata, context, CommandType.USER);
        }
    }

    /**
     * Returns a deep clone
     */
    public ExecDynamicSqlInstruction clone() {
        ExecDynamicSqlInstruction clone = new ExecDynamicSqlInstruction(
                parentProcCommand, dynamicCommand, metadata, idGenerator, capFinder, returnable);
        return clone;
    }

    public String toString() {
        return "ExecDynamicSqlInstruction"; //$NON-NLS-1$
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = new PlanNode("ExecDynamicSqlInstruction"); //$NON-NLS-1$
        props.addProperty(PROP_SQL, dynamicCommand.toString());
        return props;
    }

    public boolean isReturnable() {
        return returnable;
    }

    public void setReturnable(boolean returnable) {
        this.returnable = returnable;
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        Boolean expressionRequires = SubqueryAwareRelationalNode.requiresTransaction(transactionalReads, ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(dynamicCommand.getSql()));
        if (expressionRequires != null && expressionRequires) {
            return true;
        }
        if (this.dynamicCommand.getUsing() != null) {
            Boolean setRequires = SubqueryAwareRelationalNode.requiresTransaction(transactionalReads, ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(this.dynamicCommand.getUsing().getClauseMap().values()));
            if (setRequires == null) {
                if (expressionRequires == null) {
                    return true;
                }
                expressionRequires = null;
            } else if (setRequires) {
                return true;
            }
        }
        if (dynamicCommand.getUpdatingModelCount() > 1) {
            return true;
        }
        CommandContext context = CommandContext.getThreadLocalContext();
        if (context != null && context.isAtomicBlock()) {
            //we can't be sure what is being executed yet, so assume we need a txn
            //must be checked before returning null
            //when there is no atomic block (just auto commit) we expect the update clause to be used
            return true;
        }
        if (transactionalReads || dynamicCommand.getUpdatingModelCount() == 1) {
            return expressionRequires==null?true:null;
        }
        return expressionRequires;
    }

}
