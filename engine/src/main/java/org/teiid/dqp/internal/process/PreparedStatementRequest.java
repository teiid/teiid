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

package org.teiid.dqp.internal.process;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BaseLob;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.Streamable;
import org.teiid.dqp.internal.process.AuthorizationValidator.CommandType;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.BatchedUpdatePlanner;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.relational.rules.CriteriaCapabilityValidatorVisitor;
import org.teiid.query.optimizer.relational.rules.CriteriaCapabilityValidatorVisitor.ValidatorOptions;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;


/**
 * Specific request for handling prepared statement calls.
 */
public class PreparedStatementRequest extends Request {
    private SessionAwareCache<PreparedPlan> prepPlanCache;
    private PreparedPlan prepPlan;

    public PreparedStatementRequest(SessionAwareCache<PreparedPlan> prepPlanCache) {
        this.prepPlanCache = prepPlanCache;
    }

    @Override
    protected void checkReferences(List<Reference> references)
            throws QueryValidatorException {
        for (Iterator<Reference> i = references.iterator(); i.hasNext();) {
            if (i.next().isOptional()) {
                i.remove(); //remove any optional parameter, which accounts for out params - the client does not send any bindings
            }
        }
        prepPlan.setReferences(references);
    }

    @Override
    protected void generatePlan(boolean addLimit) throws TeiidComponentException, TeiidProcessingException {
        createCommandContext();
        String sqlQuery = requestMsg.getCommands()[0];
        if (this.preParser != null) {
            sqlQuery = this.preParser.preParse(sqlQuery, this.context);
        }
        CacheID id = new CacheID(this.workContext, Request.createParseInfo(this.requestMsg, this.workContext.getSession()), sqlQuery);
        prepPlan = prepPlanCache.get(id);

        if (prepPlan != null) {
            //already in cache. obtain the values from cache
            analysisRecord = prepPlan.getAnalysisRecord();
            ProcessorPlan cachedPlan = prepPlan.getPlan();
            this.userCommand = prepPlan.getCommand();
            if (validateAccess(requestMsg.getCommands(), userCommand, CommandType.PREPARED)) {
                LogManager.logDetail(LogConstants.CTX_DQP, requestId, "AuthorizationValidator indicates that the prepared plan for command will not be used"); //$NON-NLS-1$
                prepPlan = null;
                analysisRecord = null;
            } else {
                LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Query exist in cache: ", sqlQuery }); //$NON-NLS-1$
                processPlan = cachedPlan.clone();
            }
        }

        if (prepPlan == null) {
            //if prepared plan does not exist, create one
            prepPlan = new PreparedPlan();
            LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Query does not exist in cache: ", sqlQuery}); //$NON-NLS-1$
            super.generatePlan(true);
            prepPlan.setCommand(this.userCommand);

            //there's no need to cache the plan if it's explain or a stored procedure, since we already do that in the optimizer
            boolean cache = !(this.userCommand instanceof StoredProcedure) && explainCommand == null;

            // Defect 13751: Clone the plan in its current state (i.e. before processing) so that it can be used for later queries
            prepPlan.setPlan(cache?processPlan.clone():processPlan, this.context);
            prepPlan.setAnalysisRecord(analysisRecord);

            if (cache) {
                Determinism determinismLevel = this.context.getDeterminismLevel();
                if (userCommand.getCacheHint() != null && userCommand.getCacheHint().getDeterminism() != null) {
                    LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Cache hint modified the query determinism from ",this.context.getDeterminismLevel(), " to ", determinismLevel }); //$NON-NLS-1$ //$NON-NLS-2$
                    determinismLevel = userCommand.getCacheHint().getDeterminism();
                }

                this.prepPlanCache.put(id, determinismLevel, prepPlan, userCommand.getCacheHint() != null?userCommand.getCacheHint().getTtl():null);
            }
        }

        if (requestMsg.isBatchedUpdate()) {
            handlePreparedBatchUpdate();
        } else {
            List<Reference> params = prepPlan.getReferences();
            List<?> values = requestMsg.getParameterValues();

            PreparedStatementRequest.resolveParameterValues(params, values, this.context, this.metadata);
        }
    }

    /**
     * There are two cases
     *   if
     *     The source supports preparedBatchUpdate -> just let the command and values pass to the source
     *   else
     *     create a batchedupdatecommand that represents the batch operation
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     * @throws QueryResolverException
     * @throws QueryPlannerException
     * @throws QueryValidatorException
     */
    private void handlePreparedBatchUpdate() throws QueryMetadataException,
            TeiidComponentException, QueryResolverException, QueryPlannerException, QueryValidatorException {
        List<List<?>> paramValues = (List<List<?>>) requestMsg.getParameterValues();
        if (paramValues.isEmpty()) {
             throw new QueryValidatorException(QueryPlugin.Event.TEIID30555, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30555));
        }
        boolean supportPreparedBatchUpdate = false;
        Command command = null;
        AccessNode aNode = CriteriaCapabilityValidatorVisitor.getAccessNode(this.processPlan);
        if (aNode != null) {
            String modelName = aNode.getModelName();
            command = aNode.getCommand();
            SourceCapabilities caps = capabilitiesFinder.findCapabilities(modelName);
            supportPreparedBatchUpdate = caps.supportsCapability(SourceCapabilities.Capability.BULK_UPDATE);
            if (supportPreparedBatchUpdate
                    //only allow the plan if the multi-valued references result in expressions that can be pushed
                    && !CriteriaCapabilityValidatorVisitor.canPushLanguageObject(command, metadata.getModelID(modelName), metadata, capabilitiesFinder, analysisRecord, new ValidatorOptions(false, false, true))) {
                supportPreparedBatchUpdate = false;
            }
        }
        List<Command> commands = new LinkedList<Command>();
        List<VariableContext> contexts = new LinkedList<VariableContext>();
        List<List<Object>> multiValues = new ArrayList<List<Object>>(this.prepPlan.getReferences().size());
        for (List<?> values : paramValues) {
            PreparedStatementRequest.resolveParameterValues(this.prepPlan.getReferences(), values, this.context, this.metadata);
            contexts.add(this.context.getVariableContext());
            if(supportPreparedBatchUpdate){
                if (multiValues.isEmpty()) {
                    for (int i = 0; i < values.size(); i++) {
                        multiValues.add(new ArrayList<Object>(paramValues.size()));
                    }
                }
                for (int i = 0; i < values.size(); i++) {
                    List<Object> multiValue = multiValues.get(i);
                    Object value = this.context.getVariableContext().getGlobalValue(this.prepPlan.getReferences().get(i).getContextSymbol());
                    multiValue.add(value);
                }
            } else { //just accumulate copies of the command/plan - clones are not necessary
                if (command == null) {
                    command = this.prepPlan.getCommand();
                }
                command.setProcessorPlan(this.processPlan);
                commands.add(command);
            }
        }

        if (paramValues.size() > 1) {
            this.context.setVariableContext(new VariableContext());
        }

        if (paramValues.size() == 1) {
            return; // just use the existing plan, and global reference evaluation
        }

        if (supportPreparedBatchUpdate) {
            for (int i = 0; i < this.prepPlan.getReferences().size(); i++) {
                Constant c = new Constant(null, this.prepPlan.getReferences().get(i).getType());
                c.setMultiValued(multiValues.get(i));
                this.context.getVariableContext().setGlobalValue(this.prepPlan.getReferences().get(i).getContextSymbol(), c);
            }
            return;
        }

        BatchedUpdateCommand buc = new BatchedUpdateCommand(commands);
        buc.setVariableContexts(contexts);
        BatchedUpdatePlanner planner = new BatchedUpdatePlanner();
        this.processPlan = planner.optimize(buc, idGenerator, metadata, capabilitiesFinder, analysisRecord, context);
    }

    /**
     * @param params
     * @param values
     * @throws QueryResolverException
     * @throws QueryValidatorException
     */
    public static void resolveParameterValues(List<Reference> params,
                                        List values, CommandContext context, QueryMetadataInterface metadata) throws QueryResolverException, TeiidComponentException, QueryValidatorException {
        VariableContext result = new VariableContext();
        //the size of the values must be the same as that of the parameters
        if (params.size() != values.size()) {
            String msg = QueryPlugin.Util.getString("QueryUtil.wrong_number_of_values", new Object[] {new Integer(values.size()), new Integer(params.size())}); //$NON-NLS-1$
             throw new QueryResolverException(QueryPlugin.Event.TEIID30556, msg);
        }

        boolean embedded = context != null && context.getSession() != null && context.getSession().isEmbedded();

        //the type must be the same, or the type of the value can be implicitly converted
        //to that of the reference
        for (int i = 0; i < params.size(); i++) {
            Reference param = params.get(i);
            Object value = values.get(i);

            if(value != null) {
                if (embedded && value instanceof BaseLob) {
                    createStreamCopy(context, i, param, value);
                }
                try {
                    String targetTypeName = DataTypeManager.getDataTypeName(param.getType());
                    Expression expr = ResolverUtil.convertExpression(new Constant(DataTypeManager.convertToRuntimeType(value, param.getType() != DataTypeManager.DefaultDataClasses.OBJECT)), targetTypeName, metadata);
                    value = Evaluator.evaluate(expr);
                } catch (ExpressionEvaluationException e) {
                    String msg = QueryPlugin.Util.getString("QueryUtil.Error_executing_conversion_function_to_convert_value", i + 1, value, value.getClass(), DataTypeManager.getDataTypeName(param.getType())); //$NON-NLS-1$
                    throw new QueryResolverException(QueryPlugin.Event.TEIID30557, e, msg);
                } catch (QueryResolverException e) {
                    String msg = QueryPlugin.Util.getString("QueryUtil.Error_executing_conversion_function_to_convert_value", i + 1, value, value.getClass(), DataTypeManager.getDataTypeName(param.getType())); //$NON-NLS-1$
                    throw new QueryResolverException(QueryPlugin.Event.TEIID30558, e, msg);
                }
            }

            if (param.getConstraint() != null) {
                param.getConstraint().validate(value);
            }
            //bind variable
            result.setGlobalValue(param.getContextSymbol(), value);
        }

        context.setVariableContext(result);
    }

    /**
     * embedded lobs can be sent with just a reference to a stream,
     * create a copy instead
     * @param context
     * @param i
     * @param param
     * @param value
     * @throws QueryResolverException
     */
    private static void createStreamCopy(CommandContext context, int i,
            Reference param, Object value) throws QueryResolverException {
        try {
            InputStreamFactory isf = ((BaseLob)value).getStreamFactory();
            InputStream initial = isf.getInputStream();
            InputStream other = isf.getInputStream();
            if (initial == other) {
                //this violates the expectation that the inputstream is a new instance,
                FileStore fs = context.getBufferManager().createFileStore("bytes"); //$NON-NLS-1$
                FileStoreInputStreamFactory fsisf = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);

                SaveOnReadInputStream is = new SaveOnReadInputStream(initial, fsisf);
                context.addCreatedLob(fsisf);
                ((BaseLob) value).setStreamFactory(is.getInputStreamFactory());
            } else {
                initial.close();
                other.close();
            }
        } catch (SQLException e) {
            String msg = QueryPlugin.Util.getString("QueryUtil.Error_executing_conversion_function_to_convert_value", i + 1, value, value.getClass(), DataTypeManager.getDataTypeName(param.getType())); //$NON-NLS-1$
            throw new QueryResolverException(QueryPlugin.Event.TEIID30557, e, msg);
        } catch (IOException e) {
            String msg = QueryPlugin.Util.getString("QueryUtil.Error_executing_conversion_function_to_convert_value", i + 1, value, value.getClass(), DataTypeManager.getDataTypeName(param.getType())); //$NON-NLS-1$
            throw new QueryResolverException(QueryPlugin.Event.TEIID30557, e, msg);
        }
    }

    @Override
    public boolean isReturingParams() {
        if (userCommand instanceof StoredProcedure) {
            StoredProcedure sp = (StoredProcedure)userCommand;
            if (sp.isCallableStatement() && sp.returnsResultSet()) {
                for (SPParameter param : sp.getMapOfParameters().values()) {
                    int type = param.getParameterType();
                    if (type == SPParameter.INOUT || type == SPParameter.OUT || type == SPParameter.RETURN_VALUE) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public void processRequest() throws TeiidComponentException,
            TeiidProcessingException {
        super.processRequest();
        if (this.requestMsg.getRequestOptions().isContinuous()) {
            this.processor.setContinuous(this.prepPlan, this.requestMsg.getCommandString());
        }
    }
}
