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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

import org.teiid.PreParser;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.RequestMessage.ShowPlan;
import org.teiid.client.xa.XATransactionException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.process.AuthorizationValidator.CommandType;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.metadata.TempCapabilitiesFinder;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.proc.ProcedurePlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.ExplainCommand;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options;
import org.teiid.query.validator.AbstractValidationVisitor;
import org.teiid.query.validator.ValidationVisitor;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;


/**
 * Server side representation of the RequestMessage.  Knows how to process itself.
 */
public class Request {

    private static final String CLEAN_LOBS_ONCLOSE = "clean_lobs_onclose"; //$NON-NLS-1$
    // init state
    protected RequestMessage requestMsg;
    private String vdbName;
    private String vdbVersion;
    private BufferManager bufferManager;
    private ProcessorDataManager processorDataManager;
    private TransactionService transactionService;
    private TempTableStore tempTableStore;
    protected IDGenerator idGenerator = new IDGenerator();
    DQPWorkContext workContext;
    RequestID requestId;

    // acquired state
    protected CapabilitiesFinder capabilitiesFinder;
    protected QueryMetadataInterface metadata;

    // internal results
    protected boolean addedLimit;
    protected ProcessorPlan processPlan;
    // external results
    protected AnalysisRecord analysisRecord;
    protected CommandContext context;
    protected QueryProcessor processor;

    protected TransactionContext transactionContext;
    protected ConnectorManagerRepository connectorManagerRepo;

    protected Command userCommand;
    protected ExplainCommand explainCommand;
    protected boolean returnsUpdateCount;
    private GlobalTableStore globalTables;
    private SessionAwareCache<PreparedPlan> planCache;
    private boolean resultSetCacheEnabled = true;
    private int userRequestConcurrency;
    private AuthorizationValidator authorizationValidator;
    private Executor executor;
    protected Options options;
    protected PreParser preParser;

    void initialize(RequestMessage requestMsg,
                              BufferManager bufferManager,
                              ProcessorDataManager processorDataManager,
                              TransactionService transactionService,
                              TempTableStore tempTableStore,
                              DQPWorkContext workContext,
                              SessionAwareCache<PreparedPlan> planCache) {

        this.requestMsg = requestMsg;
        this.vdbName = workContext.getVdbName();
        this.vdbVersion = workContext.getVdbVersion();
        this.bufferManager = bufferManager;
        this.processorDataManager = processorDataManager;
        this.transactionService = transactionService;
        this.tempTableStore = tempTableStore;
        this.workContext = workContext;
        this.requestId = workContext.getRequestID(this.requestMsg.getExecutionId());
        this.connectorManagerRepo = workContext.getVDB().getAttachment(ConnectorManagerRepository.class);
        this.planCache = planCache;
    }

    public void setOptions(Options options) {
        this.options = options;
    }

    void setMetadata(CapabilitiesFinder capabilitiesFinder, QueryMetadataInterface metadata) {
        this.capabilitiesFinder = capabilitiesFinder;
        this.metadata = metadata;
    }

    public void setResultSetCacheEnabled(boolean resultSetCacheEnabled) {
        this.resultSetCacheEnabled = resultSetCacheEnabled;
    }

    public void setAuthorizationValidator(
            AuthorizationValidator authorizationValidator) {
        this.authorizationValidator = authorizationValidator;
    }

    /**
     * if the metadata has not been supplied via setMetadata, this method will create the appropriate state
     *
     * @throws TeiidComponentException
     */
    protected void initMetadata() throws TeiidComponentException {
        if (this.metadata != null) {
            return;
        }
        // Prepare dependencies for running the optimizer
        this.capabilitiesFinder = new CachedFinder(this.connectorManagerRepo, workContext.getVDB());
        if (this.bufferManager.getOptions() != null) {
            this.capabilitiesFinder = new TempCapabilitiesFinder(this.capabilitiesFinder, this.bufferManager.getOptions().getDefaultNullOrder());
        } else {
            this.capabilitiesFinder = new TempCapabilitiesFinder(this.capabilitiesFinder);
        }

        VDBMetaData vdbMetadata = workContext.getVDB();
        metadata = vdbMetadata.getAttachment(QueryMetadataInterface.class);
        if (this.workContext.isDerived()) {
            //derived should operate like a procedure scope and execute with
            //design time metadata
            metadata = metadata.getDesignTimeMetadata();
        }
        globalTables = vdbMetadata.getAttachment(GlobalTableStore.class);

        if (metadata == null) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30489, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30489, this.vdbName, this.vdbVersion));
        }

        TempMetadataAdapter tma = new TempMetadataAdapter(metadata, this.tempTableStore.getMetadataStore());
        tma.setSession(true);
        this.metadata = tma;
    }

    protected void createCommandContext() {
        if (this.context != null) {
            return;
        }
        // Create command context, used in rewriting, planning, and processing
        // Identifies a "group" of requests on a per-connection basis to allow later
        // cleanup of all resources in the group on connection shutdown
        String groupName = workContext.getSessionId();

        this.context =
            new CommandContext(
                groupName,
                workContext.getUserName(),
                requestMsg.getExecutionPayload(),
                workContext.getVdbName(),
                workContext.getVdbVersion(),
                this.requestMsg.getShowPlan() != ShowPlan.OFF
                || LogManager.isMessageToBeRecorded(LogConstants.CTX_COMMANDLOGGING, MessageLevel.TRACE));
        this.context.setProcessorBatchSize(bufferManager.getProcessorBatchSize());
        this.context.setGlobalTableStore(this.globalTables);
        boolean autoCleanLobs = true;
        if (this.workContext.getSession().isEmbedded()) {
            Object value = this.workContext.getSession().getSessionVariables().get(CLEAN_LOBS_ONCLOSE);
            if (value != null) {
                value = DataTypeManager.convertToRuntimeType(value, false);
                try {
                    value = DataTypeManager.transformValue(value, value.getClass(), DataTypeManager.DefaultDataClasses.BOOLEAN);
                    if (!(Boolean)value) {
                        autoCleanLobs = false;
                    }
                } catch (TransformationException e) {
                    LogManager.logDetail(LogConstants.CTX_DQP, e, "Improper value for", CLEAN_LOBS_ONCLOSE); //$NON-NLS-1$
                }
            }
        }
        if (!autoCleanLobs) {
            context.disableAutoCleanLobs();
        }
        context.setExecutor(this.executor);
        context.setAuthoriziationValidator(authorizationValidator);
        context.setTempTableStore(tempTableStore);
        context.setQueryProcessorFactory(new QueryProcessorFactoryImpl(this.bufferManager, this.processorDataManager, this.capabilitiesFinder, idGenerator, metadata));
        context.setMetadata(this.metadata);
        context.setBufferManager(this.bufferManager);
        context.setPreparedPlanCache(planCache);
        context.setResultSetCacheEnabled(this.resultSetCacheEnabled);
        context.setUserRequestSourceConcurrency(this.userRequestConcurrency);
        context.setSubject(workContext.getSubject());
        this.context.setOptions(options);
        this.context.setSession(workContext.getSession());
        this.context.setRequestId(this.requestId);
        this.context.setDQPWorkContext(this.workContext);
        this.context.setTransactionService(this.transactionService);
        this.context.setVDBClassLoader(workContext.getVDB().getAttachment(ClassLoader.class));
    }

    public void setUserRequestConcurrency(int userRequestConcurrency) {
        this.userRequestConcurrency = userRequestConcurrency;
    }

    protected void checkReferences(List<Reference> references) throws QueryValidatorException {
        referenceCheck(references);
    }

    static void referenceCheck(List<Reference> references) throws QueryValidatorException {
        if (references != null && !references.isEmpty()) {
             throw new QueryValidatorException(QueryPlugin.Event.TEIID30491, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30491));
        }
    }

    protected void resolveCommand(Command command) throws QueryResolverException, TeiidComponentException {
        //ensure that the user command is distinct from the processing command
        //rewrite and planning may alter options, symbols, etc.
        QueryResolver.resolveCommand(command, metadata);
    }

    private void validateQuery(Command command)
        throws QueryValidatorException, TeiidComponentException {

        // Create generic sql validation visitor
        AbstractValidationVisitor visitor = new ValidationVisitor();
        validateWithVisitor(visitor, metadata, command);
    }

    private Command parseCommand() throws QueryParserException {
        if (requestMsg.getCommand() != null) {
            return (Command)requestMsg.getCommand();
        }
        String[] commands = requestMsg.getCommands();
        ParseInfo parseInfo = createParseInfo(this.requestMsg, this.workContext.getSession());
        QueryParser queryParser = QueryParser.getQueryParser();
        if (requestMsg.isPreparedStatement() || requestMsg.isCallableStatement() || !requestMsg.isBatchedUpdate()) {
            String commandStr = commands[0];
            if (preParser != null) {
                commandStr = preParser.preParse(commandStr, this.context);
            }
            return queryParser.parseCommand(commandStr, parseInfo);
        }
        List<Command> parsedCommands = new ArrayList<Command>(commands.length);
        for (int i = 0; i < commands.length; i++) {
            String updateCommand = commands[i];
            if (preParser != null) {
                updateCommand = preParser.preParse(updateCommand, this.context);
            }
            parsedCommands.add(queryParser.parseCommand(updateCommand, parseInfo));
        }
        return new BatchedUpdateCommand(parsedCommands);
    }

    public static ParseInfo createParseInfo(RequestMessage requestMsg, SessionMetadata sessionMetadata) {
        ParseInfo parseInfo = new ParseInfo();
        parseInfo.ansiQuotedIdentifiers = requestMsg.isAnsiQuotedIdentifiers();
        Object value = sessionMetadata.getSessionVariables().get("backslashDefaultMatchEscape"); //$NON-NLS-1$
        try {
            if (value != null && Boolean.TRUE.equals(DataTypeManager.transformValue(value, DataTypeManager.DefaultDataClasses.BOOLEAN))) {
                parseInfo.setBackslashDefaultMatchEscape(true);
            }
        } catch (TransformationException e) {
        }
        return parseInfo;
    }

    public static void validateWithVisitor(
        AbstractValidationVisitor visitor,
        QueryMetadataInterface metadata,
        Command command)
        throws QueryValidatorException, TeiidComponentException {

        // Validate with visitor
        ValidatorReport report = Validator.validate(command, metadata, visitor);
        if (report.hasItems()) {
            ValidatorFailure firstFailure = report.getItems().iterator().next();
             throw new QueryValidatorException(QueryPlugin.Event.TEIID30492, firstFailure.getMessage());
        }
    }

    private void createProcessor() throws TeiidComponentException {
        if (this.userCommand instanceof CreateProcedureCommand && this.processPlan instanceof ProcedurePlan) {
            ((ProcedurePlan)this.processPlan).setValidateAccess(true);
        }
        ProcessorPlan plan = this.processPlan;
        if (this.explainCommand != null) {
            //this is the meat of how explain works
            //we set our existing flags, and create a proxy plan
            //that keeps knowledge of explain out of the other layers
            if (requestMsg.getShowPlan() != ShowPlan.DEBUG) {
                requestMsg.setShowPlan(ShowPlan.ON);
                if (!this.explainCommand.isNoExec()) {
                    //the context was already initialized, so this still needs set
                    this.context.setCollectNodeStatistics(true);
                }
            }
            if (this.requestMsg.getRequestOptions().isContinuous()) {
                throw new IllegalStateException("Explain cannot be continuous."); //$NON-NLS-1$
            }
            plan = new ExplainProcessPlan(this.processPlan, this.explainCommand);
        }
        this.context.setTransactionContext(getTransactionContext(true));
        this.processor = new QueryProcessor(plan, context, bufferManager, processorDataManager);
    }

    TransactionContext getTransactionContext(boolean startAutoWrap) throws TeiidComponentException {
        if (this.transactionContext != null) {
            return this.transactionContext;
        }
        TransactionContext tc = transactionService.getOrCreateTransactionContext(workContext.getSessionId());

        if (tc.getTransactionType() == TransactionContext.Scope.REQUEST && this.workContext.isDerived()) {
            //to a sub-request, request scope should appear as global - which means associated and non-suspendable
            tc = tc.clone();
            tc.setTransactionType(TransactionContext.Scope.INHERITED);
        }

        Assertion.assertTrue(tc.getTransactionType() != TransactionContext.Scope.REQUEST, "Transaction already associated with request."); //$NON-NLS-1$

        // If local or global transaction is not started.
        if (tc.getTransactionType() == Scope.NONE && !requestMsg.isNoExec() && (explainCommand == null || !explainCommand.isNoExec())) {
            if (!startAutoWrap) {
                return null;
            }
            Boolean startAutoWrapTxn = false;

            if(RequestMessage.TXN_WRAP_ON.equals(requestMsg.getTxnAutoWrapMode())){
                startAutoWrapTxn = true;
            } else if (RequestMessage.TXN_WRAP_DETECT.equals(requestMsg.getTxnAutoWrapMode())){
                boolean transactionalRead = requestMsg.getTransactionIsolation() == Connection.TRANSACTION_REPEATABLE_READ
                        || requestMsg.getTransactionIsolation() == Connection.TRANSACTION_SERIALIZABLE;
                startAutoWrapTxn = processPlan.requiresTransaction(transactionalRead);
                if (startAutoWrapTxn == null) {
                    startAutoWrapTxn = false;
                }
            }

            if (startAutoWrapTxn) {
                try {
                    transactionService.begin(tc);
                } catch (XATransactionException err) {
                     throw new TeiidComponentException(QueryPlugin.Event.TEIID30493, err);
                }
            }
        }

        tc.setIsolationLevel(requestMsg.getTransactionIsolation());
        this.transactionContext = tc;
        return this.transactionContext;
    }

    /**
     * state side effects:
     *      creates the analysis record
     *         creates the command context
     *         sets the pre-rewrite command on the request
     *         adds a limit clause if the row limit is specified
     *         sets the processor plan
     *
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    protected void generatePlan(boolean prepared) throws TeiidComponentException, TeiidProcessingException {
        createCommandContext();
        Command command = parseCommand();
        if (command.getType() == Command.TYPE_EXPLAIN) {
            this.explainCommand = (ExplainCommand)command;
            command = command.getActualCommand();
            //no need to hold the reference
            this.explainCommand.setCommand(null);
        }

        List<Reference> references = ReferenceCollectorVisitor.getReferences(command);

        getAnalysisRecord();

        resolveCommand(command);

        checkReferences(references);

        validateAccess(requestMsg.getCommands(), command, CommandType.USER);

        this.userCommand = (Command) command.clone();

        Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroups(command, true);
        for (GroupSymbol groupSymbol : groups) {
            if (groupSymbol.isTempTable()) {
                this.context.setDeterminismLevel(Determinism.SESSION_DETERMINISTIC);
                break;
            }
        }

        validateQuery(command);

        command = QueryRewriter.rewrite(command, metadata, context);

        /*
         * Adds a row limit to a query if Statement.setMaxRows has been called and the command
         * doesn't already have a limit clause.
         */
        if (!prepared && requestMsg.getRowLimit() > 0 && command instanceof QueryCommand) {
            QueryCommand query = (QueryCommand)command;
            if (query.getLimit() == null) {
                query.setLimit(new Limit(null, new Constant(new Integer(requestMsg.getRowLimit()), DataTypeManager.DefaultDataClasses.INTEGER)));
                this.addedLimit = true;
            }
        }

        boolean debug = analysisRecord.recordDebug();
        if(debug) {
            analysisRecord.println("\n============================================================================"); //$NON-NLS-1$
            analysisRecord.println("USER COMMAND:\n" + command);         //$NON-NLS-1$
        }
        // Run the optimizer
        try {
            CommandContext.pushThreadLocalContext(context);
            processPlan = QueryOptimizer.optimizePlan(command, metadata, idGenerator, capabilitiesFinder, analysisRecord, context);
        } finally {
            CommandContext.popThreadLocalContext();
            String debugLog = analysisRecord.getDebugLog();
            if(debugLog != null && debugLog.length() > 0) {
                LogManager.log(requestMsg.getShowPlan()==ShowPlan.DEBUG?MessageLevel.INFO:MessageLevel.TRACE, LogConstants.CTX_QUERY_PLANNER, debugLog);
            }
            if (analysisRecord.recordAnnotations() && analysisRecord.getAnnotations() != null && !analysisRecord.getAnnotations().isEmpty()) {
                LogManager.logDetail(LogConstants.CTX_QUERY_PLANNER, analysisRecord.getAnnotations());
            }
        }
        LogManager.logDetail(LogConstants.CTX_DQP, new Object[] { QueryPlugin.Util.getString("BasicInterceptor.ProcessTree_for__4"), requestId, processPlan }); //$NON-NLS-1$
    }

    private AnalysisRecord getAnalysisRecord() {
        if (this.analysisRecord == null) {
            this.analysisRecord = new AnalysisRecord(requestMsg.getShowPlan() != ShowPlan.OFF, requestMsg.getShowPlan() == ShowPlan.DEBUG);
        }
        return this.analysisRecord;
    }

    public void processRequest()
        throws TeiidComponentException, TeiidProcessingException {

        LogManager.logDetail(LogConstants.CTX_DQP, this.requestId, "executing", this.requestMsg.isPreparedStatement()?"prepared":"", this.requestMsg.getCommandString()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        initMetadata();

        generatePlan(false);

        createProcessor();
    }

    protected boolean validateAccess(String[] commandStr, Command command, CommandType type) throws QueryValidatorException, TeiidComponentException {
        boolean returnsResultSet = command.returnsResultSet();
        this.returnsUpdateCount = !(command instanceof StoredProcedure) && !returnsResultSet && explainCommand == null;
        if ((this.requestMsg.getResultsMode() == ResultsMode.UPDATECOUNT && returnsResultSet)
                || (this.requestMsg.getResultsMode() == ResultsMode.RESULTSET && !returnsResultSet)) {
            throw new QueryValidatorException(QueryPlugin.Event.TEIID30490, QueryPlugin.Util.getString(this.requestMsg.getResultsMode()==ResultsMode.RESULTSET?"Request.no_result_set":"Request.result_set")); //$NON-NLS-1$ //$NON-NLS-2$
        }
        createCommandContext();
        if (this.requestMsg.isReturnAutoGeneratedKeys() && command instanceof Insert) {
            Insert insert = (Insert)command;
            List<ElementSymbol> variables = ResolverUtil.resolveElementsInGroup(insert.getGroup(), metadata);
            variables.removeAll(insert.getVariables());
            Object pk = metadata.getPrimaryKey(insert.getGroup().getMetadataID());
            if (pk != null) {
                List<?> cols = metadata.getElementIDsInKey(pk);
                for (Iterator<ElementSymbol> iter = variables.iterator(); iter.hasNext();) {
                    ElementSymbol variable = iter.next();
                    if (!(metadata.elementSupports(variable.getMetadataID(), SupportConstants.Element.NULL) ||
                            metadata.elementSupports(variable.getMetadataID(), SupportConstants.Element.AUTO_INCREMENT))
                            || !cols.contains(variable.getMetadataID())) {
                        iter.remove();
                    }
                }
                context.setReturnAutoGeneratedKeys(variables);
            }
        }
        if (!this.workContext.isAdmin() && this.authorizationValidator != null) {
            return this.authorizationValidator.validate(commandStr, command, metadata, context, type);
        }
        return false;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public boolean isReturingParams() {
        return false;
    }

    public void setPreParser(PreParser preParser) {
        this.preParser = preParser;
    }

}
