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

package org.teiid.dqp.internal.process;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryPlannerException;
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
import org.teiid.core.id.IntegerIDFactory;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.process.multisource.MultiSourceCapabilitiesFinder;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.dqp.internal.process.multisource.MultiSourcePlanToProcessConverter;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionService;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.MetadataProvider;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.MetadataProvider.ViewDefinition;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.eval.SecurityFunctionEvaluator;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.BasicQueryMetadataWrapper;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempCapabilitiesFinder;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.processor.xml.XMLPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.ContextProperties;
import org.teiid.query.validator.AbstractValidationVisitor;
import org.teiid.query.validator.ValidationVisitor;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;


/**
 * Server side representation of the RequestMessage.  Knows how to process itself.
 */
public class Request implements SecurityFunctionEvaluator {
    
	private final class ViewDefinitionMetadataWrapper extends
			BasicQueryMetadataWrapper {
		
		private Map<List<String>, QueryNode> qnodes = new HashMap<List<String>, QueryNode>();
		
		private ViewDefinitionMetadataWrapper(
				QueryMetadataInterface actualMetadata) {
			super(actualMetadata);
		}

		@Override
		public QueryNode getVirtualPlan(Object groupID)
				throws TeiidComponentException, QueryMetadataException {
			QueryNode result = super.getVirtualPlan(groupID);
			//if there's no exception, then this must be a visible view

			String schema = getName(getModelID(groupID));
			String viewName = getName(groupID);
			List<String> key = Arrays.asList(schema, viewName);
			
			QueryNode cached = qnodes.get(key);
			if (cached != null) {
				return cached;
			}
			if (context == null) {
				//TODO: could just consider moving up when the context is created
				throw new AssertionError("Should not attempt to resolve a view before the context has been set."); //$NON-NLS-1$
			}
			ViewDefinition vd = metadataProvider.getViewDefinition(getName(getModelID(groupID)), getName(groupID), context);
			if (vd != null) {
				result = new QueryNode(DataTypeManager.getCanonicalString(vd.getSql()));
				if (vd.getScope() == MetadataProvider.Scope.USER) {
					result.setUser(context.getUserName());
				}
			}
			qnodes.put(key, result);
			return result;
		}

		@Override
		public QueryMetadataInterface getDesignTimeMetadata() {
			return new ViewDefinitionMetadataWrapper(this.actualMetadata.getDesignTimeMetadata());
		}
	}

	// init state
    protected RequestMessage requestMsg;
    private String vdbName;
    private int vdbVersion;
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
    private Set<String> multiSourceModels;

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
    protected boolean returnsUpdateCount;
	private TempTableStore globalTables;
	private SessionAwareCache<PreparedPlan> planCache;
	private boolean resultSetCacheEnabled = true;
	private int userRequestConcurrency;
	private AuthorizationValidator authorizationValidator;
	private MetadataProvider metadataProvider;

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
        idGenerator.setDefaultFactory(new IntegerIDFactory());
        this.workContext = workContext;
        this.requestId = workContext.getRequestID(this.requestMsg.getExecutionId());
        this.connectorManagerRepo = workContext.getVDB().getAttachment(ConnectorManagerRepository.class);
        this.planCache = planCache;
    }
    
	void setMetadata(CapabilitiesFinder capabilitiesFinder, QueryMetadataInterface metadata, Set multiSourceModels) {
		this.capabilitiesFinder = capabilitiesFinder;
		this.metadata = metadata;
		this.multiSourceModels = multiSourceModels;
	}
	
	public void setResultSetCacheEnabled(boolean resultSetCacheEnabled) {
		this.resultSetCacheEnabled = resultSetCacheEnabled;
	}
	
	public void setAuthorizationValidator(
			AuthorizationValidator authorizationValidator) {
		this.authorizationValidator = authorizationValidator;
	}
	
	public void setMetadataProvider(MetadataProvider metadataProvider) {
		this.metadataProvider = metadataProvider;
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
        this.capabilitiesFinder = new TempCapabilitiesFinder(this.capabilitiesFinder);

        VDBMetaData vdbMetadata = workContext.getVDB();
        metadata = vdbMetadata.getAttachment(QueryMetadataInterface.class);
        globalTables = vdbMetadata.getAttachment(TempTableStore.class);

        if (metadata == null) {
            throw new TeiidComponentException(QueryPlugin.Util.getString("DQPCore.Unable_to_load_metadata_for_VDB_name__{0},_version__{1}", this.vdbName, this.vdbVersion)); //$NON-NLS-1$
        }
        
        // Check for multi-source models and further wrap the metadata interface
        Set<String> multiSourceModelList = workContext.getVDB().getMultiSourceModelNames();
        if(multiSourceModelList != null && multiSourceModelList.size() > 0) {
        	this.multiSourceModels = multiSourceModelList;
            this.metadata = new MultiSourceMetadataWrapper(this.metadata, this.multiSourceModels);
        }
        
        if (this.metadataProvider != null) {
        	this.metadata = new ViewDefinitionMetadataWrapper(this.metadata);
        }
        
        TempMetadataAdapter tma = new TempMetadataAdapter(metadata, new TempMetadataStore());
        tma.setSession(true);
        this.metadata = tma;
    }
    
    protected void createCommandContext() throws QueryValidatorException {
    	boolean returnsResultSet = userCommand.returnsResultSet();
    	this.returnsUpdateCount = !(userCommand instanceof StoredProcedure) && !returnsResultSet;
    	if ((this.requestMsg.getResultsMode() == ResultsMode.UPDATECOUNT && !returnsUpdateCount) 
    			|| (this.requestMsg.getResultsMode() == ResultsMode.RESULTSET && !returnsResultSet)) {
        	throw new QueryValidatorException(QueryPlugin.Util.getString(this.requestMsg.getResultsMode()==ResultsMode.RESULTSET?"Request.no_result_set":"Request.result_set")); //$NON-NLS-1$ //$NON-NLS-2$
    	}

    	// Create command context, used in rewriting, planning, and processing
        // Identifies a "group" of requests on a per-connection basis to allow later
        // cleanup of all resources in the group on connection shutdown
        String groupName = workContext.getSessionId();

        RequestID reqID = workContext.getRequestID(this.requestMsg.getExecutionId());
        
        Properties props = new Properties();
        props.setProperty(ContextProperties.SESSION_ID, workContext.getSessionId());
        
        this.context =
            new CommandContext(
                reqID,
                groupName,
                workContext.getUserName(),
                requestMsg.getExecutionPayload(), 
                workContext.getVdbName(), 
                workContext.getVdbVersion(),
                props,
                this.requestMsg.getShowPlan() != ShowPlan.OFF);
        this.context.setProcessorBatchSize(bufferManager.getProcessorBatchSize());
        this.context.setConnectorBatchSize(bufferManager.getConnectorBatchSize());
        this.context.setGlobalTableStore(this.globalTables);
        if (multiSourceModels != null) {
            MultiSourcePlanToProcessConverter modifier = new MultiSourcePlanToProcessConverter(
					metadata, idGenerator, analysisRecord, capabilitiesFinder,
					multiSourceModels, workContext, context);
            context.setPlanToProcessConverter(modifier);
        }

        context.setSecurityFunctionEvaluator(this);
        context.setTempTableStore(tempTableStore);
        context.setQueryProcessorFactory(new QueryProcessorFactoryImpl(this.bufferManager, this.processorDataManager, this.capabilitiesFinder, idGenerator, metadata));
        context.setMetadata(this.metadata);
        context.setBufferManager(this.bufferManager);
        context.setPreparedPlanCache(planCache);
        context.setResultSetCacheEnabled(this.resultSetCacheEnabled);
        context.setUserRequestSourceConcurrency(this.userRequestConcurrency);
        context.setSubject(workContext.getSubject());
    }
    
    @Override
    public boolean hasRole(String roleType, String roleName)
    		throws TeiidComponentException {
        if (!DATA_ROLE.equalsIgnoreCase(roleType)) {
            return false;
        }
        return authorizationValidator.hasRole(roleName, workContext);
    }
    
    public void setUserRequestConcurrency(int userRequestConcurrency) {
		this.userRequestConcurrency = userRequestConcurrency;
	}

    protected void checkReferences(List<Reference> references) throws QueryValidatorException {
    	referenceCheck(references);
    }
    
    static void referenceCheck(List<Reference> references) throws QueryValidatorException {
    	if (references != null && !references.isEmpty()) {
    		throw new QueryValidatorException(QueryPlugin.Util.getString("Request.Invalid_character_in_query")); //$NON-NLS-1$
    	}
    }

    protected void resolveCommand(Command command) throws QueryResolverException, TeiidComponentException {
        if (this.tempTableStore != null) {
        	QueryResolver.setChildMetadata(command, tempTableStore.getMetadataStore().getData(), null);
        }
    	//ensure that the user command is distinct from the processing command
        //rewrite and planning may alter options, symbols, etc.
    	QueryResolver.resolveCommand(command, metadata);
    	
    	this.userCommand = (Command)command.clone();
    }
        
    private void validateQuery(Command command)
        throws QueryValidatorException, TeiidComponentException {
                
        // Create generic sql validation visitor
        AbstractValidationVisitor visitor = new ValidationVisitor();
        validateWithVisitor(visitor, metadata, command);
    }
    
    private Command parseCommand() throws QueryParserException {
        String[] commands = requestMsg.getCommands();
        ParseInfo parseInfo = createParseInfo(this.requestMsg);
        if (requestMsg.isPreparedStatement() || requestMsg.isCallableStatement() || !requestMsg.isBatchedUpdate()) {
        	String commandStr = commands[0];
            return QueryParser.getQueryParser().parseCommand(commandStr, parseInfo);
        } 
        List<Command> parsedCommands = new ArrayList<Command>(commands.length);
        for (int i = 0; i < commands.length; i++) {
        	String updateCommand = commands[i];
            parsedCommands.add(QueryParser.getQueryParser().parseCommand(updateCommand, parseInfo));
        }
        return new BatchedUpdateCommand(parsedCommands);
    }

	public static ParseInfo createParseInfo(RequestMessage requestMsg) {
		ParseInfo parseInfo = new ParseInfo();
    	parseInfo.ansiQuotedIdentifiers = requestMsg.isAnsiQuotedIdentifiers();
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
            throw new QueryValidatorException(firstFailure.getMessage());
        }
    }

    private void createProcessor() throws TeiidComponentException {
        
        TransactionContext tc = transactionService.getOrCreateTransactionContext(workContext.getSessionId());
        
        Assertion.assertTrue(tc.getTransactionType() != TransactionContext.Scope.REQUEST, "Transaction already associated with request."); //$NON-NLS-1$

        // If local or global transaction is not started.
        if (tc.getTransactionType() == Scope.NONE) {
            
            boolean startAutoWrapTxn = false;
            
            if(RequestMessage.TXN_WRAP_ON.equals(requestMsg.getTxnAutoWrapMode())){ 
                startAutoWrapTxn = true;
            } else if (RequestMessage.TXN_WRAP_DETECT.equals(requestMsg.getTxnAutoWrapMode())){
            	boolean transactionalRead = requestMsg.getTransactionIsolation() == Connection.TRANSACTION_REPEATABLE_READ
						|| requestMsg.getTransactionIsolation() == Connection.TRANSACTION_SERIALIZABLE;
        		startAutoWrapTxn = processPlan.requiresTransaction(transactionalRead);
            } 
            
            if (startAutoWrapTxn) {
                try {
                    tc = transactionService.begin(tc);
                } catch (XATransactionException err) {
                    throw new TeiidComponentException(err);
                }
            }
        } 
        
        this.transactionContext = tc;
        this.processor = new QueryProcessor(processPlan, context, bufferManager, processorDataManager);
    }

    /**
     * state side effects:
     *      creates the analysis record
     * 		creates the command context
     * 		sets the pre-rewrite command on the request
     * 		adds a limit clause if the row limit is specified
     * 		sets the processor plan
     * 
     * @throws TeiidComponentException
     * @throws TeiidProcessingException 
     */
    protected void generatePlan() throws TeiidComponentException, TeiidProcessingException {
        Command command = parseCommand();

        List<Reference> references = ReferenceCollectorVisitor.getReferences(command);
        
        checkReferences(references);
        
        this.analysisRecord = new AnalysisRecord(requestMsg.getShowPlan() != ShowPlan.OFF, requestMsg.getShowPlan() == ShowPlan.DEBUG);
                
        resolveCommand(command);
        
        validateAccess(userCommand);
        
        createCommandContext();

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
        if (requestMsg.getRowLimit() > 0 && command instanceof QueryCommand) {
            QueryCommand query = (QueryCommand)command;
            if (query.getLimit() == null) {
                query.setLimit(new Limit(null, new Constant(new Integer(requestMsg.getRowLimit()), DataTypeManager.DefaultDataClasses.INTEGER)));
                this.addedLimit = true;
            }
        }
        
        try {
        	// If using multi-source models, insert a proxy to simplify the supported capabilities.  This is 
            // done OUTSIDE the cache (wrapped around the cache) intentionally to avoid caching the simplified
            // capabilities which may be different for the same model in a different VDB used by this same DQP.
        	CapabilitiesFinder finder = this.capabilitiesFinder;
            if(this.multiSourceModels != null) {
                finder = new MultiSourceCapabilitiesFinder(finder, this.multiSourceModels);
            }
            
            boolean debug = analysisRecord.recordDebug();
    		if(debug) {
    			analysisRecord.println("\n============================================================================"); //$NON-NLS-1$
                analysisRecord.println("USER COMMAND:\n" + command);		 //$NON-NLS-1$
            }
            // Run the optimizer
            try {
                processPlan = QueryOptimizer.optimizePlan(command, metadata, idGenerator, finder, analysisRecord, context);
            } finally {
                String debugLog = analysisRecord.getDebugLog();
                if(debugLog != null && debugLog.length() > 0) {
                    LogManager.log(requestMsg.getShowPlan()==ShowPlan.DEBUG?MessageLevel.INFO:MessageLevel.TRACE, LogConstants.CTX_QUERY_PLANNER, debugLog);               
                }
                if (analysisRecord.recordAnnotations() && analysisRecord.getAnnotations() != null && !analysisRecord.getAnnotations().isEmpty()) {
                	LogManager.logDetail(LogConstants.CTX_QUERY_PLANNER, analysisRecord.getAnnotations());
                }
            }
            LogManager.logDetail(LogConstants.CTX_DQP, new Object[] { QueryPlugin.Util.getString("BasicInterceptor.ProcessTree_for__4"), requestId, processPlan }); //$NON-NLS-1$
        } catch (QueryMetadataException e) {
            throw new QueryPlannerException(e, QueryPlugin.Util.getString("DQPCore.Unknown_query_metadata_exception_while_registering_query__{0}.", requestId)); //$NON-NLS-1$
        }
    }

    public void processRequest() 
        throws TeiidComponentException, TeiidProcessingException {
                    
    	LogManager.logDetail(LogConstants.CTX_DQP, this.requestId, "executing", this.requestMsg.isPreparedStatement()?"prepared":"", this.requestMsg.getCommandString()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	
        initMetadata();
        
        generatePlan();
        
        postProcessXML();
        
        createProcessor();
    }

	private void postProcessXML() {
        if (requestMsg.getXMLFormat() != null && processPlan instanceof XMLPlan) {
        	((XMLPlan)processPlan).setXMLFormat(requestMsg.getXMLFormat());
        }
        this.context.setValidateXML(requestMsg.getValidationMode());
	}

	protected void validateAccess(Command command) throws QueryValidatorException, TeiidComponentException {
		this.authorizationValidator.validate(command, metadata, workContext);
	}
	
}
