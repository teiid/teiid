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
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.xa.XATransactionException;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.dqp.internal.process.multisource.MultiSourceCapabilitiesFinder;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.dqp.internal.process.multisource.MultiSourcePlanToProcessConverter;
import org.teiid.dqp.internal.process.validator.AuthorizationValidationVisitor;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerIDFactory;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.service.TransactionContext;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.service.TransactionContext.Scope;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.eval.SecurityFunctionEvaluator;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.parser.ParseInfo;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
import com.metamatrix.query.processor.TempTableDataManager;
import com.metamatrix.query.processor.xml.XMLPlan;
import com.metamatrix.query.processor.xml.XMLPostProcessor;
import com.metamatrix.query.processor.xquery.XQueryPlan;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Limit;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.XQuery;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.tempdata.TempTableStore;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ContextProperties;
import com.metamatrix.query.validator.AbstractValidationVisitor;
import com.metamatrix.query.validator.ValidationVisitor;
import com.metamatrix.query.validator.Validator;
import com.metamatrix.query.validator.ValidatorFailure;
import com.metamatrix.query.validator.ValidatorReport;

/**
 * Server side representation of the RequestMessage.  Knows how to process itself.
 */
public class Request implements QueryProcessor.ProcessorFactory {
    
	// init state
    protected RequestMessage requestMsg;
    private String vdbName;
    private int vdbVersion;
    private BufferManager bufferManager;
    private ProcessorDataManager processorDataManager;
    private TransactionService transactionService;
    private TempTableStore tempTableStore;
    protected IDGenerator idGenerator = new IDGenerator();
    private boolean procDebugAllowed = false;
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
    private int chunkSize;
    
    protected Command userCommand;
    protected boolean returnsUpdateCount;
    protected boolean useEntitlements;

    void initialize(RequestMessage requestMsg,
                              BufferManager bufferManager,
                              ProcessorDataManager processorDataManager,
                              TransactionService transactionService,
                              boolean procDebugAllowed,
                              TempTableStore tempTableStore,
                              DQPWorkContext workContext,
                              int chunckSize,
                              ConnectorManagerRepository repo,
                              boolean useEntitlements) {

        this.requestMsg = requestMsg;
        this.vdbName = workContext.getVdbName();        
        this.vdbVersion = workContext.getVdbVersion();
        this.bufferManager = bufferManager;
        this.processorDataManager = processorDataManager;
        this.transactionService = transactionService;
        this.procDebugAllowed = procDebugAllowed;
        this.tempTableStore = tempTableStore;
        idGenerator.setDefaultFactory(new IntegerIDFactory());
        this.workContext = workContext;
        this.requestId = workContext.getRequestID(this.requestMsg.getExecutionId());
        this.chunkSize = chunckSize;
        this.connectorManagerRepo = repo;
        this.useEntitlements = useEntitlements;
    }
    
	void setMetadata(CapabilitiesFinder capabilitiesFinder, QueryMetadataInterface metadata, Set multiSourceModels) {
		this.capabilitiesFinder = capabilitiesFinder;
		this.metadata = metadata;
		this.multiSourceModels = multiSourceModels;
	}
    
	/**
	 * if the metadata has not been supplied via setMetadata, this method will create the appropriate state
	 * 
	 * @throws MetaMatrixComponentException
	 */
    protected void initMetadata() throws MetaMatrixComponentException {
        if (this.metadata != null) {
        	return;
        }
    	// Prepare dependencies for running the optimizer        
        this.capabilitiesFinder = new CachedFinder(this.connectorManagerRepo, workContext.getVDB());        
        

        metadata = workContext.getVDB().getAttachment(QueryMetadataInterface.class);

        if (metadata == null) {
            throw new MetaMatrixComponentException(DQPPlugin.Util.getString("DQPCore.Unable_to_load_metadata_for_VDB_name__{0},_version__{1}", this.vdbName, this.vdbVersion)); //$NON-NLS-1$
        }
        
        this.metadata = new TempMetadataAdapter(metadata, new TempMetadataStore());
    
        // Check for multi-source models and further wrap the metadata interface
        Set<String> multiSourceModelList = workContext.getVDB().getMultiSourceModelNames();
        if(multiSourceModelList != null && multiSourceModelList.size() > 0) {
        	this.multiSourceModels = multiSourceModelList;
            this.metadata = new MultiSourceMetadataWrapper(this.metadata, this.multiSourceModels);
        }
    }
    
    protected void createCommandContext() throws QueryValidatorException {
    	boolean returnsResultSet = false;
    	this.returnsUpdateCount = true;
        if(userCommand instanceof Query) {
        	Query query = (Query)userCommand;
    		returnsResultSet = query.getInto() == null;
    		returnsUpdateCount = !returnsResultSet;
        } else if (userCommand instanceof SetQuery) {
        	returnsResultSet = true;
        	returnsUpdateCount = false;
        } else if (userCommand instanceof XQuery) {
        	returnsResultSet = true;
        	returnsUpdateCount = false;
        } else if (userCommand instanceof StoredProcedure) {
        	returnsUpdateCount = false;
        	StoredProcedure proc = (StoredProcedure)userCommand;
        	returnsResultSet = proc.returnsResultSet();
        }
    	if ((this.requestMsg.getResultsMode() == ResultsMode.UPDATECOUNT && !returnsUpdateCount) 
    			|| (this.requestMsg.getResultsMode() == ResultsMode.RESULTSET && !returnsResultSet)) {
        	throw new QueryValidatorException(DQPPlugin.Util.getString(this.requestMsg.getResultsMode()==ResultsMode.RESULTSET?"Request.no_result_set":"Request.result_set")); //$NON-NLS-1$ //$NON-NLS-2$
    	}

    	// Create command context, used in rewriting, planning, and processing
        // Identifies a "group" of requests on a per-connection basis to allow later
        // cleanup of all resources in the group on connection shutdown
        String groupName = workContext.getConnectionID();

        RequestID reqID = workContext.getRequestID(this.requestMsg.getExecutionId());
        
        Properties props = new Properties();
        props.setProperty(ContextProperties.SESSION_ID, workContext.getConnectionID());
        
        this.context =
            new CommandContext(
                reqID,
                groupName,
                workContext.getUserName(),
                requestMsg.getExecutionPayload(), 
                workContext.getVdbName(), 
                workContext.getVdbVersion(),
                props,
                useProcDebug(userCommand), 
                collectNodeStatistics(userCommand));
        this.context.setProcessorBatchSize(bufferManager.getProcessorBatchSize());
        this.context.setConnectorBatchSize(bufferManager.getConnectorBatchSize());
        this.context.setStreamingBatchSize(chunkSize);
        
        if (multiSourceModels != null) {
            MultiSourcePlanToProcessConverter modifier = new MultiSourcePlanToProcessConverter(
					metadata, idGenerator, analysisRecord, capabilitiesFinder,
					multiSourceModels, workContext, context);
            context.setPlanToProcessConverter(modifier);
        }

        context.setSecurityFunctionEvaluator(new SecurityFunctionEvaluator() {
			@Override
			public boolean hasRole(String roleType, String roleName) throws MetaMatrixComponentException {
				if (isEntitled() || !useEntitlements) {
					return true;
				}
		        if (!DATA_ROLE.equalsIgnoreCase(roleType)) {
		            return false;
		        }
				return workContext.getAllowedDataPolicies().containsKey(roleName);
			}
        });
        context.setTempTableStore(tempTableStore);
        context.setQueryProcessorFactory(this);
        context.setMetadata(this.metadata);
    }

    protected void checkReferences(List<Reference> references) throws QueryValidatorException {
    	referenceCheck(references);
    }
    
    static void referenceCheck(List<Reference> references) throws QueryValidatorException {
    	if (references != null && !references.isEmpty()) {
    		throw new QueryValidatorException(DQPPlugin.Util.getString("Request.Invalid_character_in_query")); //$NON-NLS-1$
    	}
    }

    protected void resolveCommand(Command command) throws QueryResolverException, MetaMatrixComponentException {
        if (this.tempTableStore != null) {
        	QueryResolver.setChildMetadata(command, tempTableStore.getMetadataStore().getData(), null);
        }
    	//ensure that the user command is distinct from the processing command
        //rewrite and planning may alter options, symbols, etc.
    	QueryResolver.resolveCommand(command, metadata, analysisRecord);
    	
    	this.userCommand = (Command)command.clone();
    }
        
    private void validateQuery(Command command)
        throws QueryValidatorException, MetaMatrixComponentException {
                
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
        throws QueryValidatorException, MetaMatrixComponentException {

        // Validate with visitor
        ValidatorReport report = Validator.validate(command, metadata, visitor);
        if (report.hasItems()) {
            ValidatorFailure firstFailure = (ValidatorFailure) report.getItems().iterator().next();
            throw new QueryValidatorException(firstFailure.getMessage());
        }
    }

    private void createProcessor() throws MetaMatrixComponentException {
        
        TransactionContext tc = transactionService.getOrCreateTransactionContext(workContext.getConnectionID());
        
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
                    throw new MetaMatrixComponentException(err);
                }
            }
        } 
        
        this.transactionContext = tc;
        this.processor = new QueryProcessor(processPlan, context, bufferManager, new TempTableDataManager(processorDataManager, tempTableStore));
    }

    private boolean useProcDebug(Command command) {
        if(this.procDebugAllowed) {
            Option option = command.getOption();
            if(option != null) {
                return option.getDebug();
            }
        }
        return false;
    }
    
    private boolean collectNodeStatistics(Command command) {
        if(this.requestMsg.getShowPlan()) {
            return true;
        }
        Option option = command.getOption();
        if(option != null) {
            return (option.getDebug() || option.getShowPlan());
        }
        return false;
    }

    /**
     * state side effects:
     *      creates the analysis record
     * 		creates the command context
     * 		sets the pre-rewrite command on the request
     * 		adds a limit clause if the row limit is specified
     * 		sets the processor plan
     * 
     * @throws MetaMatrixComponentException
     * @throws QueryPlannerException
     * @throws QueryParserException
     * @throws QueryResolverException
     * @throws QueryValidatorException
     */
    protected void generatePlan() throws MetaMatrixComponentException, QueryPlannerException, QueryParserException, QueryResolverException, QueryValidatorException {
        Command command = parseCommand();

        List<Reference> references = ReferenceCollectorVisitor.getReferences(command);
        
        //there should be no reference (?) for query/update executed as statement
        checkReferences(references);
        
        createAnalysisRecord(command);
                
        resolveCommand(command);
        
        createCommandContext();
        
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
                    LogManager.logInfo(LogConstants.CTX_DQP, debugLog);               
                }
            }
            LogManager.logDetail(LogConstants.CTX_DQP, new Object[] { DQPPlugin.Util.getString("BasicInterceptor.ProcessTree_for__4"), requestId, processPlan }); //$NON-NLS-1$
        } catch (QueryMetadataException e) {
            Object[] params = new Object[] { requestId};
            String msg = DQPPlugin.Util.getString("DQPCore.Unknown_query_metadata_exception_while_registering_query__{0}.", params); //$NON-NLS-1$
            throw new QueryPlannerException(e, msg);
        }
    }

    private void createAnalysisRecord(Command command) {
        Option option = command.getOption();
        boolean getPlan = requestMsg.getShowPlan();
        boolean debug = false;
        if (option != null) {
            getPlan = getPlan || option.getShowPlan() || option.getPlanOnly();          
            debug = option.getDebug();
        }
        
        this.analysisRecord = new AnalysisRecord(getPlan, getPlan, debug);
    }

    public void processRequest() 
        throws QueryValidatorException, QueryParserException, QueryResolverException, MetaMatrixComponentException, QueryPlannerException {
                    
    	LogManager.logDetail(LogConstants.CTX_DQP, this.requestId, "executing", this.requestMsg.isPreparedStatement()?"prepared":"", this.requestMsg.getCommandString()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	
        initMetadata();
        
        generatePlan();
        
        postProcessXML();
        
        validateAccess(userCommand);
        
        createProcessor();
    }

	private void postProcessXML() {
		boolean alreadyFormatted = false;
        if (requestMsg.getXMLFormat() != null) {
	        if(processPlan instanceof XQueryPlan) {
	            ((XQueryPlan)processPlan).setXMLFormat(requestMsg.getXMLFormat());
	            alreadyFormatted = true;
	        } else if (processPlan instanceof XMLPlan) {
	        	((XMLPlan)processPlan).setXMLFormat(requestMsg.getXMLFormat());
	        	alreadyFormatted = true;
	        }
        }
        boolean xml = alreadyFormatted 
        	|| (processPlan.getOutputElements().size() == 1 && DataTypeManager.DefaultDataClasses.XML.equals(((SingleElementSymbol)processPlan.getOutputElements().get(0)).getType()));
        
        if (xml && ((!alreadyFormatted && requestMsg.getXMLFormat() != null) || requestMsg.getStyleSheet() != null)) {
        	XMLPostProcessor postProcessor = new XMLPostProcessor(processPlan);
        	postProcessor.setStylesheet(requestMsg.getStyleSheet());
        	if (!alreadyFormatted) {
        		postProcessor.setXMLFormat(requestMsg.getXMLFormat());
        	}
        	this.processPlan = postProcessor;
        }
        this.context.setValidateXML(requestMsg.getValidationMode());
	}
    
	public QueryProcessor createQueryProcessor(String query, String recursionGroup, CommandContext commandContext) throws MetaMatrixProcessingException, MetaMatrixComponentException {
		boolean isRootXQuery = recursionGroup == null && commandContext.getCallStackDepth() == 0 && userCommand instanceof XQuery;
		
		ParseInfo parseInfo = new ParseInfo();
		if (isRootXQuery) {
			parseInfo.ansiQuotedIdentifiers = requestMsg.isAnsiQuotedIdentifiers();
		}
		Command newCommand = QueryParser.getQueryParser().parseCommand(query, parseInfo);
        QueryResolver.resolveCommand(newCommand, metadata);            
        
        List<Reference> references = ReferenceCollectorVisitor.getReferences(newCommand);
        
        referenceCheck(references);
        
        validateQuery(newCommand);
        
        if (isRootXQuery) {
        	validateAccess(newCommand);
        }
        
        CommandContext copy = (CommandContext) commandContext.clone();
        if (recursionGroup != null) {
        	copy.pushCall(recursionGroup);
        }
        
        newCommand = QueryRewriter.rewrite(newCommand, metadata, copy);
        ProcessorPlan plan = QueryOptimizer.optimizePlan(newCommand, metadata, idGenerator, capabilitiesFinder, analysisRecord, copy);
        return new QueryProcessor(plan, copy, bufferManager, processorDataManager);
	}

	protected void validateAccess(Command command) throws QueryValidatorException, MetaMatrixComponentException {
		AuthorizationValidationVisitor visitor = new AuthorizationValidationVisitor(this.workContext.getVDB(), !isEntitled() && this.useEntitlements, this.workContext.getAllowedDataPolicies(), this.workContext.getUserName());
		validateWithVisitor(visitor, this.metadata, command);
	}
	
    protected boolean isEntitled(){
        if (this.workContext.getSubject() == null) {
            LogManager.logDetail(com.metamatrix.common.log.LogConstants.CTX_AUTHORIZATION,new Object[]{ "Automatically entitling principal", this.workContext.getUserName()}); //$NON-NLS-1$ 
            return true;
        }
        return false;
    }	
}
