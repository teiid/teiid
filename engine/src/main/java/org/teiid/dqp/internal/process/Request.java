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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.transaction.SystemException;

import org.teiid.connector.xa.api.TransactionContext;
import org.teiid.dqp.internal.process.capabilities.ConnectorCapabilitiesFinder;
import org.teiid.dqp.internal.process.capabilities.SharedCachedFinder;
import org.teiid.dqp.internal.process.multisource.MultiSourceCapabilitiesFinder;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.dqp.internal.process.multisource.MultiSourcePlanToProcessConverter;
import org.teiid.dqp.internal.process.validator.AuthorizationValidationVisitor;
import org.teiid.dqp.internal.process.validator.ModelVisibilityValidationVisitor;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerIDFactory;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.AuthorizationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.jdbc.api.ExecutionProperties;
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
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.tempdata.TempTableStore;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ContextProperties;
import com.metamatrix.query.validator.AbstractValidationVisitor;
import com.metamatrix.query.validator.ValidationVisitor;
import com.metamatrix.query.validator.Validator;
import com.metamatrix.query.validator.ValidatorFailure;
import com.metamatrix.query.validator.ValidatorReport;
import com.metamatrix.query.validator.ValueValidationVisitor;

/**
 * Server side representation of the RequestMessage.  Knows how to process itself.
 */
public class Request implements QueryProcessor.ProcessorFactory {
    
	// init state
    protected RequestMessage requestMsg;
    private String vdbName;
    private String vdbVersion;
    private ApplicationEnvironment env;
    private VDBService vdbService;
    private BufferManager bufferManager;
    private ProcessorDataManager processorDataManager;
    private TransactionService transactionService;
    private TempTableStore tempTableStore;
    protected IDGenerator idGenerator = new IDGenerator();
    private boolean procDebugAllowed = false;
    private Map connectorCapabilitiesCache;
    DQPWorkContext workContext;
    RequestID requestId;

    // acquired state
    protected CapabilitiesFinder capabilitiesFinder;
    protected QueryMetadataInterface metadata;
    private Set multiSourceModels;

    // internal results
    protected boolean addedLimit;
    protected ProcessorPlan processPlan;
    // external results
    protected AnalysisRecord analysisRecord;
    protected CommandContext context;
    protected QueryProcessor processor;
    protected List schemas;
    
    protected TransactionContext transactionContext;
    
    private int chunkSize;
    
    protected Command userCommand;
    protected boolean returnsUpdateCount;

    void initialize(RequestMessage requestMsg,
                              ApplicationEnvironment env,
                              BufferManager bufferManager,
                              ProcessorDataManager processorDataManager,
                              Map connectorCapabilitiesCache,
                              TransactionService transactionService,
                              boolean procDebugAllowed,
                              TempTableStore tempTableStore,
                              DQPWorkContext workContext,
                              int chunckSize) {

        this.requestMsg = requestMsg;
        this.vdbName = workContext.getVdbName();        
        this.vdbVersion = workContext.getVdbVersion();
        this.env = env;
        this.vdbService = (VDBService) env.findService(DQPServiceNames.VDB_SERVICE);
        this.bufferManager = bufferManager;
        this.processorDataManager = processorDataManager;
        this.transactionService = transactionService;
        this.procDebugAllowed = procDebugAllowed;
        this.tempTableStore = tempTableStore;
        this.connectorCapabilitiesCache = connectorCapabilitiesCache;
        idGenerator.setDefaultFactory(new IntegerIDFactory());
        this.workContext = workContext;
        this.requestId = workContext.getRequestID(this.requestMsg.getExecutionId());
        this.chunkSize = chunckSize;
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
        CapabilitiesFinder baseFinder =
            new ConnectorCapabilitiesFinder(
                this.vdbService,
                (DataService) this.env.findService(DQPServiceNames.DATA_SERVICE),
                requestMsg, workContext);        
        
        // Wrap the finder with a cache
        this.capabilitiesFinder = new SharedCachedFinder(baseFinder, connectorCapabilitiesCache);
    	
        MetadataService metadataService = (MetadataService) this.env.findService(DQPServiceNames.METADATA_SERVICE);
        if(metadataService == null){
        	//should not come here. Per defect 15087, 
        	//in a rare situation it might be null
        	//try to get it again
        	metadataService = (MetadataService) this.env.findService(DQPServiceNames.METADATA_SERVICE);
        	if(metadataService == null){
	        	String msg = DQPPlugin.Util.getString("Request.MetadataServiceIsNull"); //$NON-NLS-1$
                throw new MetaMatrixComponentException(msg);
        	}
        }
        metadata = metadataService.lookupMetadata(vdbName, vdbVersion);            

        if (metadata == null) {
            throw new MetaMatrixComponentException(DQPPlugin.Util.getString("DQPCore.Unable_to_load_metadata_for_VDB_name__{0},_version__{1}", this.vdbName, this.vdbVersion)); //$NON-NLS-1$
        }
        
        this.metadata = new TempMetadataAdapter(metadata, new TempMetadataStore());
    
        //wrap metadata in the wrapper that knows VDBService
        this.metadata = new QueryMetadataWrapper(this.metadata, this.vdbName, this.vdbVersion, this.vdbService);
        
        // Check for multi-source models and further wrap the metadata interface
        List multiSourceModelList = vdbService.getMultiSourceModels(this.vdbName, this.vdbVersion);
        if(multiSourceModelList != null && multiSourceModelList.size() > 0) {
        	this.multiSourceModels = new HashSet(multiSourceModelList);
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
    	if (this.requestMsg.getRequireResultSet() != null && this.requestMsg.getRequireResultSet() != returnsResultSet) {
        	throw new QueryValidatorException(DQPPlugin.Util.getString(this.requestMsg.getRequireResultSet()?"Request.no_result_set":"Request.result_set")); //$NON-NLS-1$ //$NON-NLS-2$
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
					multiSourceModels, vdbName, vdbService, vdbVersion);
            context.setPlanToProcessConverter(modifier);
        }

        context.setSecurityFunctionEvaluator((SecurityFunctionEvaluator)this.env.findService(DQPServiceNames.AUTHORIZATION_SERVICE));
        context.setTempTableStore(tempTableStore);
        context.setQueryProcessorFactory(this);
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
        
    private void validateQuery(Command command, boolean validateVisibility)
        throws QueryValidatorException, MetaMatrixComponentException {
                
        // Create generic sql validation visitor
        AbstractValidationVisitor visitor = new ValidationVisitor();
        validateWithVisitor(visitor, metadata, command, false);

        if (validateVisibility) {
	        // Create model visibility validation visitor
	        visitor = new ModelVisibilityValidationVisitor(this.vdbService, this.vdbName, this.vdbVersion);
	        validateWithVisitor(visitor, metadata, command, true);
        }
    }
    
    protected void validateQueryValues(Command command) 
        throws QueryValidatorException, MetaMatrixComponentException {

        AbstractValidationVisitor visitor = new ValueValidationVisitor();
        validateWithVisitor(visitor, metadata, command, false);
    }

    private Command parseCommand() throws QueryParserException {
        String[] commands = requestMsg.getCommands();
        ParseInfo parseInfo = createParseInfo(this.requestMsg);
        if (!requestMsg.isBatchedUpdate()) {
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
    	parseInfo.allowDoubleQuotedVariable = requestMsg.isDoubleQuotedVariableAllowed();
		return parseInfo;
	}

    public static void validateWithVisitor(
        AbstractValidationVisitor visitor,
        QueryMetadataInterface metadata,
        Command command,
        boolean validateOnlyEmbedded)
        throws QueryValidatorException, MetaMatrixComponentException {

        // Validate with visitor
        ValidatorReport report = Validator.validate(command, metadata, visitor, validateOnlyEmbedded);
        if (report.hasItems()) {
            ValidatorFailure firstFailure = (ValidatorFailure) report.getItems().iterator().next();
            throw new QueryValidatorException(firstFailure.getMessage());
        }
    }

    protected void createProcessor(Command processingCommand) throws MetaMatrixComponentException {
        
        TransactionContext tc = null;
        
        if (transactionService != null) {
            tc = transactionService.getOrCreateTransactionContext(workContext.getConnectionID());
        }
        
        if (tc != null){ 
            Assertion.assertTrue(tc.getTransactionType() != TransactionContext.Scope.REQUEST, "Transaction already associated with request."); //$NON-NLS-1$
        }
        
        if (tc == null || !tc.isInTransaction()) {
            //if not under a transaction
            
            boolean startAutoWrapTxn = false;
            
            if(ExecutionProperties.AUTO_WRAP_ON.equals(requestMsg.getTxnAutoWrapMode())){ 
                startAutoWrapTxn = true;
            } else if ( processingCommand.updatingModelCount(metadata) > 1) { 
                if (ExecutionProperties.AUTO_WRAP_OPTIMISTIC.equals(requestMsg.getTxnAutoWrapMode())){ 
                    String msg = DQPPlugin.Util.getString("Request.txn_needed_wrong_mode", requestId); //$NON-NLS-1$
                    throw new MetaMatrixComponentException(msg);
                } else if (ExecutionProperties.AUTO_WRAP_PESSIMISTIC.equals(requestMsg.getTxnAutoWrapMode())){
                    startAutoWrapTxn = true;
                } else if (ExecutionProperties.AUTO_WRAP_OFF.equals(requestMsg.getTxnAutoWrapMode())) {
                    LogManager.logDetail(LogConstants.CTX_DQP, DQPPlugin.Util.getString("Request.potentially_unsafe")); //$NON-NLS-1$ 
                }
            } 
            if(ExecutionProperties.AUTO_WRAP_OPTIMISTIC.equals(requestMsg.getTxnAutoWrapMode())){
                this.context.setOptimisticTransaction(true);
            }
            
            if (startAutoWrapTxn) {
                if (transactionService == null) {
                    throw new MetaMatrixComponentException(DQPPlugin.Util.getString("Request.transaction_not_supported")); //$NON-NLS-1$
                }
                try {
                    tc = transactionService.start(tc);
                } catch (XATransactionException err) {
                    throw new MetaMatrixComponentException(err);
                } catch (SystemException err) {
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
    protected Command generatePlan() throws MetaMatrixComponentException, QueryPlannerException, QueryParserException, QueryResolverException, QueryValidatorException {
        Command command = parseCommand();

        List<Reference> references = ReferenceCollectorVisitor.getReferences(command);
        
        //there should be no reference (?) for query/update executed as statement
        checkReferences(references);
        
        createAnalysisRecord(command);
                
        resolveCommand(command);
        
        createCommandContext();
        
        validateQuery(command, true);
        
        validateQueryValues(command);
        
        command = QueryRewriter.rewrite(command, null, metadata, context);
        
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
            
            // Run the optimizer
            try {
                processPlan = QueryOptimizer.optimizePlan(command, metadata, idGenerator, finder, analysisRecord, context);
            } finally {
                String debugLog = analysisRecord.getDebugLog();
                if(debugLog != null && debugLog.length() > 0) {
                    LogManager.logInfo(LogConstants.CTX_DQP, debugLog);               
                }
            }
            
            if (analysisRecord.recordQueryPlan()) {
                analysisRecord.setQueryPlan(processPlan.getDescriptionProperties());
            }
            
            LogManager.logDetail(LogConstants.CTX_DQP, new Object[] { DQPPlugin.Util.getString("BasicInterceptor.ProcessTree_for__4"), requestId, processPlan }); //$NON-NLS-1$
        } catch (QueryMetadataException e) {
            Object[] params = new Object[] { requestId};
            String msg = DQPPlugin.Util.getString("DQPCore.Unknown_query_metadata_exception_while_registering_query__{0}.", params); //$NON-NLS-1$
            throw new QueryPlannerException(e, msg);
        }
        return command;
    }

    private void setSchemasForXMLPlan(Command command, QueryMetadataInterface metadata)
        throws MetaMatrixComponentException, QueryMetadataException {
    	
    	XMLPlan xmlPlan = null;
        
        if(processPlan instanceof XMLPlan) {
            xmlPlan = (XMLPlan)processPlan;
        }else if(processPlan instanceof XQueryPlan) {
            ((XQueryPlan)processPlan).setXMLFormat(requestMsg.getXMLFormat());
        }else if(command instanceof StoredProcedure) {
            Collection childPlans = processPlan.getChildPlans();
            if(!childPlans.isEmpty()) {
                ProcessorPlan plan = (ProcessorPlan)childPlans.iterator().next();
                //check the last child plan of this procedure plan
                Collection procChildPlans = plan.getChildPlans();
                if(procChildPlans.size() > 0) {
                    Iterator iter = procChildPlans.iterator();
                    ProcessorPlan lastPlan = null;
                    while(iter.hasNext()) {
                        lastPlan = (ProcessorPlan) iter.next();
                    }
                    if(lastPlan instanceof XMLPlan) {
                        xmlPlan = (XMLPlan)lastPlan;
                    }
                }
            }
        }
        
        if (xmlPlan == null) {
        	return;
        }

        // Set the post-processing options on the plan
        boolean shouldValidate = requestMsg.getValidationMode();

        xmlPlan.setShouldValidate(shouldValidate);
        xmlPlan.setStylesheet(requestMsg.getStyleSheet());
        xmlPlan.setXMLFormat(requestMsg.getXMLFormat());

        // if the validation/schema mode is set to true look up the schema in runtime metadata
        if (shouldValidate) {
            this.schemas = metadata.getXMLSchemas(xmlPlan.getDocumentGroup().getMetadataID());
            // set the schema into the plan
            xmlPlan.setXMLSchemas(schemas);
        }
    }

    private void createAnalysisRecord(Command command) throws QueryValidatorException{
        Option option = command.getOption();
        boolean getPlan = requestMsg.getShowPlan();
        boolean debug = false;
        if (option != null) {
            getPlan = getPlan || option.getShowPlan() || option.getPlanOnly();          
            debug = option.getDebug();
        }
        
        if (getPlan && !requestMsg.isQueryPlanAllowed()){
          	final String message = DQPPlugin.Util.getString("Request.query_plan_not_allowed"); //$NON-NLS-1$
            throw new QueryValidatorException(message);
        }       
        
        this.analysisRecord = new AnalysisRecord(getPlan, getPlan, debug);
    }

    public void processRequest() 
        throws QueryValidatorException, QueryParserException, QueryResolverException, MetaMatrixComponentException, QueryPlannerException {
                    
    	LogManager.logDetail(LogConstants.CTX_DQP, this.requestId, "executing", this.requestMsg.isPreparedStatement()?"prepared":"", this.requestMsg.getCommandString()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	
        initMetadata();
        
        Command processingCommand = generatePlan();
        
        validateEntitlement(userCommand);
        
        setSchemasForXMLPlan(userCommand, metadata);
        
        createProcessor(processingCommand);
    }
    
	public QueryProcessor createQueryProcessor(String query, String recursionGroup, CommandContext commandContext) throws MetaMatrixProcessingException, MetaMatrixComponentException {
		boolean isRootXQuery = recursionGroup == null && commandContext.getCallStackDepth() == 0 && userCommand instanceof XQuery;
		
		ParseInfo parseInfo = new ParseInfo();
		if (isRootXQuery && requestMsg.isDoubleQuotedVariableAllowed()) {
			parseInfo.allowDoubleQuotedVariable = true;
		}
		Command newCommand = QueryParser.getQueryParser().parseCommand(query, parseInfo);
        QueryResolver.resolveCommand(newCommand, metadata);            
        
        List<Reference> references = ReferenceCollectorVisitor.getReferences(newCommand);
        
        referenceCheck(references);
        
        validateQuery(newCommand, isRootXQuery);
        
        validateQueryValues(newCommand);
        
        if (isRootXQuery) {
        	validateEntitlement(newCommand);
        }
        
        CommandContext copy = (CommandContext) commandContext.clone();
        if (recursionGroup != null) {
        	copy.pushCall(recursionGroup);
        }
        
        QueryRewriter.rewrite(newCommand, null, metadata, copy);
        ProcessorPlan plan = QueryOptimizer.optimizePlan(newCommand, metadata, idGenerator, capabilitiesFinder, analysisRecord, copy);
        return new QueryProcessor(plan, copy, bufferManager, processorDataManager);
	}

	protected void validateEntitlement(Command command)
			throws QueryValidatorException, MetaMatrixComponentException {
		// Validate the query (may only want to validate entitlement)
		AuthorizationService authSvc = (AuthorizationService) this.env
				.findService(DQPServiceNames.AUTHORIZATION_SERVICE);
		if (authSvc != null) {
			// See if entitlement checking is turned on
			if (authSvc.checkingEntitlements()) {
				AuthorizationValidationVisitor visitor = new AuthorizationValidationVisitor(
						this.workContext.getConnectionID(), authSvc);
				validateWithVisitor(visitor, this.metadata, command, true);
			} else if (workContext.getUserName().equals(
					AuthorizationService.DEFAULT_WSDL_USERNAME)) {
				       if (command.getType() == Command.TYPE_STORED_PROCEDURE &&
					       AuthorizationValidationVisitor.GET_UPDATED_CHARACTER_VDB_RESOURCE.contains(((StoredProcedure) command).getProcedureName())) {
				    	 // do nothing... this is valid
				    } else {
				    	// Throw an exception since the WSDL user is trying to do something other than access the VDB resources
					    final String message = DQPPlugin.Util.getString("Request.wsdl_user_not_authorized"); //$NON-NLS-1$
					    throw new QueryValidatorException(message);
				    }
			}
		}
	}
}
