/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.process;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.transaction.SystemException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.xa.TransactionContext;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerIDFactory;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.config.DQPProperties;
import com.metamatrix.dqp.internal.process.capabilities.ConnectorCapabilitiesFinder;
import com.metamatrix.dqp.internal.process.capabilities.SharedCachedFinder;
import com.metamatrix.dqp.internal.process.multisource.MultiSourceCapabilitiesFinder;
import com.metamatrix.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import com.metamatrix.dqp.internal.process.multisource.MultiSourcePlanToProcessConverter;
import com.metamatrix.dqp.internal.process.validator.AuthorizationValidationVisitor;
import com.metamatrix.dqp.internal.process.validator.ModelVisibilityValidationVisitor;
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
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.tempdata.TempTableStore;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ContextProperties;
import com.metamatrix.query.util.TypeRetrievalUtil;
import com.metamatrix.query.validator.AbstractValidationVisitor;
import com.metamatrix.query.validator.ValidateCriteriaVisitor;
import com.metamatrix.query.validator.ValidationVisitor;
import com.metamatrix.query.validator.Validator;
import com.metamatrix.query.validator.ValidatorFailure;
import com.metamatrix.query.validator.ValidatorReport;
import com.metamatrix.query.validator.ValueValidationVisitor;

/**
 * Server side representation of the RequestMessage.  Knows how to process itself.
 */
public class Request {
    
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
    private IDGenerator idGenerator = new IDGenerator();
    private boolean procDebugAllowed = false;
    private Map connectorCapabilitiesCache;
    DQPWorkContext workContext;
    RequestID requestId;

    // acquired state
    private CapabilitiesFinder capabilitiesFinder;
    private QueryMetadataInterface metadata;
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

    void initialize(RequestMessage requestMsg,
                              ApplicationEnvironment env,
                              BufferManager bufferManager,
                              ProcessorDataManager processorDataManager,
                              Map connectorCapabilitiesCache,
                              TransactionService transactionService,
                              boolean procDebugAllowed,
                              TempTableStore tempTableStore,
                              DQPWorkContext workContext) {

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
            Object[] params = new Object[] { this.vdbName, this.vdbVersion };
            String msg = DQPPlugin.Util.getString("DQPCore.Unable_to_load_metadata_for_VDB_name__{0},_version__{1}", params); //$NON-NLS-1$
            MetaMatrixComponentException e = new MetaMatrixComponentException(msg);
            throw e;
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
    
    protected void createCommandContext(Command command) {
    	// Create command context, used in rewriting, planning, and processing
        // Identifies a "group" of requests on a per-connection basis to allow later
        // cleanup of all resources in the group on connection shutdown
        String groupName = workContext.getConnectionID();

        RequestID reqID = workContext.getRequestID(this.requestMsg.getExecutionId());
        
        Properties envProps = env.getApplicationProperties();
        Properties props = new Properties();
        props.setProperty(ContextProperties.SESSION_ID, workContext.getConnectionID());
        
        this.context =
            new CommandContext(
                reqID,
                groupName,
                null,
                requestMsg.getFetchSize(), 
                workContext.getUserName(), 
                workContext.getTrustedPayload(),
                requestMsg.getExecutionPayload(),
                workContext.getVdbName(), 
                workContext.getVdbVersion(),
                props,
                useProcDebug(command), 
                collectNodeStatistics(command));
        this.context.setProcessorBatchSize(bufferManager.getProcessorBatchSize());
        this.context.setConnectorBatchSize(bufferManager.getConnectorBatchSize());
        
        String streamingBatchSize = null;
        if(envProps != null) {
            streamingBatchSize = envProps.getProperty(DQPProperties.STREAMING_BATCH_SIZE);
        }
        if(streamingBatchSize != null){
        	context.setStreamingBatchSize(Integer.parseInt(streamingBatchSize));
        }
        
        if (multiSourceModels != null) {
            MultiSourcePlanToProcessConverter modifier = new MultiSourcePlanToProcessConverter(
					metadata, idGenerator, analysisRecord, capabilitiesFinder,
					multiSourceModels, vdbName, vdbService, vdbVersion);
            context.setPlanToProcessConverter(modifier);
        }

        context.setSecurityFunctionEvaluator((SecurityFunctionEvaluator)this.env.findService(DQPServiceNames.AUTHORIZATION_SERVICE));
        if(requestMsg.isPreparedBatchUpdate()){
        	context.setPreparedBatchUpdateValues(requestMsg.getParameterValues());
        }
        context.setTempTableStore(tempTableStore);
    }

    /**
     * Side effects:
     * 		creates the analysis record
     * 		creates the command context
     * 		sets the pre-rewrite command on the request
     * 		adds a limit clause if the row limit is specified
     * 
     * @return the post rewrite query
     * @throws QueryParserException
     * @throws QueryResolverException
     * @throws QueryValidatorException
     * @throws MetaMatrixComponentException
     */
    protected Command prepareCommand() throws QueryParserException,
                                   QueryResolverException,
                                   QueryValidatorException,
                                   MetaMatrixComponentException {
        
        Command command = getCommand();

        List references = ReferenceCollectorVisitor.getReferences(command);
        
        //there should be no reference (?) for query/update executed as statement
        checkReferences(references);
        
        createAnalysisRecord(command);
                
        resolveCommand(command, references);
        
        createCommandContext(command);
        
        validateQuery(command);
        
        validateQueryValues(command);
        
        Command preRewrite = command;
        
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
        
        requestMsg.setCommand(preRewrite);
        return command;
    }
    
    protected void checkReferences(List references) throws QueryValidatorException {
    	if (references != null && !references.isEmpty()) {
    		throw new QueryValidatorException(DQPPlugin.Util.getString("Request.Invalid_character_in_query")); //$NON-NLS-1$
    	}
    }

    protected void resolveCommand(Command command, List references) throws QueryResolverException, MetaMatrixComponentException {
        if (this.tempTableStore != null) {
        	QueryResolver.setChildMetadata(command, tempTableStore.getMetadataStore().getData(), null);
        }
    	
    	QueryResolver.resolveCommand(command, metadata, analysisRecord);
    }
        
    private void validateQuery(Command command)
        throws QueryValidatorException, MetaMatrixComponentException {
                
        // Create generic sql validation visitor
        AbstractValidationVisitor visitor = new ValidationVisitor();
        validateWithVisitor(visitor, metadata, command, false);

        // Create criteria validation visitor
        visitor = new ValidateCriteriaVisitor();
        validateWithVisitor(visitor, metadata, command, true);
        
        // Create model visibility validation visitor
        visitor = new ModelVisibilityValidationVisitor(this.vdbService, this.vdbName, this.vdbVersion);
        validateWithVisitor(visitor, metadata, command, true);
    }
    
    protected void validateQueryValues(Command command) 
        throws QueryValidatorException, MetaMatrixComponentException {

        AbstractValidationVisitor visitor = new ValueValidationVisitor();
        validateWithVisitor(visitor, metadata, command, false);
    }

    private Command getCommand() throws QueryParserException {
        String command = requestMsg.getCommandStr();
        ParseInfo parseInfo = new ParseInfo();
        if (requestMsg.isDoubleQuotedVariableAllowed()) {
        	parseInfo.allowDoubleQuotedVariable = true;
        }
        if (command != null) {
            return QueryParser.getQueryParser().parseCommand(command, parseInfo);
        }
        String[] commands = requestMsg.getBatchedCommands();
        List parsedCommands = new ArrayList(commands.length);
        for (int i = 0; i < commands.length; i++) {
        	String updateCommand = commands[i];
            parsedCommands.add(QueryParser.getQueryParser().parseCommand(updateCommand, parseInfo));
        }
        return new BatchedUpdateCommand(parsedCommands);
    }

    private void validateWithVisitor(
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

    protected void createProcessor() throws MetaMatrixComponentException {
        
        TransactionContext tc = null;
        
        if (transactionService != null) {
            tc = transactionService.getTransactionServer().getOrCreateTransactionContext(workContext.getConnectionID());
        }
        
        if (tc != null){ 
            Assertion.assertTrue(tc.getTransactionType() != TransactionContext.TRANSACTION_REQUEST);
        }
        
        if (tc == null || !tc.isInTransaction()) {
            //if not under a transaction
            
            boolean startAutoWrapTxn = false;
            
            if(ExecutionProperties.AUTO_WRAP_ON.equals(requestMsg.getTxnAutoWrapMode())){ 
                startAutoWrapTxn = true;
            } else if ( ((Command)requestMsg.getCommand()).updatingModelCount(metadata) > 1) { 
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
                    tc = transactionService.getTransactionServer().start(tc);
                } catch (XATransactionException err) {
                    throw new MetaMatrixComponentException(err);
                } catch (SystemException err) {
                    throw new MetaMatrixComponentException(err);
                }
            }
        } 
        
        this.transactionContext = tc;
        this.context.setTupleSourceID(getTupleSourceID(this.workContext.getConnectionID()));
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

    private TupleSourceID getTupleSourceID(String groupName) throws MetaMatrixComponentException {
        List outputElements = processPlan.getOutputElements();
        return
            this.bufferManager.createTupleSource(
                outputElements,
                TypeRetrievalUtil.getTypeNames(outputElements),
                groupName,
                TupleSourceType.FINAL);
    }

    /**
     * side effects:
     * 		sets the processor plan
     * 
     * @throws MetaMatrixComponentException
     * @throws QueryPlannerException
     * @throws QueryParserException
     * @throws QueryResolverException
     * @throws QueryValidatorException
     */
    protected void generatePlan() throws MetaMatrixComponentException, QueryPlannerException, QueryParserException, QueryResolverException, QueryValidatorException {
    	Command command = prepareCommand();
        
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
                // If debug log exists, write it to System.out, which should end up in server log
                String debugLog = analysisRecord.getDebugLog();
                if(debugLog != null && debugLog.length() > 0) {
                    System.out.println(debugLog); //TODO: fix me               
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
                    
        initMetadata();
        
        generatePlan();
        
        Command command = (Command)requestMsg.getCommand();
        
        validateEntitlement(command);
        
        setSchemasForXMLPlan(command, metadata);
        
        createProcessor();
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
				if (command.getType() == Command.TYPE_XQUERY) {
					// validate its first level children
					Iterator iter = command.getSubCommands().iterator();
					while (iter.hasNext()) {
						validateWithVisitor(visitor, this.metadata,
								(Command) iter.next(), true);
					}
				} else {
					validateWithVisitor(visitor, this.metadata, command, true);
				}
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
