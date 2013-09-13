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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
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
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.RelationalPlan;
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
    
    /** 
     * @throws TeiidComponentException 
     * @throws TeiidProcessingException 
     * @see org.teiid.dqp.internal.process.Request#generatePlan()
     */
	@Override
    protected void generatePlan(boolean addLimit) throws TeiidComponentException, TeiidProcessingException {
    	String sqlQuery = requestMsg.getCommands()[0];
    	CacheID id = new CacheID(this.workContext, Request.createParseInfo(this.requestMsg), sqlQuery);
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
            super.generatePlan(false);
        	prepPlan.setCommand(this.userCommand);
        	
        	//there's no need to cache the plan if it's a stored procedure, since we already do that in the optimizer
        	boolean cache = !(this.userCommand instanceof StoredProcedure);
        	
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
     * @param command
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
		if (this.processPlan instanceof RelationalPlan) {
			RelationalPlan rPlan = (RelationalPlan)this.processPlan;
			if (rPlan.getRootNode() instanceof AccessNode) {
				AccessNode aNode = (AccessNode)rPlan.getRootNode();
				String modelName = aNode.getModelName();
				command = aNode.getCommand();
		        SourceCapabilities caps = capabilitiesFinder.findCapabilities(modelName);
		        supportPreparedBatchUpdate = caps.supportsCapability(SourceCapabilities.Capability.BULK_UPDATE);
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
					multiValue.add(values.get(i));
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
	
	    //the type must be the same, or the type of the value can be implicitly converted
	    //to that of the reference
	    for (int i = 0; i < params.size(); i++) {
	        Reference param = params.get(i);
	        Object value = values.get(i);
	        
        	if(value != null) {
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
