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
        prepPlan.setReferences(references);
    }
    
    /** 
     * @see org.teiid.dqp.internal.process.Request#resolveCommand(org.teiid.query.sql.lang.Command)
     */
    @Override
    protected void resolveCommand(Command command) throws QueryResolverException,
                                                  TeiidComponentException {
    	handleCallableStatement(command);
    	
    	super.resolveCommand(command);
    }

    /**
     * TODO: this is a hack that maintains pre 5.6 behavior, which ignores output parameters for resolving
     * @param command
     * @param references
     */
	private void handleCallableStatement(Command command) {
		if (!this.requestMsg.isCallableStatement() || !(command instanceof StoredProcedure)) {
    		return;
    	}
		StoredProcedure proc = (StoredProcedure)command;
		if (!proc.isCallableStatement()) {
			return;
		}
		List<?> values = requestMsg.getParameterValues();
		List<SPParameter> spParams = proc.getParameters();
		proc.clearParameters();
		int inParameterCount = values.size();
		if (this.requestMsg.isBatchedUpdate() && values.size() > 0) {
			inParameterCount = ((List)values.get(0)).size();
		}
		int index = 1;
		for (Iterator<SPParameter> params = spParams.iterator(); params.hasNext();) {
			SPParameter param = params.next();
			if (param.getParameterType() == SPParameter.RETURN_VALUE) {
				inParameterCount++;
			} else if (param.getExpression() instanceof Reference && index > inParameterCount) {
				//assume it's an output parameter
				this.prepPlan.getReferences().remove(param.getExpression());
				continue;
			}
			param.setIndex(index++);
			proc.setParameter(param);					
		}
	}
    
    /** 
     * @throws TeiidComponentException 
     * @throws TeiidProcessingException 
     * @see org.teiid.dqp.internal.process.Request#generatePlan()
     */
    protected void generatePlan() throws TeiidComponentException, TeiidProcessingException {
    	String sqlQuery = requestMsg.getCommands()[0];
    	CacheID id = new CacheID(this.workContext, Request.createParseInfo(this.requestMsg), sqlQuery);
        prepPlan = prepPlanCache.get(id);
        
        if (prepPlan != null) {
        	ProcessorPlan cachedPlan = prepPlan.getPlan();
        	this.userCommand = prepPlan.getCommand();
        	if (validateAccess(requestMsg.getCommands(), userCommand, CommandType.PREPARED)) {
        		LogManager.logDetail(LogConstants.CTX_DQP, requestId, "AuthorizationValidator indicates that the prepared plan for command will not be used"); //$NON-NLS-1$
            	prepPlan = null;
            } else {
	        	LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Query exist in cache: ", sqlQuery }); //$NON-NLS-1$
	            processPlan = cachedPlan.clone();
	            //already in cache. obtain the values from cache
	            analysisRecord = prepPlan.getAnalysisRecord();
            }
        }
        
        if (prepPlan == null) {
            //if prepared plan does not exist, create one
            prepPlan = new PreparedPlan();
            LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Query does not exist in cache: ", sqlQuery}); //$NON-NLS-1$
            super.generatePlan();
	        if (!this.addedLimit) { //TODO: this is a little problematic
            	prepPlan.setCommand(this.userCommand);
		        // Defect 13751: Clone the plan in its current state (i.e. before processing) so that it can be used for later queries
		        prepPlan.setPlan(processPlan.clone(), this.context);
		        prepPlan.setAnalysisRecord(analysisRecord);
				
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
			throw new QueryValidatorException("No batch values sent for prepared batch update"); //$NON-NLS-1$
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
	        throw new QueryResolverException(msg);
	    }
	
	    //the type must be the same, or the type of the value can be implicitly converted
	    //to that of the reference
	    for (int i = 0; i < params.size(); i++) {
	        Reference param = params.get(i);
	        Object value = values.get(i);
	        
	        //TODO: why is the list check in here
        	if(value != null && !(value instanceof List)) {
                try {
                    String targetTypeName = DataTypeManager.getDataTypeName(param.getType());
                    Expression expr = ResolverUtil.convertExpression(new Constant(value), targetTypeName, metadata);
                    value = Evaluator.evaluate(expr);
				} catch (ExpressionEvaluationException e) {
                    String msg = QueryPlugin.Util.getString("QueryUtil.Error_executing_conversion_function_to_convert_value", new Integer(i + 1), value, DataTypeManager.getDataTypeName(param.getType())); //$NON-NLS-1$
                    throw new QueryResolverException(msg);
				} catch (QueryResolverException e) {
					String msg = QueryPlugin.Util.getString("QueryUtil.Error_executing_conversion_function_to_convert_value", new Integer(i + 1), value, DataTypeManager.getDataTypeName(param.getType())); //$NON-NLS-1$
                    throw new QueryResolverException(msg);
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
} 
