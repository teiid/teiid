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

import org.teiid.dqp.internal.process.PreparedPlanCache.CacheID;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.optimizer.CommandTreeNode;
import com.metamatrix.query.optimizer.batch.BatchedUpdatePlanner;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.relational.RelationalPlanner;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.relational.AccessNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.util.CommandContext;

/**
 * Specific request for handling prepared statement calls.
 */
public class PreparedStatementRequest extends Request {
    private PreparedPlanCache prepPlanCache;
    private PreparedPlanCache.PreparedPlan prepPlan;
    
    public PreparedStatementRequest(PreparedPlanCache prepPlanCache) {
    	this.prepPlanCache = prepPlanCache;
    }
    
    @Override
    protected void checkReferences(List<Reference> references)
    		throws QueryValidatorException {
        prepPlan.setReferences(references);
    }
    
    /** 
     * @see org.teiid.dqp.internal.process.Request#resolveCommand(com.metamatrix.query.sql.lang.Command)
     */
    @Override
    protected void resolveCommand(Command command) throws QueryResolverException,
                                                  MetaMatrixComponentException {
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
		List values = requestMsg.getParameterValues();
		List spParams = proc.getParameters();
		proc.clearParameters();
		int inParameterCount = values.size();
		if (this.requestMsg.isPreparedBatchUpdate() && values.size() > 0) {
			inParameterCount = ((List)values.get(0)).size();
		}
		int index = 1;
		for (Iterator params = spParams.iterator(); params.hasNext();) {
			SPParameter param = (SPParameter) params.next();
			if (param.getParameterType() == SPParameter.RETURN_VALUE) {
				continue;
			}
			if (param.getExpression() instanceof Reference && index > inParameterCount) {
				//assume it's an output parameter
				this.prepPlan.getReferences().remove(param.getExpression());
				continue;
			}
			param.setIndex(index++);
			proc.setParameter(param);					
		}
	}
    
    @Override
    protected void validateQueryValues(Command command)
    		throws QueryValidatorException, MetaMatrixComponentException {
    	//do nothing initially - check after parameter values have been set
    }
    
    /** 
     * @throws MetaMatrixComponentException 
     * @throws QueryValidatorException 
     * @throws QueryResolverException 
     * @throws QueryParserException 
     * @throws QueryPlannerException 
     * @see org.teiid.dqp.internal.process.Request#generatePlan()
     */
    protected Command generatePlan() throws QueryPlannerException, QueryParserException, QueryResolverException, QueryValidatorException, MetaMatrixComponentException {
    	String sqlQuery = requestMsg.getCommands()[0];
    	CacheID id = new PreparedPlanCache.CacheID(this.workContext, Request.createParseInfo(this.requestMsg), sqlQuery);
        prepPlan = prepPlanCache.getPreparedPlan(id);
        if (prepPlan == null) {
            //if prepared plan does not exist, create one
            prepPlan = new PreparedPlanCache.PreparedPlan();
            LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Query does not exist in cache: ", sqlQuery}); //$NON-NLS-1$
        }

        ProcessorPlan cachedPlan = prepPlan.getPlan();
        
        if (cachedPlan == null) {
        	prepPlan.setRewritenCommand(super.generatePlan());
        	if (!this.addedLimit) { //TODO: this is a little problematic
            	prepPlan.setCommand(this.userCommand);
		        // Defect 13751: Clone the plan in its current state (i.e. before processing) so that it can be used for later queries
		        prepPlan.setPlan((ProcessorPlan)processPlan.clone());
		        prepPlan.setAnalysisRecord(analysisRecord);
		        this.prepPlanCache.putPreparedPlan(id, this.context.isSessionFunctionEvaluated(), prepPlan);
        	}
        } else {
        	LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Query exist in cache: ", sqlQuery }); //$NON-NLS-1$
            processPlan = (ProcessorPlan)cachedPlan.clone();
            //already in cache. obtain the values from cache
            analysisRecord = prepPlan.getAnalysisRecord();
            
            this.userCommand = prepPlan.getCommand();
            createCommandContext();
        }
                
        if (requestMsg.isPreparedBatchUpdate()) {
	        handlePreparedBatchUpdate();
        } else {
	        List<Reference> params = prepPlan.getReferences();
	        List<?> values = requestMsg.getParameterValues();
	
			resolveAndValidateParameters(this.userCommand, params, values);
        }
        return prepPlan.getRewritenCommand();
    }

    /**
     * There are two cases
     *   if 
     *     The source supports preparedBatchUpdate -> just let the command and values pass to the source
     *   else 
     *     create a batchedupdatecommand that represents the batch operation 
     * @param command
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     * @throws QueryResolverException
     * @throws QueryPlannerException 
     * @throws QueryValidatorException 
     */
	private void handlePreparedBatchUpdate() throws QueryMetadataException,
			MetaMatrixComponentException, QueryResolverException, QueryPlannerException, QueryValidatorException {
		List<List<?>> paramValues = requestMsg.getParameterValues();
		if (paramValues.isEmpty()) {
			throw new QueryValidatorException("No batch values sent for prepared batch update"); //$NON-NLS-1$
		}
		boolean supportPreparedBatchUpdate = false;
		if (this.processPlan instanceof RelationalPlan && this.prepPlan.getRewritenCommand().getSubCommands().isEmpty()) {
			RelationalPlan rPlan = (RelationalPlan)this.processPlan;
			if (rPlan.getRootNode() instanceof AccessNode) {
				AccessNode aNode = (AccessNode)rPlan.getRootNode();
				String modelName = aNode.getModelName();
		        SourceCapabilities caps = capabilitiesFinder.findCapabilities(modelName);
		        supportPreparedBatchUpdate = caps.supportsCapability(SourceCapabilities.Capability.BULK_UPDATE);
			}
		}
		CommandTreeNode ctn = new CommandTreeNode();
		List<Command> commands = new LinkedList<Command>();
		List<VariableContext> contexts = new LinkedList<VariableContext>();
		List<List<Object>> multiValues = new ArrayList<List<Object>>(this.prepPlan.getReferences().size());
		for (List<?> values : paramValues) {
			resolveAndValidateParameters(this.userCommand, this.prepPlan.getReferences(), values);
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
				continue; 
			}
			Command c = (Command)this.prepPlan.getRewritenCommand().clone();
			commands.add(c);
			CommandTreeNode child = new CommandTreeNode();
			child.setCommand(c);
			child.setProcessorPlan((ProcessorPlan)this.processPlan.clone());
			ctn.addLastChild(child);
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
		ctn.setCommand(buc);
		ctn.setProperty(RelationalPlanner.VARIABLE_CONTEXTS, contexts);
		BatchedUpdatePlanner planner = new BatchedUpdatePlanner();
		this.processPlan = planner.optimize(ctn, idGenerator, metadata, capabilitiesFinder, analysisRecord, context);
	}

	private void resolveAndValidateParameters(Command command, List<Reference> params,
			List<?> values) throws QueryResolverException,
			MetaMatrixComponentException, QueryValidatorException {
		// validate parameters values - right number and right type
    	PreparedStatementRequest.resolveParameterValues(params, values, this.context);
        // call back to Request.validateQueryValues to ensure that bound references are valid
        super.validateQueryValues(command);
	}

	/** 
	 * @param params
	 * @param values
	 * @throws QueryResolverException
	 */
	public static void resolveParameterValues(List<Reference> params,
	                                    List values, CommandContext context) throws QueryResolverException, MetaMatrixComponentException {
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
                    Expression expr = ResolverUtil.convertExpression(new Constant(value), targetTypeName);
                    value = Evaluator.evaluate(expr);
				} catch (ExpressionEvaluationException e) {
                    String msg = QueryPlugin.Util.getString("QueryUtil.Error_executing_conversion_function_to_convert_value", new Integer(i + 1), value, DataTypeManager.getDataTypeName(param.getType())); //$NON-NLS-1$
                    throw new QueryResolverException(msg);
				} catch (QueryResolverException e) {
					String msg = QueryPlugin.Util.getString("QueryUtil.Error_executing_conversion_function_to_convert_value", new Integer(i + 1), value, DataTypeManager.getDataTypeName(param.getType())); //$NON-NLS-1$
                    throw new QueryResolverException(msg);
				}
	        }
	        	        
	        //bind variable
	        result.setGlobalValue(param.getContextSymbol(), value);
	    }
	    
	    context.setVariableContext(result);
	}
} 
