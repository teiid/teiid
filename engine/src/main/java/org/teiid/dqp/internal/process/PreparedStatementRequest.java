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

import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.PreparedBatchUpdate;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;

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
    protected void checkReferences(List references)
    		throws QueryValidatorException {
    	//do nothing - references are allowed
    }
    
    /** 
     * @see org.teiid.dqp.internal.process.Request#resolveCommand(com.metamatrix.query.sql.lang.Command)
     */
    protected void resolveCommand(Command command, List references) throws QueryResolverException,
                                                  MetaMatrixComponentException {
    	
    	handleCallableStatement(command, references);
    	
    	super.resolveCommand(command, references);

    	if(requestMsg.isPreparedBatchUpdate()){
        	((PreparedBatchUpdate)command).setParameterReferences(references);
        }
    	//save the command in it's present form so that it can be validated later
        prepPlan.setCommand((Command) command.clone());
        prepPlan.setReferences(references);
    }

    /**
     * TODO: this is a hack that maintains pre 5.6 behavior, which ignores output parameters for resolving
     * @param command
     * @param references
     */
	private void handleCallableStatement(Command command, List references) {
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
				references.remove(param.getExpression());
				continue;
			}
			param.setIndex(index++);
			proc.setParameter(param);					
		}
	}

    protected void resolveParameterValues() throws QueryResolverException, MetaMatrixComponentException {
        List params = prepPlan.getReferences();
        List values = requestMsg.getParameterValues();
        if(requestMsg.isPreparedBatchUpdate()){
        	if(values.size() > 1){
        		((PreparedBatchUpdate)userCommand).setUpdatingModelCount(2);
        	}
        	for(int i=0; i<values.size(); i++){
        	   if (params.size() != ((List)values.get(i)).size()) {
        		   String msg = DQPPlugin.Util.getString("DQPCore.wrong_number_of_values", new Object[] {new Integer(values.size()), new Integer(params.size())}); //$NON-NLS-1$
        		   throw new QueryResolverException(msg);
        	   }
        	}
        } else {
        	PreparedStatementRequest.resolveParameterValues(params, values);
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
    protected void generatePlan() throws QueryPlannerException, QueryParserException, QueryResolverException, QueryValidatorException, MetaMatrixComponentException {
    	
    	String sqlQuery = requestMsg.getCommands()[0];
        prepPlan = prepPlanCache.getPreparedPlan(this.workContext.getConnectionID(), sqlQuery, requestMsg.isPreparedBatchUpdate());
        if (prepPlan == null) {
            //if prepared plan does not exist, create one
            prepPlan = prepPlanCache.createPreparedPlan(this.workContext.getConnectionID(), sqlQuery, requestMsg.isPreparedBatchUpdate());
            LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Query does not exist in cache: ", sqlQuery}); //$NON-NLS-1$
        }

        ProcessorPlan cachedPlan = prepPlan.getPlan();
        Command command = prepPlan.getCommand();
        
        if (cachedPlan == null) {
        	super.generatePlan();
        	
        	if (!this.addedLimit) { //TODO: this is a little problematic 
		        // Defect 13751: Clone the plan in its current state (i.e. before processing) so that it can be used for later queries
		        prepPlan.setPlan((ProcessorPlan)processPlan.clone());
		        prepPlan.setAnalysisRecord(analysisRecord);
        	}
        	command = prepPlan.getCommand();
        } else {
        	LogManager.logTrace(LogConstants.CTX_DQP, new Object[] { "Query exist in cache: ", sqlQuery }); //$NON-NLS-1$
            processPlan = (ProcessorPlan)cachedPlan.clone();
            //already in cache. obtain the values from cache
            analysisRecord = prepPlan.getAnalysisRecord();
            
            this.userCommand = command;
            createCommandContext(command);
        }
        
        // validate parameters values - right number and right type
        resolveParameterValues();
        // call back to Request.validateQueryValues to ensure that bound references are valid
        super.validateQueryValues(command);
    }

	/** 
	 * @param params
	 * @param values
	 * @throws QueryResolverException
	 */
	public static void resolveParameterValues(List params,
	                                    List values) throws QueryResolverException, MetaMatrixComponentException {
	    //the size of the values must be the same as that of the parameters
	    if (params.size() != values.size()) {
	        String msg = QueryPlugin.Util.getString("QueryUtil.wrong_number_of_values", new Object[] {new Integer(values.size()), new Integer(params.size())}); //$NON-NLS-1$
	        throw new QueryResolverException(msg);
	    }
	
	    if (params.isEmpty()) {
	        return;
	    }
	
	    //the type must be the same, or the type of the value can be implicitly converted
	    //to that of the reference
	    for (int i = 0; i < params.size(); i++) {
	        Reference param = (Reference) params.get(i);
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
	        
	        // Create with expected type if null
	        Constant constant = new Constant(value, param.getType());
	        
	        //bind variable
	        param.setExpression(constant);
	    }
	}
} 
