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

package org.teiid.query.processor.xml;

import static org.teiid.query.analysis.AnalysisRecord.*;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogManager;
import org.teiid.query.mapping.xml.ResultSetInfo;


/**
 * Executes a SQL statement, defines a result set.
 */
public class ExecSqlInstruction extends ProcessorInstruction {
    String resultSetName;
    ResultSetInfo info;
    
    public ExecSqlInstruction(String resultSetName, ResultSetInfo info) {
        this.resultSetName = resultSetName;
        this.info = info;
    }


    public XMLContext process(XMLProcessorEnvironment env, XMLContext context)
        throws BlockedException, TeiidComponentException, TeiidProcessingException{

        execute(env, context);
        
        env.incrementCurrentProgramCounter();
        return context;
    }

	protected void execute(XMLProcessorEnvironment env, XMLContext context)
			throws TeiidComponentException, BlockedException,
			TeiidProcessingException {
		LogManager.logTrace(org.teiid.logging.LogConstants.CTX_XML_PLAN, new Object[]{"SQL: Result set DOESN'T exist:",resultSetName}); //$NON-NLS-1$
        PlanExecutor executor = getPlanExecutor(env, context);
        
        // this execute can throw the blocked exception
        executor.execute(context.getReferenceValues(), false);
        
        // now that we done executing the plan; remove the plan from context
        context.removeResultExecutor(resultSetName);
        
        // save this executioner in the context, so that all the nodes
        // below can access the data.
        context.setResultSet(this.resultSetName, executor);
	}

	public PlanExecutor getPlanExecutor(XMLProcessorEnvironment env,
			XMLContext context) throws TeiidComponentException {
		PlanExecutor executor = context.getResultExecutor(resultSetName);
        if (executor == null) {
            executor = env.createResultExecutor(resultSetName, info);
            context.setResultExecutor(resultSetName, executor);
        }
		return executor;
	}
    
    public String toString() {
        return "SQL  " + resultSetName; //$NON-NLS-1$ 
    }

    public PlanNode getDescriptionProperties() {
    	PlanNode props = new PlanNode("EXECUTE SQL"); //$NON-NLS-1$
        props.addProperty(PROP_RESULT_SET, this.resultSetName);
    	props.addProperty(PROP_SQL, this.info.getPlan().getDescriptionProperties());
        return props;
    }
}
