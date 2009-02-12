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

package com.metamatrix.query.processor.xml;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.query.mapping.xml.ResultSetInfo;
import com.metamatrix.query.util.LogConstants;

/**
 * Executes a SQL statement, defines a result set.
 */
public class ExecSqlInstruction extends ProcessorInstruction {
    private String resultSetName;
    ResultSetInfo info;
    
    public ExecSqlInstruction(String resultSetName, ResultSetInfo info) {
        this.resultSetName = resultSetName;
        this.info = info;
    }


    public XMLContext process(XMLProcessorEnvironment env, XMLContext context)
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException{

        LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"SQL: Result set DOESN'T exist:",resultSetName}); //$NON-NLS-1$
        PlanExecutor executor = context.getResultExecutor(resultSetName);
        if (executor == null) {
            executor = env.createResultExecutor(resultSetName, info);
            context.setResultExecutor(resultSetName, executor);
        }
        
        // this execute can throw the blocked exception
        executor.execute(context.getReferenceValues());
        
        // now that we done executing the plan; remove the plan from context
        context.removeResultExecutor(resultSetName);
        
        // save this executioner in the context, so that all the nodes
        // below can access the data.
        context.setResultSet(this.resultSetName, executor);
        
        env.incrementCurrentProgramCounter();
        return context;
    }
    
    public String toString() {
        return "SQL  " + resultSetName; //$NON-NLS-1$ 
    }

    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "EXECUTE SQL"); //$NON-NLS-1$
        props.put(PROP_SQL, this.resultSetName);
        props.put(PROP_RESULT_SET, this.resultSetName);
        props.put(PROP_IS_STAGING, "false"); //$NON-NLS-1$
                
        return props;
    }
}
