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
import com.metamatrix.query.util.LogConstants;

/**
 * <p>This instruction closes a result set, which closes and removes the TupleSource
 * associated with it.  This is needed to explicitly close temporary group
 * result sets once processing moves out of their scope, otherwise they would not
 * get cleaned up.  But it can be used for other result sets besides temporary groups.
 * </p>
 * <p>(This instruction does the same thing that a
 * {@link WhileInstruction WhileInstruction} does when the end of a 
 * result set is reached.)</p>
 */
public class EndBlockInstruction extends ProcessorInstruction {

    private static final String CLOSE_FINISHED = "CLOSE finished result set:"; //$NON-NLS-1$

    private String resultSetName;

    /**
     * Constructor for CloseResultSetInstruction.
     */
    public EndBlockInstruction(String resultSetName) {
        super();
        this.resultSetName = resultSetName;
    }

    /**
     * @see ProcessorInstruction#process(ProcessorEnvironment)
     */
    public XMLContext process(XMLProcessorEnvironment env, XMLContext context)
    throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException{

        // now that we are done with while loop above this statement; bubble up the context
        context = context.getParentContext();

        context.removeResultSet(resultSetName);

        LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{CLOSE_FINISHED,resultSetName});
            
        env.incrementCurrentProgramCounter();
        return context;
    }

    public String toString() {
        return "CLOSE " + resultSetName; //$NON-NLS-1$
    }

    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "CLOSE RESULTSET"); //$NON-NLS-1$
        props.put(PROP_RESULT_SET, this.resultSetName);
        
        return props;
    }

    /** 
     * @return the resultSetName
     */
    String getResultSetName() {
        return this.resultSetName;
    }
    
    /** 
     * @param resultSetName the resultSetName to set
     */
    void setResultSetName(String resultSetName) {
        this.resultSetName = resultSetName;
    }
}
