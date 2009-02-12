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
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.util.LogConstants;

/**
 * Loop instruction on the result set execute before this instruction.
 */
public class WhileInstruction extends ProcessorInstruction {

    private String resultSetName;
    private Program blockProgram;

    /**
     * Constructor for WhileInstruction.
     */
    public WhileInstruction(String rsName) {
        this.resultSetName = rsName;
    }
    
    public void setBlockProgram(Program blockProgram) {
        this.blockProgram = blockProgram;
    }
    
    public Program getBlockProgram() { 
        return this.blockProgram;    
    }

    /**
     * @see ProcessorInstruction#process(ProcessorEnvironment)
     */
    public XMLContext process(XMLProcessorEnvironment env, XMLContext context)
    throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException{
        
        List row = context.getCurrentRow(resultSetName);
        
        if(row != null) {
            pushProgram(env, context, row);
            setFirst(context.getVariableContext(), Boolean.TRUE);
        } 
        else {
            LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"WHILE removed finished result set:",resultSetName}); //$NON-NLS-1$

            //increment the current program counter, so this 
            //While instruction will not be executed again
            env.incrementCurrentProgramCounter();
        }
        return context;
    }
    
    Map getPreviousValues(VariableContext varContext) {
        return (Map)varContext.getValue(new ElementSymbol("$" + getResultSetName() + "$previousValues")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    void setPreviousValues(VariableContext varContext, Map values) {
        varContext.setValue(new ElementSymbol("$" + getResultSetName() + "$previousValues"), values); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    Boolean getFirst(VariableContext varContext) {
        return (Boolean)varContext.getValue(new ElementSymbol("$" + getResultSetName() + "$first")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    void setFirst(VariableContext varContext, Boolean value) {
        varContext.setValue(new ElementSymbol("$" + getResultSetName() + "$first"), value); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    protected void pushProgram(XMLProcessorEnvironment env,
                             XMLContext context,
                             List row) throws MetaMatrixComponentException {
        LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"WHILE repeating for result set:",resultSetName,", block program:", blockProgram}); //$NON-NLS-1$ //$NON-NLS-2$

        context.setVariableValues(resultSetName, row);
        
        //push the block Program onto the stack
        env.pushProgram(blockProgram);
    }

    public String toString() {
        return "LOOP " + resultSetName; //$NON-NLS-1$
    }

    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "LOOP"); //$NON-NLS-1$ 
        props.put(PROP_RESULT_SET, this.resultSetName);           
        props.put(PROP_PROGRAM, this.blockProgram.getDescriptionProperties());           
        return props;
    }

    public String getResultSetName() {
        return this.resultSetName;
    }
}
