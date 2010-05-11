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

import java.util.List;
import java.util.Map;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogManager;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.util.VariableContext;


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
    throws BlockedException, TeiidComponentException, TeiidProcessingException{
        
        List row = context.getCurrentRow(resultSetName);
        
        if(row != null) {
            pushProgram(env, context, row);
            setFirst(context.getVariableContext(), Boolean.TRUE);
        } 
        else {
            LogManager.logTrace(org.teiid.logging.LogConstants.CTX_XML_PLAN, new Object[]{"WHILE removed finished result set:",resultSetName}); //$NON-NLS-1$

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
                             List row) throws TeiidComponentException {
        LogManager.logTrace(org.teiid.logging.LogConstants.CTX_XML_PLAN, new Object[]{"WHILE repeating for result set:",resultSetName,", block program:", blockProgram}); //$NON-NLS-1$ //$NON-NLS-2$

        context.setVariableValues(resultSetName, row);
        
        //push the block Program onto the stack
        env.pushProgram(blockProgram);
    }

    public String toString() {
        return "LOOP " + resultSetName; //$NON-NLS-1$
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = new PlanNode("LOOP"); //$NON-NLS-1$ 
        props.addProperty(PROP_RESULT_SET, this.resultSetName);           
        props.addProperty(PROP_PROGRAM, this.blockProgram.getDescriptionProperties());           
        return props;
    }

    public String getResultSetName() {
        return this.resultSetName;
    }
}
