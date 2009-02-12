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

import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.util.VariableContext;

/**
 * Loop instruction on the result set execute before this instruction.
 */
public class JoinedWhileInstruction extends WhileInstruction {

    private Integer mappingClassNumber;
    private ElementSymbol mappingClassSymbol;
    private String originalResultSet;

    /**
     * Constructor for WhileInstruction.
     */
    public JoinedWhileInstruction(String rsName,
                                  Integer mappingClassNumber,
                                  ElementSymbol mappingClassSymbol, String originalResultSet) {
        super(rsName);
        this.mappingClassNumber = mappingClassNumber;
        this.mappingClassSymbol = mappingClassSymbol;
        this.originalResultSet = originalResultSet;
    }

    /**
     * @see ProcessorInstruction#process(ProcessorEnvironment)
     */
    public XMLContext process(XMLProcessorEnvironment env,
                              XMLContext context) throws BlockedException,
                                                 MetaMatrixComponentException,
                                                 MetaMatrixProcessingException {

        List values = context.getCurrentRow(getResultSetName());

        if (values == null) {
            env.incrementCurrentProgramCounter();
            return context;
        }

        List outputElements = context.getOutputElements(getResultSetName());

        int index = outputElements.indexOf(mappingClassSymbol);

        Object value = values.get(index);

        VariableContext varContext = context.getVariableContext();

        // move on to the next row and don't push the program
        if (value == null) {
            if (Boolean.TRUE.equals(getFirst(varContext))) {
                context.getNextRow(getResultSetName());                
            }
            setFirst(varContext, Boolean.TRUE);
            env.incrementCurrentProgramCounter();
            return context;
        }

        boolean canConsume = true;

        // TODO: this is very inefficient. each root should check only its values and pass that value through the context
        // TODO: likewise the context update below should also only involve the columns from this context
        if (!Boolean.TRUE.equals(getFirst(varContext))) {
            Map previousValues = getPreviousValues(varContext); 

            if (previousValues != null) {
                for (int i = 0; i < index - 1 && canConsume; i++) {
                    Object previousValue = previousValues.get(outputElements.get(i));
                    Object currentValue = values.get(i);
                    if (previousValue != null) {
                        if (!previousValue.equals(currentValue)) {
                            canConsume = false;
                        }
                    } else if (currentValue != null) {
                        canConsume = false;
                    }
                }
            }
        }
        
        // consume this row only if the parent values are the same
        if (value.equals(mappingClassNumber) && canConsume) {
            //it is not necessary to push the values back into the context here
            setPreviousValues(varContext, context.getReferenceValues());
            
            pushProgram(env, context, values);
            
            setFirst(varContext, Boolean.FALSE);
        } else {
            setFirst(varContext, Boolean.TRUE);
            
            env.incrementCurrentProgramCounter();
            return context;
        }

        return context;
    }
    
    Boolean getFirst(VariableContext varContext) {
        return (Boolean)varContext.getValue(new ElementSymbol("$" + getResultSetName() + "$" + originalResultSet + "$first")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    void setFirst(VariableContext varContext, Boolean value) {
        varContext.setValue(new ElementSymbol("$" + getResultSetName()  + "$" + originalResultSet + "$first"), value); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public String toString() {
        return "JOINED " + originalResultSet + " " + super.toString(); //$NON-NLS-1$ //$NON-NLS-2$ 
    }

    public Map getDescriptionProperties() {
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "JOINED LOOP"); //$NON-NLS-1$ 
        return props;
    }
}
