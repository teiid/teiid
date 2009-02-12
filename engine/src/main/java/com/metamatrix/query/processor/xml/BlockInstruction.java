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

/**
 */
public class BlockInstruction extends ProcessorInstruction {

    private String resultSetName;

    /**
     * Constructor for MoveCursorInstruction.
     */
    public BlockInstruction(String resultSetName) {
        this.resultSetName = resultSetName;
    }

    /**
     * @see ProcessorInstruction#process(ProcessorEnvironment)
     * @throws MetaMatrixProcessingException if row limit is reached and exception should
     * be thrown
     */
    public XMLContext process(XMLProcessorEnvironment env, XMLContext context)
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        context = new XMLContext(context);

        env.incrementCurrentProgramCounter();
        return context;
    }

    public String toString() {
        return "BLOCK " + resultSetName; //$NON-NLS-1$
    }

    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "BLOCK"); //$NON-NLS-1$

        props.put(PROP_RESULT_SET, this.resultSetName);
                
        return props;
    }

    /** 
     * @return the rsName
     */
    String getResultSetName() {
        return this.resultSetName;
    }
    
    /** 
     * @param rsName the rsName to set
     */
    void setResultSetName(String rsName) {
        this.resultSetName = rsName;
    }
}
