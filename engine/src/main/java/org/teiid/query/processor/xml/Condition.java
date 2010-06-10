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

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;

/**
 * This is a condition which can be evaluated, and which supplies a 
 * Program instance which should act as the resulting sub Program to be run
 * if the condition evaluates to true.
 */
public abstract class Condition {

    private Program thenProgram;

    public Condition( Program thenProgram ) {
        this.thenProgram = thenProgram;
    }
    
    /**
     * If this Condition {@link #evaluate evaluates} to true, this Program should
     * be retrieved to be run immediately.
     * @return Program sub Program to be run if this Condition evaluates to true
     */
    public Program getThenProgram() {
        return thenProgram;
    }
    
    /**
     * Indicates if the then Program is recursive 
     * @return if the then Program is recursive 
     */
    public boolean isProgramRecursive() {
        return false;
    }
        
    /**
     * This method causes the Condition to evaluate itself.
     * @param elementMap Map of elements to their index in the currentRowData
     * @param currentRowData List of Objects representing the current row of 
     * the result set(s)
     * @param env XMLProcessorEnvironment of the XMLPlan, maintains state of the running
     * XML Processor Plan
     */
    public abstract boolean evaluate(XMLProcessorEnvironment env, XMLContext ontext)
     throws TeiidComponentException, TeiidProcessingException;
    
}
