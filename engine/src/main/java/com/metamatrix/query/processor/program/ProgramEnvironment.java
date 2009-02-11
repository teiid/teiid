/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.processor.program;

import java.util.Stack;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.query.processor.ProcessorPlan;

/**
 * This interface defines the environment that programs run in and what 
 * {@link ProgramInstruction}s can access during execution.
 */
public abstract class ProgramEnvironment {

    // Stack of programs, with current program on top
    private Stack programs = new Stack();
    
           
    /**
     * Default constructor
     */
    public ProgramEnvironment() {
    }
    
    /**
     * Connect an XMLPlan to the environment
     * @param plan XMLPlan
     */
    public abstract void initialize(ProcessorPlan plan);

    /**
     * Get the Stack of Program objects, with the currently running
     * Program on top.
     * @return Stack of currently running {@link Program Programs},
     * with the current Program on top.
     */
    public Stack getProgramStack() {
        return programs;
    }
    
    public Program peek() {
        return (Program)programs.peek();
    }
    
    public void pop() throws MetaMatrixComponentException {
        this.programs.pop();
    }
    
    public void push(Program program) {
        program.resetProgramCounter();
        this.programs.push(program);
    }
    
    public void incrementProgramCounter() throws MetaMatrixComponentException {
        peek().incrementProgramCounter();
    }
    
    /**
     * <p>Execute a ProcessorPlan and return the tupelSourceID for the results.
     * The TupleSourceID could be used by differrent {@link ProgramInstruction}s that
     * use this environment to access results. The 
     * @param command The command to execute which is a ProcessPlan
     * @param rsName The name of the result set, which can be used later to obtain the tuple source.
     */
	public abstract void executePlan(Object command, String rsName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException;
        
    /**
     * Remove the a data source from the environment.
     * @param rsName The name of the result set.
     * @throws MetaMatrixComponentException
     */
    public abstract void removeResults(String rsName) 
        throws MetaMatrixComponentException;
}
