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

import java.util.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.query.processor.Describable;

/**
 * A program is a sequence of {@link ProcessorInstruction ProcessorInstructions}.  Certain
 * ProcessorInstructions, such as {@link IfInstruction} and {@link WhileInstruction} may have
 * pointers to sub programs.  XMLPlan will maintain a stack of programs during
 * execution.
 */
public class Program implements Describable {

    private List processorInstructions;

	/**
	 * Constructor for Program.
	 */
	public Program() {
		super();
	}

    /**
     * Returns the instruction to be executed at the indicated index,
     * or null if there is no instruction at that index.
     * @return instruction to be executed at the indicated index,
     * or null if there is no instruction at that index.
     */
    public ProcessorInstruction getInstructionAt(int instructionIndex){
        return getInstructionAtIndex(instructionIndex);
    }
        
    /**
     * Removes the instruction to be executed at the indicated index,
     * or do nothing if there is no instruction at that index.
     * @param instructionIndex index of instruction to be removed
     */
    public void removeInstructionAt(int instructionIndex) {
        List instructions = getProcessorInstructions();
        if (instructionIndex < instructions.size()) {
            instructions.remove(instructionIndex);
        }
    }

    
    public void addInstruction(ProcessorInstruction instruction){
        if (instruction != null){
            getProcessorInstructions().add(instruction);
        }
    }

    public void addInstructions(Program instructions){
        if (instructions != null){
            getProcessorInstructions().addAll(instructions.getProcessorInstructions());
        }
    }

    public String toString(){
        return ("PROGRAM size " + getProcessorInstructions().size()); //$NON-NLS-1$ 
    }

    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "XML Program"); //$NON-NLS-1$
        
        if(this.processorInstructions != null) {
            List children = new ArrayList();
            Iterator iter = this.processorInstructions.iterator();
            while(iter.hasNext()) {
                ProcessorInstruction inst = (ProcessorInstruction) iter.next();
                Map childProps = inst.getDescriptionProperties();
                children.add(childProps);
            }
            props.put(PROP_CHILDREN, children);
        }
        return props;
    }

    //=========================================================================
    //UTILITY
    //=========================================================================

    /**
     * Returns the instruction to be executed at the indicated index,
     * or null if there is no instruction at that index.
     * @return instruction to be executed at the indicated index,
     * or null if there is no instruction at that index.
     */
    private ProcessorInstruction getInstructionAtIndex(int instructionIndex){
        if (processorInstructions != null){
            if (instructionIndex < getProcessorInstructions().size()){ 
                return (ProcessorInstruction)getProcessorInstructions().get(instructionIndex);
            }
        }
        return null;
    }

    List getProcessorInstructions(){
        if (processorInstructions == null){
            processorInstructions = new ArrayList();
        }
        return processorInstructions;
    }

}

