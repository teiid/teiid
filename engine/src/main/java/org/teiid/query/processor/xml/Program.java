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

import java.util.ArrayList;
import java.util.List;

import org.teiid.client.plan.PlanNode;

/**
 * A program is a sequence of {@link ProcessorInstruction ProcessorInstructions}.  Certain
 * ProcessorInstructions, such as {@link IfInstruction} and {@link WhileInstruction} may have
 * pointers to sub programs.  XMLPlan will maintain a stack of programs during
 * execution.
 */
public class Program {

    private List<ProcessorInstruction> processorInstructions;

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
        List<ProcessorInstruction> instructions = getProcessorInstructions();
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

    public PlanNode getDescriptionProperties() {
    	PlanNode props = new PlanNode("XML Program"); //$NON-NLS-1$
        
        if(this.processorInstructions != null) {
        	for (int i = 0; i < processorInstructions.size(); i++) {
                ProcessorInstruction inst = processorInstructions.get(i);
                PlanNode childProps = inst.getDescriptionProperties();
                props.addProperty("Instruction " + i, childProps); //$NON-NLS-1$
            }
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
                return getProcessorInstructions().get(instructionIndex);
            }
        }
        return null;
    }

    List<ProcessorInstruction> getProcessorInstructions(){
        if (processorInstructions == null){
            processorInstructions = new ArrayList<ProcessorInstruction>();
        }
        return processorInstructions;
    }

}

