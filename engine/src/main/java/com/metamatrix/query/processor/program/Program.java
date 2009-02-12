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

package com.metamatrix.query.processor.program;

import java.util.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.query.processor.Describable;

/**
 * A program is a sequence of {@link ProgramInstruction ProgramInstruction}.  Certain
 * ProgramInstructions, such as {@link IfInstruction} and {@link WhileInstruction} may 
 * have pointers to sub programs.
 */
public class Program implements Cloneable, Describable {

    private List programInstructions;
    private int counter = 0;

	/**
	 * Constructor for Program.
	 */
	public Program() {
		super();
	}

    /**
     * Returns the next instruction to be executed, or null if there are
     * none or no more instructions.
     * @return ProgramInstruction to be executed next, or null if there
     * are no more to execute (or if this Program is empty)
     */
    public ProgramInstruction getCurrentInstruction(){
        return getInstructionAtIndex(counter);
    }

    /**
     * Increments the program counter, so that the next call to
     * {@link #getCurrentInstruction} will return the following
     * instruction.  This method is intended to be used by a 
     * ProcessingInstruction itself, to control the flow of execution.
     */
    public void incrementProgramCounter(){
        counter++;
    }
    
    /**
     * Decrements the program counter, so that the next call to
     * {@link #getCurrentInstruction} will return the previous
     * instruction.  This method is intended to be used by a 
     * ProcessingInstruction itself, to control the flow of execution.
     */
    public void decrementProgramCounter(){
        counter--;
    }
    
    /**
     * Resets this program, so it can be run through again.
     */
    public void resetProgramCounter(){
        counter = 0;
    }

    int getProgramCounter(){
        return counter;
    }


    /**
     * Returns the instruction to be executed at the indicated index,
     * or null if there is no instruction at that index.
     * @return instruction to be executed at the indicated index,
     * or null if there is no instruction at that index.
     */
    public ProgramInstruction getInstructionAt(int instructionIndex){
        return getInstructionAtIndex(instructionIndex);
    }

    
    public void addInstruction(ProgramInstruction instruction){
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
        return ("PROGRAM size " + getProcessorInstructions().size() + ", counter " + this.counter); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Produces a deep clone.
     */
    public Object clone(){
        Program program = new Program();
        program.counter = this.counter;
        
        if (this.programInstructions != null){
            ArrayList clonedInstructions = new ArrayList(this.programInstructions.size());
            Iterator i = this.programInstructions.iterator();
            while (i.hasNext()){
                clonedInstructions.add( ((ProgramInstruction)i.next()).clone() );
            }
            program.programInstructions = clonedInstructions;
        }
        
        return program;
    }

    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "Program"); //$NON-NLS-1$
        
        if(this.programInstructions != null) {
            List children = new ArrayList();
            Iterator iter = this.programInstructions.iterator();
            while(iter.hasNext()) {
                ProgramInstruction inst = (ProgramInstruction) iter.next();
                Map childProps = inst.getDescriptionProperties();
                children.add(childProps);
            }
            props.put(PROP_CHILDREN, children);
        }
        return props;
    }
    
    /**
     * Finds all nested plans and returns them.
     * @return List of ProcessorPlan 
     * @since 4.2
     */
    public Collection getChildPlans() {
        List plans = new ArrayList();
        if (programInstructions != null) {
            for(int i=0; i<programInstructions.size(); i++) {
                ProgramInstruction inst = (ProgramInstruction) programInstructions.get(i);
                Collection instPlans = inst.getChildPlans();            
                if(instPlans != null) {
                    plans.addAll(instPlans);
                }            
            }
        }
        return plans;
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
    private ProgramInstruction getInstructionAtIndex(int instructionIndex){
        if (programInstructions != null){
            if (instructionIndex < getProcessorInstructions().size()){ 
                return (ProgramInstruction)getProcessorInstructions().get(instructionIndex);
            }
        }
        return null;
    }

    public List getProcessorInstructions(){
        if (programInstructions == null){
            programInstructions = new ArrayList();
        }
        return programInstructions;
    }

}
