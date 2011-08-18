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

package org.teiid.query.processor.proc;

import java.util.ArrayList;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.proc.Statement.Labeled;
import org.teiid.query.tempdata.TempTableStore;


/**
 * A program is a sequence of {@link ProgramInstruction ProgramInstruction}.  Certain
 * ProgramInstructions, such as {@link IfInstruction} and {@link WhileInstruction} may 
 * have pointers to sub programs.
 */
public class Program implements Cloneable, Labeled {

    private List<ProgramInstruction> programInstructions;
    private int counter = 0;
    private boolean atomic;
    private String label;
    private TempTableStore tempTables;
    private boolean startedTxn;

	/**
	 * Constructor for Program.
	 */
	public Program(boolean atomic) {
		this.atomic = atomic;
	}
	
	public void setStartedTxn(boolean startedTxn) {
		this.startedTxn = startedTxn;
	}
	
	public boolean startedTxn() {
		return startedTxn;
	}
	
    @Override
    public String getLabel() {
    	return label;
    }
    
    @Override
    public void setLabel(String label) {
    	this.label = label;
    }
    
	public boolean isAtomic() {
		return atomic;
	}
	
	public TempTableStore getTempTableStore() {
		return tempTables;
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
    public void reset(String sessionId){
        counter = 0;
        this.tempTables = new TempTableStore(sessionId);
        this.startedTxn = false;
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

    /**
     * Produces a deep clone.
     */
    public Object clone(){
        Program program = new Program(atomic);
        program.counter = this.counter;
        
        if (this.programInstructions != null){
            ArrayList<ProgramInstruction> clonedInstructions = new ArrayList<ProgramInstruction>(this.programInstructions.size());
            for (ProgramInstruction pi : this.programInstructions) {
                clonedInstructions.add( pi.clone() );
            }
            program.programInstructions = clonedInstructions;
        }
        program.label = label;
        return program;
    }

    public PlanNode getDescriptionProperties() {
    	PlanNode props = new PlanNode("Program"); //$NON-NLS-1$
        
        if(this.programInstructions != null) {
        	for (int i = 0; i < programInstructions.size(); i++) {
                ProgramInstruction inst = programInstructions.get(i);
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
    private ProgramInstruction getInstructionAtIndex(int instructionIndex){
        if (programInstructions != null){
            if (instructionIndex < getProcessorInstructions().size()){ 
                return getProcessorInstructions().get(instructionIndex);
            }
        }
        return null;
    }

    public List<ProgramInstruction> getProcessorInstructions(){
        if (programInstructions == null){
            programInstructions = new ArrayList<ProgramInstruction>();
        }
        return programInstructions;
    }
    
    public String toString() {
        StringBuilder str = new StringBuilder();   
            
        programToString(str);
        
        return "PROGRAM counter " + this.counter + "\n" + str.toString(); //$NON-NLS-1$ //$NON-NLS-2$ 
    }

    /**
     * This method calls itself recursively if either a While or If instruction is encountered. 
     * The sub program(s) from those kinds of instructions are passed, recursively, into this
     * method.
     */
    private final void programToString(StringBuilder str) {

        int instructionIndex = 0;
        ProgramInstruction inst = getInstructionAt(instructionIndex);
    
        while(inst != null) {
            
            printLine(instructionIndex++, inst.toString(), str);

			if(instructionIndex > 1000) { 
			    printLine(instructionIndex, "[OUTPUT TRUNCATED...]", str); //$NON-NLS-1$
			    break;
			}

            inst = getInstructionAt(instructionIndex);
        }
    }

    private static final void printLine(int counter, String line, StringBuilder buffer) {
        // Pad counter with spaces
        String counterStr = "" + counter + ": "; //$NON-NLS-1$ //$NON-NLS-2$
        if(counter < 10) { 
            counterStr += " ";     //$NON-NLS-1$
        }
        if(counterStr.length() == 1) { 
            counterStr += "  "; //$NON-NLS-1$
        } else if(counterStr.length() == 2) { 
            counterStr += " ";     //$NON-NLS-1$
        } 
        
        buffer.append(counterStr + line + "\n"); //$NON-NLS-1$
    }
    
    void getChildPlans(List<ProcessorPlan> plans) {
    	for (ProgramInstruction instruction : programInstructions) {
			instruction.getChildPlans(plans);
		}
    }
        
}
