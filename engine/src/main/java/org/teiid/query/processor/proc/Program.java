/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.processor.proc;

import java.util.ArrayList;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.query.sql.proc.Statement.Labeled;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;


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
    private String exceptionGroup;
    private Program exceptionProgram;
    private boolean trappingExceptions = false;

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
        this.tempTables = new TempTableStore(sessionId, TransactionMode.ISOLATE_WRITES);
        this.startedTxn = false;
        this.trappingExceptions = false;
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
    public Program clone(){
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
        program.exceptionGroup = this.exceptionGroup;
        if (this.exceptionProgram != null) {
            program.exceptionProgram = this.exceptionProgram.clone();
        }
        return program;
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = new PlanNode("Program"); //$NON-NLS-1$
        if (label != null) {
            props.addProperty("Label", label); //$NON-NLS-1$
        }
        if(this.programInstructions != null) {
            for (int i = 0; i < programInstructions.size(); i++) {
                ProgramInstruction inst = programInstructions.get(i);
                PlanNode childProps = inst.getDescriptionProperties();
                props.addProperty("Instruction " + i, childProps); //$NON-NLS-1$
            }
        }

        if (this.exceptionGroup != null) {
            props.addProperty("EXCEPTION GROUP", this.exceptionGroup); //$NON-NLS-1$
            if (this.exceptionProgram != null) {
                props.addProperty("EXCEPTION HANDLER", this.exceptionProgram.getDescriptionProperties()); //$NON-NLS-1$
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
        if (exceptionGroup != null) {
            str.append("\nEXCEPTION ").append(exceptionGroup).append("\n"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        if (exceptionProgram != null) {
            exceptionProgram.programToString(str);
        }
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

    public void setExceptionGroup(String exceptionGroup) {
        this.exceptionGroup = exceptionGroup;
    }

    public void setExceptionProgram(Program exceptionBlock) {
        this.exceptionProgram = exceptionBlock;
    }

    public String getExceptionGroup() {
        return exceptionGroup;
    }

    public Program getExceptionProgram() {
        return exceptionProgram;
    }

    public boolean isTrappingExceptions() {
        return trappingExceptions;
    }

    public void setTrappingExceptions(boolean trappingExceptions) {
        this.trappingExceptions = trappingExceptions;
    }

    public Boolean requiresTransaction(boolean transactionalReads) {
        return instructionsRequireTransaction(transactionalReads);
    }

    public Boolean instructionsRequireTransaction(boolean transactionalReads) {
        boolean possiblyRequired = false;
        boolean last = false;
        if (this.programInstructions == null) {
            return false;
        }
        for (ProgramInstruction instruction : this.programInstructions) {
            Boolean instructionRequires = instruction.requiresTransaction(transactionalReads);
            if (instructionRequires == null) {
                if (possiblyRequired) {
                    return true;
                }
                possiblyRequired = true;
                last = true;
                continue;
            }
            last = false;
            if (instructionRequires) {
                return true;
            }
        }
        if (possiblyRequired) {
            if (!last) {
                //we'd have to test more in depth about whether the later statements could fail
                return true;
            }
            return null;
        }
        return false;
    }

}
