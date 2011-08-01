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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.mapping.xml.ResultSetInfo;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;



public class XMLProcessorEnvironment {

    /** XML result documents should be in String form */
    public static final String STRING_RESULT = "String"; //$NON-NLS-1$

    /* Stack <ProgramState> */
    private LinkedList<ProgramState> programStack = new LinkedList<ProgramState>();
    
    private Set<String> loadedStagingTables = Collections.synchronizedSet(new HashSet<String>());

    private DocumentInProgress documentInProgress;
    
    private String xmlFormat;
    
    private GroupSymbol documentGroup;
    
    private ProcessorDataManager dataMgr;
    private BufferManager bufferMgr;
    private CommandContext commandContext;
    
    protected XMLProcessorEnvironment(){
    }
    
    public XMLProcessorEnvironment(Program mainProgram){
        pushProgram(mainProgram);
    }
    
    /**
     * @see ProcessorEnvironment#start()
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
        this.dataMgr = dataMgr;
        this.bufferMgr = bufferMgr;
        this.commandContext = context;
    }
                     
    /**
     * An object to hold state about a Program.  Programs are
     * immutable, therefore their program counter needs to be held
     * in this wrapper object.
     */
    private static class ProgramState {
        private Program program;
        private int programCounter = 0;
        private int lookaheadCounter;
        private int recursionCount = NOT_RECURSIVE;
        
        private static final int NOT_RECURSIVE = 0;
        
        public String toString() {
            return program.toString() + ", counter " + programCounter + ", recursionCount " + (recursionCount == NOT_RECURSIVE? "not recursive": "" + recursionCount); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }
    }
    
    public Program getCurrentProgram() {
        // jh case 5266
        if ( this.programStack.size() > 0 ) {
            ProgramState programState = this.programStack.getFirst();
            return programState.program;
        }
        return null;
    }
    
    /**
     * Indicates if there is a recursive program anywhere in the
     * current program stack (not just at the top). 
     * @return whether there is a recursive program anywhere in the
     * program stack
     */
    public boolean isRecursiveProgramInStack() {
        Iterator stackedPrograms = this.programStack.iterator();
        // Always at least one program in the stack
        ProgramState programState = (ProgramState)stackedPrograms.next();
        while (programState.recursionCount == ProgramState.NOT_RECURSIVE && stackedPrograms.hasNext()) {
            programState = (ProgramState) stackedPrograms.next();
        }
        return (programState.recursionCount > ProgramState.NOT_RECURSIVE);
    }

    public void incrementCurrentProgramCounter() {
        ProgramState programState = this.programStack.getFirst();
        programState.programCounter++;
        
        // Always leave one Program in the Program stack, even if it is finished
        while (this.programStack.size() > 1 &&
               programState.programCounter >= programState.program.getProcessorInstructions().size()) {
            this.programStack.removeFirst();
            if(LogManager.isMessageToBeRecorded(org.teiid.logging.LogConstants.CTX_XML_PLAN, MessageLevel.TRACE)) {
                LogManager.logTrace(org.teiid.logging.LogConstants.CTX_XML_PLAN, new Object[]{"Processor Environment popped program w/ recursion count " + programState.recursionCount, "; " + this.programStack.size(), " programs left."}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
            programState = this.programStack.getFirst();
        }        
    }
    
    public void pushProgram(Program program) {
        pushProgram(program, false);
    }
    
    public void pushProgram(Program program, boolean isRecursive) {
        ProgramState programState = new ProgramState();
        programState.program = program;
        if (isRecursive) {
            
            ProgramState previousState = getProgramState(program);
            if (previousState != null) {
                programState.recursionCount = previousState.recursionCount + 1;
            } else {
                programState.recursionCount = ProgramState.NOT_RECURSIVE + 1;
            }
            LogManager.logTrace(org.teiid.logging.LogConstants.CTX_XML_PLAN, new Object[]{"Pushed recursive program w/ recursion count " + programState.recursionCount}); //$NON-NLS-1$
            
        } else {
            LogManager.logTrace(org.teiid.logging.LogConstants.CTX_XML_PLAN, new Object[]{"Pushed non-recursive program w/ recursion count " + programState.recursionCount}); //$NON-NLS-1$
        }
        this.programStack.addFirst(programState);
    }

    public ProcessorInstruction getCurrentInstruction(XMLContext context) throws TeiidComponentException, TeiidProcessingException {
        ProgramState programState = this.programStack.getFirst();
        
        //Case 5266: account for an empty program on to the stack; 
        //this is needed to handle an empty sequence or an excluded Choice properly.
        if (programState != null && programState.program.getProcessorInstructions().isEmpty()) {
            incrementCurrentProgramCounter();
            
            programState = this.programStack.getFirst();
        }
        
        if (programState == null) {
            return null;
        }
        
        //start all siblings
        List<ProcessorInstruction> instrs = programState.program.getProcessorInstructions();
        if (programState.programCounter >= programState.lookaheadCounter && instrs.size() > programState.programCounter + 1) {
        	for (programState.lookaheadCounter = programState.programCounter; programState.lookaheadCounter < instrs.size(); programState.lookaheadCounter++) {
        		ProcessorInstruction pi = instrs.get(programState.lookaheadCounter);
        		boolean staging = false;
        		if (pi instanceof ExecStagingTableInstruction) {
        			staging = true;
        			ExecStagingTableInstruction esti = (ExecStagingTableInstruction)pi;
        			if (!esti.info.isAutoStaged()) {
        				//need to load staging tables prior to source queries
        				break;
        			}
        		}
        		if (pi instanceof ExecSqlInstruction) {
        			ExecSqlInstruction esi = (ExecSqlInstruction)pi;
        			if (!staging && esi.info.isAutoStaged() && esi.info.getTempTable() == null) {
        				continue; //derived load
        			}
        			PlanExecutor pe = esi.getPlanExecutor(this, context);
        			pe.execute(context.getReferenceValues(), true);
        		}
        	}
        }
        return programState.program.getInstructionAt(programState.programCounter);
    }
    
    public int getProgramRecursionCount(Program program){
        
        ProgramState programState = getProgramState(program);
        if (programState == null) {
            return ProgramState.NOT_RECURSIVE;
        }
        return programState.recursionCount;
    }
    
    private ProgramState getProgramState(Program program) {
        ProgramState result = null;
        Iterator<ProgramState> stackedPrograms = this.programStack.iterator();
        while (stackedPrograms.hasNext()) {
            ProgramState programState = stackedPrograms.next();
            Program stackedProgram = programState.program;
            if (stackedProgram == program) {
                result = programState;
                break;
            }
        }
            
        return result;
    }

    public PlanExecutor createResultExecutor(String resultSetName, ResultSetInfo info) 
        throws TeiidComponentException{
    
        // cloning the plan inside the resultset is not possible
        // because of the dependencies.
        ResultSetInfo clone = (ResultSetInfo)info.clone();
        ProcessorPlan plan = clone.getPlan();
        plan = plan.clone();
        clone.setPlan(plan);
        
        return new RelationalPlanExecutor(clone, this.commandContext, this.dataMgr, this.bufferMgr);
    }
    
    public DocumentInProgress getDocumentInProgress() {
        return this.documentInProgress;
    }

    public void setDocumentInProgress(DocumentInProgress documentInProgress) {
        this.documentInProgress = documentInProgress;
    }

    public String getXMLFormat() {
        return this.xmlFormat;
    }

    public void setXMLFormat(String xmlFormat) {
        this.xmlFormat = xmlFormat;
    }

    public ProcessorDataManager getDataManager() {
        return this.dataMgr;
    }

    public CommandContext getProcessorContext() {
        return this.commandContext;
    }
    
    public Object clone() {
        XMLProcessorEnvironment clone = new XMLProcessorEnvironment();
        copyIntoClone(clone);
        return clone;
    }
 
    /** 
     * Utility method to copy cloned state into newly-instantiated
     * (empty) clone.  Clone will appear as if it were reset.
     * @param clone new but empty
     */
    protected void copyIntoClone(XMLProcessorEnvironment clone) {
        // Programs - just get the one at the bottom of the stack
        ProgramState initialProgramState = this.programStack.getLast();
        ProgramState newState = new ProgramState();
        newState.program = initialProgramState.program;
        clone.programStack.addFirst(newState);
               
        // XML results form and format
        clone.setXMLFormat(this.getXMLFormat());
    }

    public GroupSymbol getDocumentGroup() {
        return this.documentGroup;
    }

    public void setDocumentGroup(GroupSymbol documentGroup) {
        this.documentGroup = documentGroup;
    }
    
    
    boolean isStagingTableLoaded(String tableName) {
        return this.loadedStagingTables.contains(tableName);
    }
    
    void markStagingTableAsLoaded(String tableName) {
        this.loadedStagingTables.add(tableName);
    }
    
    public BufferManager getBufferManager() {
		return bufferMgr;
	}
    
}
