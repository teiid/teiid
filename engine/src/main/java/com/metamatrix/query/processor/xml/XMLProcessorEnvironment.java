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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.query.mapping.xml.ResultSetInfo;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.LogConstants;


public class XMLProcessorEnvironment {

    /** XML result documents should be in String form */
    public static final String STRING_RESULT = "String"; //$NON-NLS-1$

    /** XML result documents should be in JDOM document form */
    public static final String JDOM_DOCUMENT_RESULT = "JDOM Document"; //$NON-NLS-1$
    
    /* Stack <ProgramState> */
    private LinkedList programStack = new LinkedList();
    
    private Map loadedStagingTables = Collections.synchronizedMap(new HashMap());

    private DocumentInProgress documentInProgress;
    
    private String xmlFormat;
    private String xmlResultsForm;
    
    private GroupSymbol documentGroup;
    
    private ProcessorDataManager dataMgr;
    private BufferManager bufferMgr;
    private CommandContext commandContext;
    
    private Collection childPlans;
    
    protected XMLProcessorEnvironment(){
    }
    
    public XMLProcessorEnvironment(Program mainProgram){
        pushProgram(mainProgram);
    }
    
    /**
     * @see ProcessorEnvironment#initialize(XMLPlan)
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
        private int recursionCount = NOT_RECURSIVE;
        
        private static final int NOT_RECURSIVE = 0;
        
        public String toString() {
            return program.toString() + ", counter " + programCounter + ", recursionCount " + (recursionCount == NOT_RECURSIVE? "not recursive": "" + recursionCount); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }
    }
    
    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#getCurrentProgram()
     */
    public Program getCurrentProgram() {
        // jh case 5266
        if ( this.programStack.size() > 0 ) {
            ProgramState programState = (ProgramState)this.programStack.getFirst();
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

    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#incrementCurrentProgramCounter()
     */
    public void incrementCurrentProgramCounter() {
        ProgramState programState = (ProgramState)this.programStack.getFirst();
        programState.programCounter++;
        
        // Always leave one Program in the Program stack, even if it is finished
        while (this.programStack.size() > 1 &&
               programState.programCounter >= programState.program.getProcessorInstructions().size()) {
            this.programStack.removeFirst();
            if(LogManager.isMessageToBeRecorded(LogConstants.CTX_XML_PLAN, MessageLevel.TRACE)) {
                LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"Processor Environment popped program w/ recursion count " + programState.recursionCount, "; " + this.programStack.size(), " programs left."}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
            programState = (ProgramState)this.programStack.getFirst();
        }        
    }
    
    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#pushProgram(Program)
     */
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
            LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"Pushed recursive program w/ recursion count " + programState.recursionCount}); //$NON-NLS-1$
            
        } else {
            LogManager.logTrace(LogConstants.CTX_XML_PLAN, new Object[]{"Pushed non-recursive program w/ recursion count " + programState.recursionCount}); //$NON-NLS-1$
        }
        this.programStack.addFirst(programState);
    }

    /** 
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#getCurrentInstruction()
     */
    public ProcessorInstruction getCurrentInstruction() {
        ProgramState programState = (ProgramState)this.programStack.getFirst();
        
        //Case 5266: account for an empty program on to the stack; 
        //this is needed to handle an empty sequence or an excluded Choice properly.
        if (programState != null && programState.program.getProcessorInstructions().isEmpty()) {
            incrementCurrentProgramCounter();
            
            programState = (ProgramState)this.programStack.getFirst();
        }
        
        if (programState == null) {
            return null;
        }
        
        return programState.program.getInstructionAt(programState.programCounter);
    }
    
    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#getProgramRecursionCount(Program)
     */
    public int getProgramRecursionCount(Program program){
        
        ProgramState programState = getProgramState(program);
        if (programState == null) {
            return ProgramState.NOT_RECURSIVE;
        }
        return programState.recursionCount;
    }
    
    private ProgramState getProgramState(Program program) {
        ProgramState result = null;
        Iterator stackedPrograms = this.programStack.iterator();
        while (stackedPrograms.hasNext()) {
            ProgramState programState = (ProgramState) stackedPrograms.next();
            Program stackedProgram = programState.program;
            if (stackedProgram == program) {
                result = programState;
                break;
            }
        }
            
        return result;
    }

    public PlanExecutor createResultExecutor(String resultSetName, ResultSetInfo info) 
        throws MetaMatrixComponentException{
    
        // this cloning code is here beacuse, cloning the plan inside the resultset is not possible
        // becuse of the dependencies.
        ResultSetInfo clone = (ResultSetInfo)info.clone();
        ProcessorPlan plan = (ProcessorPlan)clone.getPlan();
        plan = (ProcessorPlan)plan.clone();
        clone.setPlan(plan);
        
        return new RelationalPlanExecutor(clone, this.commandContext, this.dataMgr, this.bufferMgr);
    }
    
    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#getDocumentInProgress()
     */
    public DocumentInProgress getDocumentInProgress() {
        return this.documentInProgress;
    }

    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#setDocumentInProgress(DocumentInProgress)
     */
    public void setDocumentInProgress(DocumentInProgress documentInProgress) {
        this.documentInProgress = documentInProgress;
    }

    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#getXMLFormat
     */
    public String getXMLFormat() {
        return this.xmlFormat;
    }

    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#setXMLFormat
     */
    public void setXMLFormat(String xmlFormat) {
        this.xmlFormat = xmlFormat;
    }

    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#getXMLResultsForm
     */
    public String getXMLResultsForm(){
        return this.xmlResultsForm;
    }

    public void setXMLResultsForm(String xmlResultsForm){
        this.xmlResultsForm = xmlResultsForm;
    }
   
    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#getDataManager()
     */
    public ProcessorDataManager getDataManager() {
        return this.dataMgr;
    }

    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#getProcessorContext()
     */
    public CommandContext getProcessorContext() {
        return this.commandContext;
    }
    
    
    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#getChildPlans()
     */
    public Collection getChildPlans() {
        return this.childPlans;
    }
 
    /**
     * @see com.metamatrix.query.processor.xml.ProcessorEnvironment#clone()
     */
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
        ProgramState initialProgramState = (ProgramState)this.programStack.getLast();
        ProgramState newState = new ProgramState();
        newState.program = initialProgramState.program;
        newState.programCounter = 0;
        newState.recursionCount = ProgramState.NOT_RECURSIVE;
        clone.programStack.addFirst(newState);
               
        // XML results form and format
        clone.setXMLFormat(this.getXMLFormat());
        clone.setXMLResultsForm(this.getXMLResultsForm());
    }

    public GroupSymbol getDocumentGroup() {
        return this.documentGroup;
    }

    public void setDocumentGroup(GroupSymbol documentGroup) {
        this.documentGroup = documentGroup;
    }
    
    
    boolean isStagingTableLoaded(String tableName) {
        return Boolean.TRUE.equals(this.loadedStagingTables.get(tableName));
    }
    
    void markStagingTableAsLoaded(String tableName) {
        this.loadedStagingTables.put(tableName, Boolean.TRUE);
    }
    
    public void setChildPlans(Collection childPlans){
    	this.childPlans = childPlans;
    }
}
