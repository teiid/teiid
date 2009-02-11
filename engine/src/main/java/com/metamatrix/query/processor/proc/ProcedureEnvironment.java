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

package com.metamatrix.query.processor.proc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.processor.NullTupleSource;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.program.Program;
import com.metamatrix.query.processor.program.ProgramEnvironment;
import com.metamatrix.query.processor.program.ProgramInstruction;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.tempdata.TempTableStore;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p>This class defines the environment that procedure language programs run in
 * and what {@link ProgramInstruction}s can access during execution. This
 * environment holds references to the {@link VariableContext}, this variable
 * context is updated with new variables and their values upon execution of the
 * program instructions.</p>
 */
public class ProcedureEnvironment extends ProgramEnvironment {

    private Map tupleSourceMap = new HashMap();     // rsName -> TupleSource
    private Map tupleSourceIDMap = new HashMap();   // rsName -> TupleSourceID
    private Map currentRowMap = new HashMap();

	private static ElementSymbol ROWS_UPDATED =
			new ElementSymbol(ProcedureReservedWords.VARIABLES+"."+ProcedureReservedWords.ROWS_UPDATED); //$NON-NLS-1$

	private static int NO_ROWS_UPDATED = 0;
	private ProcedurePlan plan;
	private VariableContext currentVarContext;
    private boolean isUpdateProcedure = true;

    private TupleSource lastTupleSource;
    
    private List outputElements;
    
    private TempTableStore tempTableStore;
    
    private LinkedList tempContext = new LinkedList();

    /**
     * Constructor for ProcedureEnvironment.
     */
    public ProcedureEnvironment() {
        super();
        this.currentVarContext = new VariableContext();
        this.currentVarContext.setValue(ROWS_UPDATED, new Integer(NO_ROWS_UPDATED));
    }

    /**
     * Initialize the environment with the procedure plan.
     */
    public void initialize(ProcessorPlan plan) {
        this.plan =  (ProcedurePlan)plan;
    }

    private TupleSource getUpdateCountAsToupleSource() {
    	Object rowCount = currentVarContext.getValue(ROWS_UPDATED);
    	if(rowCount == null) {
			rowCount = new Integer(NO_ROWS_UPDATED);
    	}

        final List updateResult = new ArrayList(1);
        updateResult.add(rowCount);

        return new UpdateCountTupleSource(updateResult);
    }

    /**
     * <p> Get the current <code>VariavleContext</code> on this environment.
     * The VariableContext is updated with variables and their values by
     * {@link ProgramInstruction}s that are part of the ProcedurePlan that use
     * this environment.</p>
     * @return The current <code>VariariableContext</code>.
     */
    public VariableContext getCurrentVariableContext() {
		return this.currentVarContext;
    }

    public void executePlan(Object command, String rsName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException {
        boolean isExecSQLInstruction = rsName.equals(ExecSqlInstruction.RS_NAME);
        // Defect 14544: Close all non-final ExecSqlInstruction tuple sources before creating a new source.
        // This guarantees that the tuple source will be removed predictably from the buffer manager.
        if (isExecSQLInstruction) {
            removeResults(ExecSqlInstruction.RS_NAME);
        }
        
        TupleSourceID tsID = this.plan.registerRequest(command);
        TupleSource source = this.plan.getResults(tsID);
        tupleSourceIDMap.put(rsName.toUpperCase(), tsID);
        tupleSourceMap.put(rsName.toUpperCase(), source);
        if(isExecSQLInstruction){
            //keep a reference to the tuple source
            //it may be the last one
            this.lastTupleSource = source;
        }
    }
    
    /** 
     * @throws MetaMatrixComponentException 
     * @see com.metamatrix.query.processor.program.ProgramEnvironment#pop()
     */
    public void pop() throws MetaMatrixComponentException {
        super.pop();
        Set current = getTempContext();

        Set tempTables = getLocalTempTables();

        tempTables.addAll(current);
        
        for (Iterator i = tempTables.iterator(); i.hasNext();) {
            removeResults((String)i.next());
        }
        
        this.tempContext.removeLast();
    }
    
    /** 
     * @see com.metamatrix.query.processor.program.ProgramEnvironment#push(com.metamatrix.query.processor.program.Program)
     */
    public void push(Program program) {
        super.push(program);
        Set current = getTempContext();
        
        Set tempTables = getLocalTempTables();
        
        current.addAll(tempTables);
        this.tempContext.add(new HashSet());
    }
    
    /** 
     * @see com.metamatrix.query.processor.program.ProgramEnvironment#incrementProgramCounter()
     */
    public void incrementProgramCounter() throws MetaMatrixComponentException {
        Program program = peek();
        ProgramInstruction instr = program.getCurrentInstruction();
        if (instr instanceof RepeatedInstruction) {
            RepeatedInstruction repeated = (RepeatedInstruction)instr;
            repeated.postInstruction(this);
        }
        super.incrementProgramCounter();
    }

    /** 
     * @return
     */
    private Set getLocalTempTables() {
        Set tempTables = this.tempTableStore.getAllTempTables();
        
        //determine what was created in this scope
        for (int i = 0; i < tempContext.size() - 1; i++) {
            tempTables.removeAll((Set)tempContext.get(i));
        }
        return tempTables;
    }

    public Set getTempContext() {
        if (this.tempContext.isEmpty()) {
            tempContext.addLast(new HashSet());
        }
        return (Set)this.tempContext.getLast();
    }

    public List getCurrentRow(String rsName) {
        return (List) currentRowMap.get(rsName.toUpperCase());
    }

    public boolean iterateCursor(String rsName)
        throws MetaMatrixComponentException, MetaMatrixProcessingException {

        String rsKey = rsName.toUpperCase();

        TupleSource source = (TupleSource) tupleSourceMap.get(rsKey);
        if(source == null) {
            // TODO - throw exception?
            return false;
        }

        List row = source.nextTuple();
        currentRowMap.put(rsKey, row);
        return (row != null);
    }

    public void removeResults(String rsName) throws MetaMatrixComponentException {
        String rsKey = rsName.toUpperCase();
        TupleSource source = (TupleSource) tupleSourceMap.get(rsKey);
        if(source != null) {
            source.closeSource();
            TupleSourceID tsID = (TupleSourceID) tupleSourceIDMap.get(rsKey);
            this.plan.removeTupleSource(tsID);
            tupleSourceMap.remove(rsKey);
            tupleSourceIDMap.remove(rsKey);
            currentRowMap.remove(rsKey);
            this.tempTableStore.removeTempTableByName(rsKey);
        }
    }


    /**
     * Get the schema from the tuple source that
     * represents the columns in a result set
     * @param rsName the ResultSet name (not a temp group)
     * @return List of elements
     * @throws QueryProcessorException if the list of elements is null
     */
    public List getSchema(String rsName) throws MetaMatrixComponentException {

        // get the tuple source
        String rsKey = rsName.toUpperCase();
        TupleSource source = (TupleSource) tupleSourceMap.get(rsKey);
        if(source == null){
            throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0037, rsName));
        }
        // get the schema from the tuple source
        List schema = source.getSchema();
        if(schema == null){
            throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0038));
        }

        return schema;
    }

    public boolean resultSetExists(String rsName) {
        String rsKey = rsName.toUpperCase();
        boolean exists = this.tupleSourceMap.containsKey(rsKey);
        return exists;
    }

    public ProcessorDataManager getDataManager() {
        return this.plan.getDataManager();
    }

    public CommandContext getContext() {
        return this.plan.getContext();
    }


    /**
     * @return
     */
    public boolean isUpdateProcedure() {
        return isUpdateProcedure;
    }

    /**
     * @param b
     */
    public void setUpdateProcedure(boolean b) {
        isUpdateProcedure = b;
    }

    /**
     * @return
     */
    public TupleSource getFinalTupleSource() {
        if(this.isUpdateProcedure){
            return this.getUpdateCountAsToupleSource();
        }

        if(lastTupleSource == null){
            return new NullTupleSource(null);
        }
        return lastTupleSource;
    }

    public List getOutputElements() {
		return outputElements;
	}

	public void setOutputElements(List outputElements) {
		this.outputElements = outputElements;
	}

	public void reset() {
        tupleSourceMap.clear();
        tupleSourceIDMap.clear();
        currentRowMap.clear();
        currentVarContext = new VariableContext();
        currentVarContext.setValue(ROWS_UPDATED, new Integer(NO_ROWS_UPDATED));
        lastTupleSource = null;
	}

    
    /** 
     * @return Returns the tempTableStore.
     * @since 5.5
     */
    public TempTableStore getTempTableStore() {
        return this.tempTableStore;
    }

    
    /** 
     * @param tempTableStore The tempTableStore to set.
     * @since 5.5
     */
    public void setTempTableStore(TempTableStore tempTableStore) {
        this.tempTableStore = tempTableStore;
    }
}
