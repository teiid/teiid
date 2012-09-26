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

/*
 */
package org.teiid.query.processor.proc;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.Map;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.symbol.ElementSymbol;


/**
 */
public class CreateCursorResultSetInstruction extends ProgramInstruction {
	
	public enum Mode {
		UPDATE,
		HOLD,
		NOHOLD
	}
	
    protected String rsName;
    protected ProcessorPlan plan;
    private Mode mode;
    private Map<ElementSymbol, ElementSymbol> procAssignments;
    
    public CreateCursorResultSetInstruction(String rsName, ProcessorPlan plan, Mode mode){
        this.rsName = rsName;
        this.plan = plan;
        this.mode = mode;
    }
    
    public void setProcAssignments(
			Map<ElementSymbol, ElementSymbol> procAssignments) {
		this.procAssignments = procAssignments;
	}
    
    public void process(ProcedurePlan procEnv)
        throws BlockedException, TeiidComponentException, TeiidProcessingException {
    	
        procEnv.executePlan(plan, rsName, procAssignments, mode);
    }

    /**
     * Returns a deep clone
     */
    public CreateCursorResultSetInstruction clone(){
        ProcessorPlan clonedPlan = this.plan.clone();
        CreateCursorResultSetInstruction clone = new CreateCursorResultSetInstruction(this.rsName, clonedPlan, mode);
        clone.setProcAssignments(procAssignments);
        return clone;
    }
    
    public String toString(){
        return "CREATE CURSOR RESULTSET INSTRUCTION - " + rsName; //$NON-NLS-1$
    }
    
    public PlanNode getDescriptionProperties() {
        PlanNode props = new PlanNode("CREATE CURSOR"); //$NON-NLS-1$
        props.addProperty(PROP_RESULT_SET, this.rsName);
        props.addProperty(PROP_SQL, this.plan.getDescriptionProperties());
        return props;
    }
    
    public ProcessorPlan getCommand() { //Defect 13291 - added method to support changes to ProcedurePlan
        return plan;
    }
    
}
