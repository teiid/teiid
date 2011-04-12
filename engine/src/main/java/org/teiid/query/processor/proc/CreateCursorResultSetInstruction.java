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

import java.util.List;
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
	
	public static final String RS_NAME = "EXECSQL_INSTRUCTION"; //$NON-NLS-1$
	
    protected String rsName;
    protected ProcessorPlan plan;
    private boolean update;
    private Map<ElementSymbol, ElementSymbol> procAssignments;
    
    public CreateCursorResultSetInstruction(String rsName, ProcessorPlan plan, boolean update){
        this.rsName = rsName;
        this.plan = plan;
        this.update = update;
    }
    
    public void setProcAssignments(
			Map<ElementSymbol, ElementSymbol> procAssignments) {
		this.procAssignments = procAssignments;
	}
    
    public void process(ProcedurePlan procEnv)
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        if(procEnv.resultSetExists(rsName)) {
            procEnv.removeResults(rsName);
        }
        
        procEnv.executePlan(plan, rsName, procAssignments, !update);
        
        if (update) {
        	boolean hasNext = procEnv.iterateCursor(rsName);
    		if (hasNext) {
    			procEnv.getContext().getVariableContext().setValue(ProcedurePlan.ROWCOUNT, procEnv.getCurrentRow(rsName).get(0));
    		} else {
    			procEnv.getContext().getVariableContext().setValue(ProcedurePlan.ROWCOUNT, 0);
    		}
    		procEnv.removeResults(rsName);
        }
    }

    /**
     * Returns a deep clone
     */
    public CreateCursorResultSetInstruction clone(){
        ProcessorPlan clonedPlan = this.plan.clone();
        CreateCursorResultSetInstruction clone = new CreateCursorResultSetInstruction(this.rsName, clonedPlan, update);
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
    
    @Override
    public void getChildPlans(List<ProcessorPlan> plans) {
    	plans.add(this.plan);
    }

}
