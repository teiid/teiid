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

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.ProcessorPlan;


/**
 */
public class CreateCursorResultSetInstruction extends ProgramInstruction {
	
	public static final String RS_NAME = "EXECSQL_INSTRUCTION"; //$NON-NLS-1$
	
    protected String rsName;
    protected ProcessorPlan plan;
    
    public CreateCursorResultSetInstruction(String rsName, ProcessorPlan plan){
        this.rsName = rsName;
        this.plan = plan;
    }
    
    /**
     * If the result set named rsName does not exist yet in the {@link ProcedurePlan}, then
     * this instruction will define that result set.  It will then throw a BlockedException if
     * this result set is selecting from other than temp groups (because those results will be
     * delivered asynchronously).  IF the result set named rsName does already exist, this 
     * instruction will just increment the program counter and do nothing else.
     * @throws BlockedException if this result set is not selecting from
     * only temp groups
     */
    public void process(ProcedurePlan procEnv)
        throws BlockedException, TeiidComponentException, TeiidProcessingException {

        if(procEnv.resultSetExists(rsName)) {
            procEnv.removeResults(rsName);
        }
        
        procEnv.executePlan(plan, rsName);
    }

    /**
     * Returns a deep clone
     */
    public CreateCursorResultSetInstruction clone(){
        ProcessorPlan clonedPlan = this.plan.clone();
        return new CreateCursorResultSetInstruction(this.rsName, clonedPlan);
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
