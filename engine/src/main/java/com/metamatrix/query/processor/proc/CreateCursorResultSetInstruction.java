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
package com.metamatrix.query.processor.proc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.program.ProgramInstruction;

/**
 */
public class CreateCursorResultSetInstruction extends ProgramInstruction {
    protected String rsName;
    protected ProcessorPlan plan;
    
    public CreateCursorResultSetInstruction(String rsName, ProcessorPlan plan){
        this.rsName = rsName;
        this.plan = plan;
    }
    
    /**
     * If the result set named rsName does not exist yet in the {@link ProcessorEnvironment}, then
     * this instruction will define that result set.  It will then throw a BlockedException if
     * this result set is selecting from other than temp groups (because those results will be
     * delivered asynchronously).  IF the result set named rsName does already exist, this 
     * instruction will just increment the program counter and do nothing else.
     * @throws BlockedException if this result set is not selecting from
     * only temp groups
     */
    public void process(ProcedurePlan procEnv)
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {

        if(procEnv.resultSetExists(rsName)) {
            procEnv.removeResults(rsName);
        }
        
        procEnv.executePlan(plan, rsName);
    }

    /**
     * Returns a deep clone
     */
    public Object clone(){
        ProcessorPlan clonedPlan = (ProcessorPlan) this.plan.clone();
        return new CreateCursorResultSetInstruction(this.rsName, clonedPlan);
    }
    
    public String toString(){
        return "CREATE CURSOR RESULTSET INSTRUCTION - " + rsName; //$NON-NLS-1$
    }
    
    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "CREATE CURSOR"); //$NON-NLS-1$
        props.put(PROP_RESULT_SET, this.rsName);
        props.put(PROP_SQL, this.plan.toString());
        return props;
    }
    
    public Object getCommand() { //Defect 13291 - added method to support changes to ProcedurePlan
        return plan;
    }

    /** 
     * @see com.metamatrix.query.processor.program.ProgramInstruction#getChildPlans()
     * @since 4.2
     */
    public Collection getChildPlans() {
        List plans = new ArrayList(1);
        plans.add(this.plan);
        return plans;
    }

}
