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

import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.ProcessorPlan;


/**
 * <p>Abstract superclass of all program instructions.</p>
 * 
 * <p>All processor
 * instructions need to be cloneable, but it most cases the default 
 * Object.clone operation will suffice, since in most cases the processing
 * instructions will be stateless, or the state will be immutable.
 * The exception to this are instructions that have sub programs in them -
 * those sub programs need to be cloned.</p>
 */
public abstract class ProgramInstruction implements Cloneable {

    public ProgramInstruction() {
    } 
   
    /**
     * Allow this instruction to do whatever processing it needs, and to
     * in turn manipulate the running program. A typical instruction should simply {@link
     * Program#incrementProgramCounter increment} the program counter of the current program, but specialized
     * instructions may add sub programs to the stack or not increment the counter (so that they are executed again.)
     */
    public abstract void process(ProcedurePlan env) 
        throws TeiidComponentException, TeiidProcessingException;
        
    /**
     * Finds all nested plans and returns them.
     * @return List of ProcessorPlan 
     * @since 4.2
     */
    public void getChildPlans(List<ProcessorPlan> plans) {
    }
    
    /**
     * Override Object.clone() to make the method public.  This method 
     * simply calls super.clone(), deferring to the default shallow
     * cloning.  Some ProcessorInstruction subclasses may need to 
     * override with custom safe or deep cloning.
     * @return shallow clone
     */
    public ProgramInstruction clone() {
        try {
            return (ProgramInstruction)super.clone();
        } catch (CloneNotSupportedException e) {
            //should never get here, since
            //this Class does support clone
        }
        return null;
    }

	public abstract PlanNode getDescriptionProperties();
    
}
