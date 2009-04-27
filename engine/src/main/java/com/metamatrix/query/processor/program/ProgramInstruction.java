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

package com.metamatrix.query.processor.program;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.query.processor.Describable;
import com.metamatrix.query.processor.proc.ProcedurePlan;

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
public abstract class ProgramInstruction implements Cloneable, Describable {

    public ProgramInstruction() {
    } 
   
    /**
     * Allow this instruction to do whatever processing it needs, and to
     * in turn manipulate the running program, (via the ProcessorEnvironment
     * {@link ProgramEnvironment#getProgramStack getProgramStack} method.) A typical instruction should simply {@link
     * Program#incrementProgramCounter increment} the program counter of the current program, but specialized
     * instructions may add sub programs to the stack or not increment the counter (so that they are executed again.)
     */
    public abstract void process(ProcedurePlan env) 
        throws MetaMatrixComponentException, MetaMatrixProcessingException;
        
    /**
     * Finds all nested plans and returns them.
     * @return List of ProcessorPlan 
     * @since 4.2
     */
    public Collection getChildPlans() {
        return null;
    }
    
    /**
     * Override Object.clone() to make the method public.  This method 
     * simply calls super.clone(), deferring to the default shallow
     * cloning.  Some ProcessorInstruction subclasses may need to 
     * override with custom safe or deep cloning.
     * @return shallow clone
     */
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            //should never get here, since
            //this Class does support clone
        }
        return null;
    }
    
    public Map getDescriptionProperties() {
        Map props = new HashMap();
        return props;        
    }
    

}
