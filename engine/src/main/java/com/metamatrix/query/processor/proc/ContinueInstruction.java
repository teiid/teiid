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

import java.util.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.query.processor.program.*;

/**
 * <p>This {@link ProgramInstruction} continue with the next loop when processed</p>.
 */
public class ContinueInstruction extends ProgramInstruction {
    public String toString() {
        return "CONTINUE INSTRUCTION"; //$NON-NLS-1$
    }

    public void process(ProcedurePlan env) throws MetaMatrixComponentException {
        Program parentProgram = env.peek();
        
        //find the parent program that contains the loop/while instruction
        while(true){            
            if(parentProgram.getCurrentInstruction() instanceof RepeatedInstruction){
                break;
            }
            env.pop(); 
            parentProgram = env.peek();
        } 
    }
    
    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "CONTINUE"); //$NON-NLS-1$
        return props;
    }
    
}
