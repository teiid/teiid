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

import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.sql.proc.BranchingStatement;
import org.teiid.query.sql.proc.BranchingStatement.BranchingMode;

/**
 * <p>This {@link ProgramInstruction} continue with the next loop when processed</p>.
 */
public class BranchingInstruction extends ProgramInstruction {
	
	private BranchingStatement bs;
	
    public BranchingInstruction(BranchingStatement bs) {
    	this.bs = bs;
	}

	public String toString() {
        return bs.toString();
    }

    public void process(ProcedurePlan env) throws TeiidComponentException {
        Program parentProgram = env.peek();
        
        //find the parent program that contains the loop/while instruction
        while(true){
        	if (bs.getMode() == BranchingMode.LEAVE && bs.getLabel().equalsIgnoreCase(parentProgram.getLabel())) {
        		env.pop(true);
        		break;
        	}
            if(parentProgram.getCurrentInstruction() instanceof RepeatedInstruction){
            	if (bs.getLabel() == null) {
            		break;
            	}
            	RepeatedInstruction ri = (RepeatedInstruction)parentProgram.getCurrentInstruction();
            	if (bs.getLabel().equalsIgnoreCase(ri.getLabel())) {
            		break;
            	}
            }
            env.pop(true); 
            parentProgram = env.peek();
        } 
        
        if (bs.getMode() != BranchingMode.CONTINUE) {
        	env.incrementProgramCounter();
        }
    }
    
    public PlanNode getDescriptionProperties() {
        return new PlanNode(bs.toString());
    }
    
}
