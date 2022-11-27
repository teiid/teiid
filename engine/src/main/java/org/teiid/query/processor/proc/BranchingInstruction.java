/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 */
package org.teiid.query.processor.proc;

import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.sql.proc.BranchingStatement;
import org.teiid.query.sql.proc.BranchingStatement.BranchingMode;

/**
 * <p>This {@link ProgramInstruction} continue with the next loop when processed.
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
