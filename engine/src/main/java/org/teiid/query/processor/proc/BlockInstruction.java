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

package org.teiid.query.processor.proc;

import static org.teiid.query.analysis.AnalysisRecord.*;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;

public class BlockInstruction extends ProgramInstruction {

    private Program program;

    public BlockInstruction(Program ifProgram) {
        this.program = ifProgram;
    }

    public void process(ProcedurePlan procEnv)
        throws BlockedException, TeiidComponentException, TeiidProcessingException {
        procEnv.push(program);
    }

    public Program getProgram(){
        return this.program;
    }

    /**
     * Returns a deep clone
     */
    public BlockInstruction clone(){
        return new BlockInstruction(this.program.clone());
    }

    public String toString() {
        return "BLOCK INSTRUCTION:\n " + this.program; //$NON-NLS-1$
    }

    public PlanNode getDescriptionProperties() {
        PlanNode props = new PlanNode("BLOCK"); //$NON-NLS-1$
        props.addProperty(PROP_PROGRAM, this.program.getDescriptionProperties());
        return props;
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        return program.requiresTransaction(transactionalReads);
    }

}
