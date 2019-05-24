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
    private boolean usesLocalTemp;

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

        procEnv.executePlan(plan, rsName, procAssignments, mode, usesLocalTemp);
    }

    /**
     * Returns a deep clone
     */
    public CreateCursorResultSetInstruction clone(){
        ProcessorPlan clonedPlan = this.plan.clone();
        CreateCursorResultSetInstruction clone = new CreateCursorResultSetInstruction(this.rsName, clonedPlan, mode);
        clone.setProcAssignments(procAssignments);
        clone.usesLocalTemp = true;
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

    public Mode getMode() {
        return mode;
    }

    public void setUsesLocalTemp(boolean b) {
        this.usesLocalTemp = b;
    }

    @Override
    public Boolean requiresTransaction(boolean transactionalReads) {
        return plan.requiresTransaction(transactionalReads);
    }

}
