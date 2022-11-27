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

import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.jdbc.TeiidSQLException;


/**
 * <p>Abstract superclass of all program instructions.
 *
 * <p>All processor
 * instructions need to be cloneable, but it most cases the default
 * Object.clone operation will suffice, since in most cases the processing
 * instructions will be stateless, or the state will be immutable.
 * The exception to this are instructions that have sub programs in them -
 * those sub programs need to be cloned.
 */
public abstract class ProgramInstruction implements Cloneable {

    public ProgramInstruction() {
    }

    /**
     * Allow this instruction to do whatever processing it needs, and to
     * in turn manipulate the running program. A typical instruction should simply {@link
     * Program#incrementProgramCounter increment} the program counter of the current program, but specialized
     * instructions may add sub programs to the stack or not increment the counter (so that they are executed again.)
     * @throws TeiidSQLException
     */
    public abstract void process(ProcedurePlan env)
        throws TeiidComponentException, TeiidProcessingException, TeiidSQLException;

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

    public Boolean requiresTransaction(boolean transactionalReads) {
        return false;
    }

}
