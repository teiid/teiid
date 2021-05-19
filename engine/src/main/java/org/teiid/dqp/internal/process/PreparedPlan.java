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

package org.teiid.dqp.internal.process;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.teiid.cache.Cachable;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.util.CommandContext;


public class PreparedPlan implements Cachable {
    private ProcessorPlan plan;
    private Command command;
    private List<Reference> refs;
    private AnalysisRecord analysisRecord;

    private AccessInfo accessInfo = new AccessInfo();

    /**
     * Return the ProcessorPlan.
     */
    public ProcessorPlan getPlan(){
        return plan;
    }

    /**
     * Return the plan description.
     */
    public AnalysisRecord getAnalysisRecord(){
        return this.analysisRecord;
    }

    /**
     * Return the Command .
     */
    public Command getCommand(){
        return command;
    }

    /**
     * Return the list of Reference.
     */
    public List<Reference> getReferences(){
        return refs;
    }

    /**
     * Set the ProcessorPlan.
     * @param context
     */
    public void setPlan(ProcessorPlan planValue, CommandContext context){
        plan = planValue;
        this.accessInfo.populate(context, false);
        //TODO: expand this logic
        if (planValue instanceof RelationalPlan) {
            RelationalPlan rp = (RelationalPlan)planValue;
            if (rp.getRootNode() instanceof AccessNode) {
                this.accessInfo.setSensitiveToMetadataChanges(false);
            }
        }
    }

    /**
     * Set the plan description.
     */
    public void setAnalysisRecord(AnalysisRecord analysisRecord){
        this.analysisRecord = analysisRecord;
    }

    /**
     * Set the Command.
     */
    public void setCommand(Command commandValue){
        command = commandValue;
    }

    /**
     * Set the list of Reference.
     */
    public void setReferences(List<Reference> refsValue){
        if (refsValue != null) {
            // the object order is not necessarily the same as the parsing order
            // make sure they align
            Collections.sort(refsValue, new Comparator<Reference>() {

                @Override
                public int compare(Reference o1, Reference o2) {
                    return Integer.compare(o1.getIndex(), o2.getIndex());
                }

            });
        }
        refs = refsValue;
    }

    @Override
    public AccessInfo getAccessInfo() {
        return accessInfo;
    }

    @Override
    public boolean prepare(TupleBufferCache bufferManager) {
        return true; //no remotable actions
    }

    @Override
    public boolean restore(TupleBufferCache bufferManager) {
        return true; //no remotable actions
    }

    public boolean validate() {
        return this.accessInfo.validate(false, 0);
    }

}