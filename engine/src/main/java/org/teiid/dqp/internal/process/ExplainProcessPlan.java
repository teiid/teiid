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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.language.SQLConstants;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.ExplainCommand;
import org.teiid.query.sql.lang.ExplainCommand.Format;
import org.teiid.query.util.CommandContext;

public class ExplainProcessPlan extends ProcessorPlan {

    private ExplainCommand explainCommand;
    private ProcessorPlan actualPlan;

    public ExplainProcessPlan(ProcessorPlan actualPlan, ExplainCommand explainCommand) {
        this.actualPlan = actualPlan;
        this.explainCommand = explainCommand;
    }

    @Override
    public void initialize(CommandContext context, ProcessorDataManager dataMgr,
            BufferManager bufferMgr) {
        super.initialize(context, dataMgr, bufferMgr);
        this.actualPlan.initialize(context, dataMgr, bufferMgr);
    }

    @Override
    public List getOutputElements() {
        return explainCommand.getProjectedSymbols();
    }

    @Override
    public void open()
            throws TeiidComponentException, TeiidProcessingException {
        if (!explainCommand.isNoExec()) {
            actualPlan.open();
        }
    }

    @Override
    public TupleBatch nextBatch() throws BlockedException,
            TeiidComponentException, TeiidProcessingException {
        while (!explainCommand.isNoExec()) {
            TupleBatch batch = actualPlan.nextBatch();
            if (batch.getTerminationFlag()) {
                break;
            }
        }
        RequestWorkItem workItem = this.getContext().getWorkItem();
        PlanNode planNode = workItem.getQueryPlan();
        Format format = Format.TEXT;
        if (explainCommand.getFormat() != null) {
            format = explainCommand.getFormat();
        }
        List<Object> row = new ArrayList<Object>(1);
        switch (format) {
        case TEXT:
            String result = planNode.toString();
            row.add(new ClobType(new ClobImpl(result)));
            break;
        case YAML:
            String yaml = planNode.toYaml();
            row.add(new ClobType(new ClobImpl(yaml)));
            break;
        case XML:
            String xml = planNode.toXml();
            row.add(new XMLType(new SQLXMLImpl(xml)));
            break;
        default:
            throw new AssertionError("format not implemented"); //$NON-NLS-1$
        }
        TupleBatch result = new TupleBatch(1, Arrays.asList(row));
        result.setTerminationFlag(true);
        return result;
    }

    @Override
    public void close() throws TeiidComponentException {
        if (!explainCommand.isNoExec()) {
            actualPlan.close();
        }
    }

    @Override
    public ProcessorPlan clone() {
        ExplainProcessPlan clone = new ExplainProcessPlan(actualPlan.clone(), explainCommand);
        return clone;
    }

    @Override
    public PlanNode getDescriptionProperties() {
        return actualPlan.getDescriptionProperties();
    }

    @Override
    public void reset() {
        actualPlan.reset();
    }

    @Override
    public String toString() {
        return SQLConstants.NonReserved.EXPLAIN + ' ' + actualPlan.toString();
    }

}
