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

import static org.junit.Assert.*;
import static org.teiid.query.processor.TestProcessor.*;

import org.junit.Test;
import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.ShowPlan;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestExplainRequest {

    @Test public void testExplainPlanning() {
        // explain doesn't modify the plan, we should just get the expected plan back
        String sql = "explain SELECT * FROM pm1.g1"; //$NON-NLS-1$

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        assertTrue(plan instanceof RelationalPlan);
    }

    @Test public void testExplainRequestProcessing() throws TeiidComponentException, TeiidProcessingException {
        String sql = "explain SELECT e1 FROM pm1.g1"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        RequestMessage message = new RequestMessage(sql);
        DQPWorkContext workContext = RealMetadataFactory.buildWorkContext(metadata, RealMetadataFactory.example1VDB());

        Request request = TestRequest.helpProcessMessage(message, null, workContext);

        assertFalse(request.requestMsg.isNoExec()); //we handle noexec in the processorplan
        assertEquals(ShowPlan.ON, request.requestMsg.getShowPlan());
        assertEquals(DataTypeManager.DefaultDataClasses.CLOB, ((Expression)request.processor.getOutputElements().get(0)).getType());

        sql = "explain (format XML, analyze) select e1 from pm1.g1";
        message = new RequestMessage(sql);
        workContext = RealMetadataFactory.buildWorkContext(metadata, RealMetadataFactory.example1VDB());
        request = TestRequest.helpProcessMessage(message, null, workContext);

        assertFalse(request.requestMsg.isNoExec());
        assertEquals(ShowPlan.ON, request.requestMsg.getShowPlan());
        assertEquals(DataTypeManager.DefaultDataClasses.XML, ((Expression)request.processor.getOutputElements().get(0)).getType());
    }


}
