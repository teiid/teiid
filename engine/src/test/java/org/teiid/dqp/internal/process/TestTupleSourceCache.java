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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestTupleSourceCache {

    @Test public void testNodeId() throws Exception {
        TupleSourceCache tsc = new TupleSourceCache();
        HardcodedDataManager pdm = new HardcodedDataManager() {
            @Override
            public TupleSource registerRequest(CommandContext context,
                    Command command, String modelName,
                    RegisterRequestParameter parameterObject)
                    throws TeiidComponentException {
                assertEquals(1, parameterObject.nodeID);
                return Mockito.mock(TupleSource.class);
            }
        };
        CommandContext context = TestProcessor.createCommandContext();
        BufferManagerImpl bufferMgr = BufferManagerFactory.createBufferManager();
        Command command = new Insert();
        RegisterRequestParameter parameterObject = new RegisterRequestParameter("z", 1, 1);
        parameterObject.info = new RegisterRequestParameter.SharedAccessInfo();

        tsc.getSharedTupleSource(context, command, "x", parameterObject, bufferMgr, pdm);
    }

    @Test public void testJoinProcessingWithNestedSubquery() throws Exception {
        HardcodedDataManager pdm = new HardcodedDataManager();
        pdm.setBlockOnce(true);

        String sql = "select e1 from (select e1, e2 from pm1.g1 where (select e3 from pm2.g1) = true) x inner join (select e2 from pm1.g2) y on x.e2 = y.e2 "
                + "union all "
                + "select e1 from (select e1, e2 from pm1.g1 where (select e3 from pm2.g1) = true) x inner join (select e2 from pm1.g2) y on x.e2 = y.e2";

        pdm.addData("SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1", Arrays.asList(1, "a"));
        pdm.addData("SELECT pm1.g2.e2 FROM pm1.g2", Arrays.asList(1));
        pdm.addData("SELECT pm2.g1.e3 FROM pm2.g1", Arrays.asList(true));

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        TestProcessor.helpProcess(plan, pdm, new List<?>[] {Arrays.asList("a"), Arrays.asList("a")});
    }

}
