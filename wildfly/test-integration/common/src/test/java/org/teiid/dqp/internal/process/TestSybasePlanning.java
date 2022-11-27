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

import java.util.Arrays;
import java.util.List;

import org.teiid.dqp.internal.datamgr.CapabilitiesConverter;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.jdbc.sybase.SybaseExecutionFactory;
import org.teiid.util.Version;

@SuppressWarnings("nls")
public class TestSybasePlanning extends BaseQueryTest {

    private static final boolean DEBUG = false;

    public TestSybasePlanning(String name) {
        super(name);
    }

    public void testQuery3() throws Exception{
        SybaseExecutionFactory ef = new SybaseExecutionFactory();
        ef.setDatabaseVersion(Version.DEFAULT_VERSION);

        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(CapabilitiesConverter.convertCapabilities(ef));

        HardcodedDataManager dataMgr = new HardcodedDataManager();

        List<?>[] expected =
            new List<?>[] { Arrays.asList("2002") };

        dataMgr.addData("SELECT g_0.TimestampValue FROM BQT1.SmallA AS g_0", //$NON-NLS-1$
                        new List<?>[] {Arrays.asList(TimestampUtil.createTimestamp(102, 1, 2, 3, 4, 5, 6))});

        doProcess(RealMetadataFactory.exampleBQTCached(),
                "select formattimestamp(timestampvalue, 'yyyy') from bqt1.smalla", //$NON-NLS-1$
                capFinder, dataMgr, expected, DEBUG);

    }

}
