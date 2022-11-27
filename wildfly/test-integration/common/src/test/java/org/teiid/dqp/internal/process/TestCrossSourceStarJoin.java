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

import static org.teiid.query.unittest.RealMetadataFactory.*;

import java.util.List;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestCrossSourceStarJoin {

    @Test public void testCrossSourceStarJoin() throws Exception {
        String sql = "select p.Description, sum(AMOUNT) from s3 p, s2 c, s1 b, o1 f " +
                "where p.PRODUCTID = f.PRODUCT and c.CurrencyCode = f.CURRENCY and b.BOOKID = f.BOOK and b.Name = 'xyz' and c.Name = 'abc' Group by p.Description";

        MetadataStore metadataStore = new MetadataStore();

        Schema oracle = createPhysicalModel("oracle", metadataStore); //$NON-NLS-1$
        Schema sybase = createPhysicalModel("sybase", metadataStore); //$NON-NLS-1$

        // Create physical groups
        Table f = createPhysicalGroup("o1", oracle); //$NON-NLS-1$
        f.setCardinality(5276965);
        Table b = createPhysicalGroup("s1", sybase); //$NON-NLS-1$
        b.setCardinality(141496);
        Table c = createPhysicalGroup("s2", sybase); //$NON-NLS-1$
        c.setCardinality(228);
        Table p = createPhysicalGroup("s3", sybase); //$NON-NLS-1$
        p.setCardinality(200);

        List<Column> f_cols = createElements(f,
                new String[] { "PRODUCT", "CURRENCY", "BOOK", "AMOUNT"}, //$NON-NLS-1$
                new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.BIG_DECIMAL});

        f_cols.get(0).setDistinctValues(400);
        f_cols.get(1).setDistinctValues(228);
        f_cols.get(2).setDistinctValues(141496);
        createKey(KeyRecord.Type.Index, "idx_p", f, f_cols.subList(0, 1));
        createKey(KeyRecord.Type.Index, "idx_c", f, f_cols.subList(1, 2));
        createKey(KeyRecord.Type.Index, "idx_b", f, f_cols.subList(2, 3));

        List<Column> b_cols = createElements(b,
                new String[] { "BOOKID", "Name"}, //$NON-NLS-1$
                new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        createKey(KeyRecord.Type.Primary, "pk", b, b_cols.subList(0, 1));
        b_cols.get(1).setDistinctValues(70000);

        //createKey(KeyRecord.Type.Unique, "uk", b, b_cols.subList(1, 2));

        List<Column> c_cols = createElements(c,
                new String[] { "Name", "CurrencyCode"}, //$NON-NLS-1$
                new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER});

        createKey(KeyRecord.Type.Primary, "pk", c, c_cols.subList(1, 2));
        //createKey(KeyRecord.Type.Unique, "uk", c, c_cols.subList(0, 1));

        List<Column> p_cols = createElements(p,
                new String[] { "PRODUCTID", "Description"}, //$NON-NLS-1$
                new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        createKey(KeyRecord.Type.Primary, "pk", p, p_cols.subList(0, 1));

        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("oracle", TestTPCR.oracleCapabilities()); //$NON-NLS-1$
        finder.addCapabilities("sybase", TestTPCR.sqlServerCapabilities()); //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "star");

        TestOptimizer.helpPlan(sql, metadata, new String[] {
                "SELECT g_0.CurrencyCode AS c_0 FROM sybase.s2 AS g_0 WHERE g_0.Name = 'abc' ORDER BY c_0",
                "SELECT g_0.BOOKID AS c_0 FROM sybase.s1 AS g_0 WHERE g_0.Name = 'xyz' ORDER BY c_0",
                "SELECT g_0.PRODUCTID AS c_0, g_0.Description AS c_1 FROM sybase.s3 AS g_0 ORDER BY c_0",
                "SELECT g_0.BOOK, g_0.PRODUCT, g_0.CURRENCY, SUM(g_0.AMOUNT) FROM oracle.o1 AS g_0 WHERE (g_0.BOOK IN (<dependent values>)) AND (g_0.PRODUCT IN (<dependent values>)) AND (g_0.CURRENCY IN (<dependent values>)) GROUP BY g_0.BOOK, g_0.PRODUCT, g_0.CURRENCY"
        }, finder, ComparisonMode.EXACT_COMMAND_STRING);

        //test that aggregate will not be staged
        f.setCardinality(527696);
        TestOptimizer.helpPlan(sql, metadata, new String[] {
                "SELECT g_0.CurrencyCode AS c_0 FROM sybase.s2 AS g_0 WHERE g_0.Name = 'abc' ORDER BY c_0",
                "SELECT g_0.BOOK, g_0.PRODUCT, g_0.CURRENCY, g_0.AMOUNT FROM oracle.o1 AS g_0 WHERE (g_0.BOOK IN (<dependent values>)) AND (g_0.CURRENCY IN (<dependent values>))",
                "SELECT g_0.PRODUCTID AS c_0, g_0.Description AS c_1 FROM sybase.s3 AS g_0 ORDER BY c_0",
                "SELECT g_0.BOOKID AS c_0 FROM sybase.s1 AS g_0 WHERE g_0.Name = 'xyz' ORDER BY c_0"
        }, finder, ComparisonMode.EXACT_COMMAND_STRING);
    }

}
