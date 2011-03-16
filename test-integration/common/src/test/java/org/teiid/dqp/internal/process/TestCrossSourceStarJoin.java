/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.dqp.internal.process;

import static org.teiid.query.unittest.RealMetadataFactory.*;

import java.util.List;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.datamgr.CapabilitiesConverter;
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
import org.teiid.translator.jdbc.oracle.OracleExecutionFactory;
import org.teiid.translator.jdbc.sybase.SybaseExecutionFactory;

@SuppressWarnings("nls")
public class TestCrossSourceStarJoin {
	
    @Test public void testCrossSourceStartJoin() throws Exception {
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

        f_cols.get(0).setDistinctValues(200);
        f_cols.get(1).setDistinctValues(228);
        f_cols.get(2).setDistinctValues(141496);
        createKey(KeyRecord.Type.Index, "idx_p", f, f_cols.subList(0, 1));
        createKey(KeyRecord.Type.Index, "idx_c", f, f_cols.subList(1, 2));
        createKey(KeyRecord.Type.Index, "idx_b", f, f_cols.subList(2, 3));
        
        List<Column> b_cols = createElements(b,
                new String[] { "BOOKID", "Name"}, //$NON-NLS-1$
                new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        
        createKey(KeyRecord.Type.Primary, "pk", b, b_cols.subList(0, 1));
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
        finder.addCapabilities("oracle", CapabilitiesConverter.convertCapabilities(new OracleExecutionFactory())); //$NON-NLS-1$
        finder.addCapabilities("sybase", CapabilitiesConverter.convertCapabilities(new SybaseExecutionFactory())); //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "star");
        
        TestOptimizer.helpPlan(sql, metadata, new String[] {
        		"SELECT g_0.CurrencyCode AS c_0 FROM sybase.s2 AS g_0 WHERE g_0.Name = 'abc' ORDER BY c_0", 
        		"SELECT g_0.BOOKID FROM sybase.s1 AS g_0 WHERE g_0.Name = 'xyz'", 
        		"SELECT g_0.PRODUCTID AS c_0, g_0.Description AS c_1 FROM sybase.s3 AS g_0 ORDER BY c_0", 
        		"SELECT g_0.CURRENCY AS c_0, g_0.PRODUCT AS c_1, g_0.BOOK AS c_2, SUM(g_0.AMOUNT) AS c_3 FROM oracle.o1 AS g_0 WHERE (g_0.CURRENCY IN (<dependent values>)) AND (g_0.PRODUCT IN (<dependent values>)) AND (g_0.BOOK IN (<dependent values>)) GROUP BY g_0.CURRENCY, g_0.PRODUCT, g_0.BOOK ORDER BY c_0 NULLS FIRST"
        }, finder, ComparisonMode.EXACT_COMMAND_STRING);
    } 

}
