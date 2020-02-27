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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.QueryProcessorFactoryImpl;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.query.tempdata.GlobalTableStoreImpl.MatTableInfo;
import org.teiid.query.tempdata.TempTableDataManager;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"nls", "unchecked"})
public class TestMaterialization {

    private TempMetadataAdapter metadata;
    private TempTableDataManager dataManager;
    private TempTableStore tempStore;
    private GlobalTableStoreImpl globalStore;
    private ProcessorPlan previousPlan;
    private HardcodedDataManager hdm;

    @Before public void setUp() {
        tempStore = new TempTableStore("1", TransactionMode.ISOLATE_WRITES); //$NON-NLS-1$
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        TransformationMetadata actualMetadata = RealMetadataFactory.exampleMaterializedView();
        globalStore = new GlobalTableStoreImpl(bm, actualMetadata.getVdbMetaData(), actualMetadata);
        metadata = new TempMetadataAdapter(actualMetadata, tempStore.getMetadataStore());
        hdm = new HardcodedDataManager();
        hdm.addData("SELECT MatSrc.MatSrc.x FROM MatSrc.MatSrc", new List[] {Arrays.asList((String)null), Arrays.asList("one"), Arrays.asList("two"), Arrays.asList("three")});
        hdm.addData("SELECT MatTable.info.e1, MatTable.info.e2 FROM MatTable.info", new List[] {Arrays.asList("a", 1), Arrays.asList("a", 2)});
        hdm.addData("SELECT MatTable.info.e2, MatTable.info.e1 FROM MatTable.info", new List[] {Arrays.asList(1, "a"), Arrays.asList(2, "a")});

        SessionAwareCache<CachedResults> cache = new SessionAwareCache<CachedResults>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
        cache.setTupleBufferCache(bm);
        dataManager = new TempTableDataManager(hdm, bm, cache);
    }

    private void execute(String sql, List<?>... expectedResults) throws Exception {
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setTempTableStore(tempStore);
        cc.setGlobalTableStore(globalStore);
        cc.setMetadata(metadata);
        CapabilitiesFinder finder = new DefaultCapabilitiesFinder();
        previousPlan = TestProcessor.helpGetPlan(TestProcessor.helpParse(sql), metadata, finder, cc);
        cc.setQueryProcessorFactory(new QueryProcessorFactoryImpl(BufferManagerFactory.getStandaloneBufferManager(), dataManager, finder, null, metadata));
        TestProcessor.doProcess(previousPlan, dataManager, expectedResults, cc);
    }

    @Test public void testPopulate() throws Exception {
        execute("SELECT * from vgroup3 where x = 'one'", Arrays.asList("one", "zne"));
        assertEquals(1, hdm.getCommandHistory().size());
        execute("SELECT * from vgroup3 where x is null", Arrays.asList(null, null));
        assertEquals(1, hdm.getCommandHistory().size());
    }

    @Test public void testReadWrite() throws Exception {
        execute("SELECT * from vgroup3 where x = 'one'", Arrays.asList("one", "zne"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String matTableName = RelationalPlanner.MAT_PREFIX + "MATVIEW.VGROUP3";
        this.globalStore.getState(matTableName, baos);
        MatTableInfo matTableInfo = this.globalStore.getMatTableInfo(matTableName);
        long time = matTableInfo.getUpdateTime();
        this.globalStore.failedLoad(matTableName);
        this.globalStore.setState(matTableName, new ByteArrayInputStream(baos.toByteArray()));
        assertEquals(time, matTableInfo.getUpdateTime());
        execute("SELECT * from vgroup3 where x = 'one'", Arrays.asList("one", "zne"));

        execute("select lookup('mattable.info', 'e1', 'e2', 5)", Arrays.asList((String)null));
        baos = new ByteArrayOutputStream();
        String codeTableName = "#CODE_MATTABLE.INFO.E2.E1";
        this.globalStore.getState(codeTableName, baos);
        this.globalStore.setState(codeTableName, new ByteArrayInputStream(baos.toByteArray()));
    }

    @Test(expected=TeiidProcessingException.class) public void testCodeTableResponseException() throws Exception {
        //duplicate key
        execute("select lookup('mattable.info', 'e2', 'e1', 'a')");
    }

    @Test public void testCodeTable() throws Exception {
        execute("select lookup('mattable.info', 'e1', 'e2', 5)", Arrays.asList((String)null));
        assertEquals(1, hdm.getCommandHistory().size());
        execute("select lookup('mattable.info', 'e1', 'e2', 1)", Arrays.asList("a"));
        assertEquals(1, hdm.getCommandHistory().size());
    }

    @Test public void testCodeTableReservedWord() throws Exception {
        hdm.addData("SELECT MatTable.info.\"value\", MatTable.info.e1 FROM MatTable.info", Arrays.asList("5", "a"));
        execute("select lookup('mattable.info', 'e1', 'VALUE', '5')", Arrays.asList("a"));
    }

    @Test public void testTtl() throws Exception {
        execute("SELECT * from vgroup4 where x = 'one'", Arrays.asList("one"));
        assertEquals(1, hdm.getCommandHistory().size());
        execute("SELECT * from vgroup4 where x is null", Arrays.asList((String)null));
        assertEquals(1, hdm.getCommandHistory().size());
        Thread.sleep(150);
        execute("SELECT * from vgroup4 where x is null", Arrays.asList((String)null));
        assertEquals(2, hdm.getCommandHistory().size());
    }

    @Test public void testProcedureCache() throws Exception {
        execute("call sp1('one')", Arrays.asList("one"));
        assertEquals(1, hdm.getCommandHistory().size());
        execute("call sp1('one')", Arrays.asList("one"));
        assertEquals(1, hdm.getCommandHistory().size());
        execute("call sp1('one') option nocache sp.sp1", Arrays.asList("one"));
        assertEquals(2, hdm.getCommandHistory().size());
        execute("call sp1(null)");
        assertEquals(3, hdm.getCommandHistory().size());
        execute("call sp1(null)");
        assertEquals(3, hdm.getCommandHistory().size());
    }

    @Test public void testCoveringSecondaryIndex() throws Exception {
        execute("SELECT * from vgroup3 where y in ('zne', 'zwo') order by y desc", Arrays.asList("two", "zwo"), Arrays.asList("one", "zne"));
        execute("SELECT * from vgroup3 where y is null", Arrays.asList((String)null, (String)null));
    }

    @Test public void testNonCoveringSecondaryIndex() throws Exception {
        execute("SELECT * from vgroup5 where y in ('zwo', 'zne') order by y desc", Arrays.asList("two", "zwo", 1), Arrays.asList("one", "zne", 1));
        execute("SELECT * from vgroup5 where y is null", Arrays.asList((String)null, (String)null, 1), Arrays.asList(" b", (String)null, 1), Arrays.asList(" c", (String)null, 1), Arrays.asList(" d", (String)null, 1));
        execute("SELECT * from vgroup5 where y is null and z = 2");
    }

    @Test public void testNonCoveringSecondaryIndexWithoutPrimaryKey() throws Exception {
        execute("SELECT * from vgroup6 where y in ('zne', 'zwo') order by y desc", Arrays.asList("two", "zwo"), Arrays.asList("one", "zne"));
        execute("SELECT * from vgroup6 where y is null", Arrays.asList((String)null, (String)null));
    }

    @Test public void testPrimaryKeyOnOtherColumn() throws Exception {
        execute("SELECT * from vgroup7 where y is null", Arrays.asList("1", null, 1));
    }

    @Test public void testKeyJoin() throws Exception {
        execute("SELECT * from vgroup7, vgroup5 where vgroup7.x = vgroup5.x");
    }

    @Test public void testFunctionBasedIndexQuery() throws Exception {
        TempMetadataID id = this.globalStore.getGlobalTempTableMetadataId(metadata.getGroupID("MatView.vgroup2a"));
        assertEquals("SELECT MatView.VGroup2a.*, ucase(x) FROM MatView.VGroup2a option nocache MatView.VGroup2a", id.getQueryNode().getQuery());
    }

}
