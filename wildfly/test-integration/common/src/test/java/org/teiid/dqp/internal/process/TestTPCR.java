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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.CapabilitiesConverter;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.jdbc.oracle.OracleExecutionFactory;
import org.teiid.translator.jdbc.sqlserver.SQLServerExecutionFactory;
import org.teiid.util.Version;


@SuppressWarnings("nls")
public class TestTPCR extends BaseQueryTest {

    private static final boolean DEBUG = false;

    private static final QueryMetadataInterface METADATA = createMetadata(UnitTestUtil.getTestDataPath()+"/TPC_R.vdb");  //$NON-NLS-1$

    public TestTPCR(String name) {
        super(name);
    }

    /**
     * Will create a full push down query
     */
    public void testQuery3() throws Exception{
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("TPCR_Oracle_9i", oracleCapabilities()); //$NON-NLS-1$

        HardcodedDataManager dataMgr = new HardcodedDataManager();

        List<?>[] expected =
            new List<?>[] { Arrays.asList(new Object[] { new Double(2456423.0), new BigDecimal("406181.0111"), TimestampUtil.createDate(95, 2, 5), new Double(0.0) }), //$NON-NLS-1$
                         Arrays.asList(new Object[] { new Double(3459808.0), new BigDecimal("405838.6989"), TimestampUtil.createDate(95, 2, 4), new Double(0.0) }), //$NON-NLS-1$
                         Arrays.asList(new Object[] { new Double(492164.0), new BigDecimal("390324.0610"), TimestampUtil.createDate(95, 1, 19), new Double(0.0) }) }; //$NON-NLS-1$

        dataMgr.addData("SELECT g_2.L_ORDERKEY AS c_0, SUM((g_2.L_EXTENDEDPRICE * (1 - g_2.L_DISCOUNT))) AS c_1, g_1.O_ORDERDATE AS c_2, g_1.O_SHIPPRIORITY AS c_3 FROM TPCR_Oracle_9i.CUSTOMER AS g_0, TPCR_Oracle_9i.ORDERS AS g_1, TPCR_Oracle_9i.LINEITEM AS g_2 WHERE (g_0.C_CUSTKEY = g_1.O_CUSTKEY) AND (g_2.L_ORDERKEY = g_1.O_ORDERKEY) AND (g_0.C_MKTSEGMENT = 'BUILDING') AND (g_1.O_ORDERDATE < {d'1995-03-15'}) AND (g_2.L_SHIPDATE > {ts'1995-03-15 00:00:00.0'}) GROUP BY g_2.L_ORDERKEY, g_1.O_ORDERDATE, g_1.O_SHIPPRIORITY ORDER BY c_1 DESC, c_2", //$NON-NLS-1$
                        expected);

        doProcess(METADATA,
                "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority " + //$NON-NLS-1$
                "from customer, orders, lineitem " +  //$NON-NLS-1$
                "where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey " +  //$NON-NLS-1$
                "and o_orderdate < {ts'1995-03-15 00:00:00'} " +  //$NON-NLS-1$
                "and l_shipdate > {ts'1995-03-15 00:00:00'} " +  //$NON-NLS-1$
                "group by l_orderkey, o_orderdate, o_shippriority " + //$NON-NLS-1$
                "order by revenue desc, o_orderdate", //$NON-NLS-1$
                finder, dataMgr, expected, DEBUG);

    }

    public void testQueryCase3042() throws Exception{
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("TPCR_Ora", oracleCapabilities()); //$NON-NLS-1$

        HardcodedDataManager dataMgr = new HardcodedDataManager();
        List<?>[] expected =
            new List<?>[] { Arrays.asList(new Object[] { new Integer(5) } ) };

        dataMgr.addData("SELECT COUNT(*) FROM TPCR_Ora.CUSTOMER AS g_0 LEFT OUTER JOIN TPCR_Ora.ORDERS AS g_1 ON g_0.C_CUSTKEY = g_1.O_CUSTKEY WHERE (g_1.O_ORDERKEY IS NULL) OR ((g_1.O_ORDERDATE < {ts'1992-01-02 00:00:00.0'}) AND (g_0.C_ACCTBAL > 0))", //$NON-NLS-1$
                       expected);

        doProcess(BaseQueryTest.createMetadata(UnitTestUtil.getTestDataPath()+"/TPCR_3.vdb"),  //$NON-NLS-1$
                "SELECT count (*)  " + //$NON-NLS-1$
                "FROM TPCR_Ora.CUSTOMER LEFT OUTER JOIN TPCR_Ora.ORDERS ON C_CUSTKEY = O_CUSTKEY " +  //$NON-NLS-1$
                "WHERE (O_ORDERKEY IS NULL) OR O_ORDERDATE < '1992-01-02 00:00:00' " +  //$NON-NLS-1$
                "AND C_ACCTBAL > 0", //$NON-NLS-1$
                finder, dataMgr, expected, DEBUG);

    }

    /**
     * Test of case 3047 - need a query planner optimization to recognize when join clause criteria
     * could be migrated to WHERE clause of an atomic query, as long as the join is not being pushed
     * down.  In this case, there is a left outer join.  The join criteria includes
     * O_ORDERDATE < {ts'1992-01-02 00:00:00.0'} which is on the inner side of the outer join and
     * thus cannot normally be moved to the WHERE clause.  However, since the join is cross-data
     * source, the join will be performed in MetaMatrix, and the above criteria could be moved to
     * the WHERE clause of the atomic query, since that WHERE clause will effectively still be
     * applied before the join is processed, and the results will be the same.  This is what the
     * user wants to happen.
     * @throws Exception
     */
    public void testQueryCase3047() throws Exception{
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("TPCR_Ora", oracleCapabilities()); //$NON-NLS-1$
        finder.addCapabilities("TPCR_SQLS", sqlServerCapabilities()); //$NON-NLS-1$

        HardcodedDataManager dataMgr = new HardcodedDataManager();

        List<?>[] oracleExpected =
            new List<?>[] { Arrays.asList(new Object[] { new Long(5), "Bill", "101 Fake St.", "392839283", "21.12" } ), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                         Arrays.asList(new Object[] { new Long(6), "Stu", "102 Fake St.", "385729385", "51.50" } )}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        dataMgr.addData("SELECT g_0.C_CUSTKEY AS c_0, g_0.C_NAME AS c_1, g_0.C_ADDRESS AS c_2, g_0.C_PHONE AS c_3, g_0.C_ACCTBAL AS c_4 FROM TPCR_Ora.CUSTOMER AS g_0 WHERE g_0.C_ACCTBAL > 50 ORDER BY c_0", //$NON-NLS-1$
                        oracleExpected);

        List<?>[] sqlServerExpected =
            new List<?>[] { Arrays.asList(new Object[] { new Integer(5), new Integer(12), new Long(5) } ),
                         Arrays.asList(new Object[] { new Integer(5), new Integer(13), new Long(5) } )};
        dataMgr.addData("SELECT g_0.O_CUSTKEY AS c_0, g_0.O_ORDERKEY AS c_1, convert(g_0.O_CUSTKEY, long) AS c_2 FROM TPCR_SQLS.ORDERS AS g_0 WHERE g_0.O_ORDERDATE < {ts'1992-01-02 00:00:00.0'} ORDER BY c_2", //$NON-NLS-1$
                        sqlServerExpected);

        List<?>[] expected =
            new List<?>[] { Arrays.asList(new Object[] { new Long(5), "Bill", "101 Fake St.", "392839283", "21.12", new Integer(12) } ), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                         Arrays.asList(new Object[] { new Long(5), "Bill", "101 Fake St.", "392839283", "21.12", new Integer(13) } ), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                         Arrays.asList(new Object[] { new Long(6), "Stu", "102 Fake St.", "385729385", "51.50", null } )}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        doProcess(BaseQueryTest.createMetadata(UnitTestUtil.getTestDataPath()+"/TPCR_3.vdb"),  //$NON-NLS-1$
                "SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_PHONE, C_ACCTBAL, O_ORDERKEY FROM TPCR_Ora.CUSTOMER " + //$NON-NLS-1$
                "LEFT OUTER JOIN TPCR_SQLS.ORDERS ON C_CUSTKEY = O_CUSTKEY " + //$NON-NLS-1$
                "AND O_ORDERDATE < {ts'1992-01-02 00:00:00.0'} " + //$NON-NLS-1$
                "WHERE (C_ACCTBAL > 50)", //$NON-NLS-1$
                finder, dataMgr, expected, DEBUG);

    }

    /**
     * Confirm the workaround for case 3047 (using an inline view to get the desired piece
     * of criteria pushed down)
     * @throws Exception
     * @since 4.3
     */
    public void testQueryCase3047workaround() throws Exception{
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("TPCR_Ora", oracleCapabilities()); //$NON-NLS-1$
        finder.addCapabilities("TPCR_SQLS", sqlServerCapabilities()); //$NON-NLS-1$

        HardcodedDataManager dataMgr = new HardcodedDataManager();

        List<?>[] oracleExpected =
            new List<?>[] { Arrays.asList(new Object[] { new Long(5), "Bill", "101 Fake St.", "392839283", "51.12" } ), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                         Arrays.asList(new Object[] { new Long(6), "Stu", "102 Fake St.", "385729385", "51.50" } )}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        dataMgr.addData("SELECT g_0.C_CUSTKEY AS c_0, g_0.C_NAME AS c_1, g_0.C_ADDRESS AS c_2, g_0.C_PHONE AS c_3, g_0.C_ACCTBAL AS c_4 FROM TPCR_Ora.CUSTOMER AS g_0 WHERE g_0.C_ACCTBAL > 50 ORDER BY c_0", //$NON-NLS-1$
                        oracleExpected);

        List<?>[] sqlServerExpected =
            new List<?>[] { Arrays.asList(new Object[] { new Integer(5), new Integer(12), new Long(5) } ),
                         Arrays.asList(new Object[] { new Integer(5), new Integer(13), new Long(5) } )};
        dataMgr.addData("SELECT g_0.O_CUSTKEY AS c_0, g_0.O_ORDERKEY AS c_1, convert(g_0.O_CUSTKEY, long) AS c_2 FROM TPCR_SQLS.ORDERS AS g_0 WHERE g_0.O_ORDERDATE < {ts'1992-01-02 00:00:00.0'} ORDER BY c_2", //$NON-NLS-1$
                        sqlServerExpected);

        List<?>[] expected =
            new List<?>[] { Arrays.asList(new Object[] { new Long(5), "Bill", "101 Fake St.", "392839283", "51.12", new Integer(12) } ), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                         Arrays.asList(new Object[] { new Long(5), "Bill", "101 Fake St.", "392839283", "51.12", new Integer(13) } ), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                         Arrays.asList(new Object[] { new Long(6), "Stu", "102 Fake St.", "385729385", "51.50", null } )}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        doProcess(BaseQueryTest.createMetadata(UnitTestUtil.getTestDataPath()+"/TPCR_3.vdb"),  //$NON-NLS-1$
                "SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_PHONE, C_ACCTBAL, O_ORDERKEY FROM TPCR_Ora.CUSTOMER " + //$NON-NLS-1$
                "LEFT OUTER JOIN " + //$NON-NLS-1$
                "(SELECT O_CUSTKEY, O_ORDERKEY FROM TPCR_SQLS.ORDERS WHERE O_ORDERDATE < {ts'1992-01-02 00:00:00.0'}) AS X " + //$NON-NLS-1$
                "ON C_CUSTKEY = O_CUSTKEY " + //$NON-NLS-1$
                "WHERE (C_ACCTBAL > 50)", //$NON-NLS-1$
                finder, dataMgr, expected, DEBUG);

    }

    public void testQuery22() throws Exception{
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("TPCR_Oracle_9i", oracleCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT custsale.cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM (SELECT left(C_PHONE, 2) AS cntrycode, CUSTOMER.C_ACCTBAL FROM CUSTOMER WHERE (left(C_PHONE, 2) IN ('13','31','23','29','30','18','17')) AND (CUSTOMER.C_ACCTBAL > (SELECT AVG(CUSTOMER.C_ACCTBAL) FROM CUSTOMER WHERE (CUSTOMER.C_ACCTBAL > 0.0) AND (left(C_PHONE, 2) IN ('13','31','23','29','30','18','17')))) AND (NOT (EXISTS (SELECT * FROM ORDERS WHERE O_CUSTKEY = C_CUSTKEY)))) AS custsale GROUP BY custsale.cntrycode ORDER BY custsale.cntrycode", //$NON-NLS-1$
                METADATA, null, finder,
                new String[] {"SELECT left(g_0.C_PHONE, 2) AS c_0, COUNT(*) AS c_1, SUM(g_0.C_ACCTBAL) AS c_2 FROM TPCR_Oracle_9i.CUSTOMER AS g_0 WHERE (left(g_0.C_PHONE, 2) IN ('13', '31', '23', '29', '30', '18', '17')) AND (g_0.C_ACCTBAL > (SELECT AVG(g_1.C_ACCTBAL) FROM TPCR_Oracle_9i.CUSTOMER AS g_1 WHERE (g_1.C_ACCTBAL > 0.0) AND (left(g_1.C_PHONE, 2) IN ('13', '31', '23', '29', '30', '18', '17')))) AND (NOT EXISTS (SELECT 1 FROM TPCR_Oracle_9i.ORDERS AS g_2 WHERE g_2.O_CUSTKEY = g_0.C_CUSTKEY)) GROUP BY left(g_0.C_PHONE, 2) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    public static SourceCapabilities oracleCapabilities() {
        OracleExecutionFactory oef = new OracleExecutionFactory();
        oef.setDatabaseVersion(Version.DEFAULT_VERSION);
        return CapabilitiesConverter.convertCapabilities(oef);
    }

    public void testDefect22475() throws Exception {
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("TPCR_Oracle_9i", sqlServerCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT from (SELECT SUPPLIER.S_ACCTBAL, SUPPLIER.S_NAME, NATION.N_NAME, PART.P_PARTKEY, PART.P_MFGR, SUPPLIER.S_ADDRESS, SUPPLIER.S_PHONE, SUPPLIER.S_COMMENT FROM PART, SUPPLIER, PARTSUPP, NATION, REGION WHERE (PART.P_PARTKEY = PS_PARTKEY) AND (S_SUPPKEY = PS_SUPPKEY) AND (P_SIZE = 15) AND (P_TYPE LIKE '%BRASS') AND (S_NATIONKEY = N_NATIONKEY) AND (N_REGIONKEY = R_REGIONKEY) AND (R_NAME = 'EUROPE') AND (PS_SUPPLYCOST = (SELECT MIN(PS_SUPPLYCOST) FROM PARTSUPP, SUPPLIER, NATION, REGION WHERE (PART.P_PARTKEY = PS_PARTKEY) AND (S_SUPPKEY = PS_SUPPKEY) AND (S_NATIONKEY = N_NATIONKEY) AND (N_REGIONKEY = R_REGIONKEY) AND (R_NAME = 'EUROPE'))) ORDER BY SUPPLIER.S_ACCTBAL DESC, NATION.N_NAME, SUPPLIER.S_NAME, PART.P_PARTKEY) as x", //$NON-NLS-1$
                METADATA, null, finder,
                new String[] {"SELECT g_1.S_ACCTBAL, g_1.S_NAME, g_3.N_NAME, g_0.P_PARTKEY, g_0.P_MFGR, g_1.S_ADDRESS, g_1.S_PHONE, g_1.S_COMMENT FROM TPCR_Oracle_9i.PART AS g_0, TPCR_Oracle_9i.SUPPLIER AS g_1, TPCR_Oracle_9i.PARTSUPP AS g_2, TPCR_Oracle_9i.NATION AS g_3, TPCR_Oracle_9i.REGION AS g_4 WHERE (g_3.N_REGIONKEY = g_4.R_REGIONKEY) AND (g_1.S_NATIONKEY = g_3.N_NATIONKEY) AND (g_1.S_SUPPKEY = g_2.PS_SUPPKEY) AND (g_2.PS_SUPPLYCOST = (SELECT MIN(g_5.PS_SUPPLYCOST) FROM TPCR_Oracle_9i.PARTSUPP AS g_5, TPCR_Oracle_9i.SUPPLIER AS g_6, TPCR_Oracle_9i.NATION AS g_7, TPCR_Oracle_9i.REGION AS g_8 WHERE (g_6.S_SUPPKEY = g_5.PS_SUPPKEY) AND (g_6.S_NATIONKEY = g_7.N_NATIONKEY) AND (g_7.N_REGIONKEY = g_8.R_REGIONKEY) AND (g_5.PS_PARTKEY = g_0.P_PARTKEY) AND (g_8.R_NAME = 'EUROPE'))) AND (g_0.P_PARTKEY = g_2.PS_PARTKEY) AND (g_0.P_SIZE = 15.0) AND (g_0.P_TYPE LIKE '%BRASS') AND (g_4.R_NAME = 'EUROPE')"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    public static SourceCapabilities sqlServerCapabilities() {
        SQLServerExecutionFactory sef = new SQLServerExecutionFactory();
        sef.setDatabaseVersion(Version.DEFAULT_VERSION);
        return CapabilitiesConverter.convertCapabilities(sef);
    }

    public void testMultiLevelCorrelationWithSubqueryMergeJoin() throws Exception {
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();

        String ddl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tpch.ddl"));
        TransformationMetadata tm = RealMetadataFactory.fromDDL("x", new RealMetadataFactory.DDLHolder("tpch1", ddl),
                new RealMetadataFactory.DDLHolder("tpch2", ddl));

        OracleExecutionFactory oef = new OracleExecutionFactory();
        oef.setDatabaseVersion(Version.getVersion("11.0"));
        SourceCapabilities bsc1 = CapabilitiesConverter.convertCapabilities(oef, 1);
        SourceCapabilities bsc2 = CapabilitiesConverter.convertCapabilities(oef, 2);

        finder.addCapabilities("tpch1", bsc1); //$NON-NLS-1$
        finder.addCapabilities("tpch2", bsc2); //$NON-NLS-1$

        HardcodedDataManager dm = new HardcodedDataManager(tm);
        dm.addData("SELECT g_0.\"S_SUPPLIERKEY\" AS c_0, g_0.\"S_NAME\" AS c_1, g_0.\"S_ADDRESS\" AS c_2 FROM \"SOAEDS\".\"SUPPLIER\" AS g_0, \"SOAEDS\".\"NATION\" AS g_1 WHERE g_0.\"S_NATIONKEY\" = g_1.\"N_NATIONKEY\" AND g_1.\"N_NAME\" = 'BRAZIL' ORDER BY c_0", Arrays.asList(1, "x", "y"));
        dm.addData("SELECT g_0.\"PS_SUPPLIERKEY\" AS c_0 FROM \"SOAEDS\".\"PARTSUPP\" AS g_0 WHERE g_0.\"PS_PARTKEY\" IN (SELECT g_1.\"P_PARTKEY\" FROM \"SOAEDS\".\"PART\" AS g_1 WHERE g_1.\"P_NAME\" LIKE 'powder%') AND g_0.\"PS_AVAILQTY\" > (SELECT (0.5 * SUM(g_2.\"L_QUANTITY\")) FROM \"SOAEDS\".\"LINEITEM\" AS g_2 WHERE g_2.\"L_PARTKEY\" = g_0.\"PS_PARTKEY\" AND g_2.\"L_SUPPLIERKEY\" = g_0.\"PS_SUPPLIERKEY\" AND g_2.\"L_SHIPDATE\" >= {ts '1994-01-01 00:00:00.0'} AND g_2.\"L_SHIPDATE\" < {ts '1995-01-01 00:00:00.0'}) ORDER BY c_0", Arrays.asList(1));

        String sql = "select s_name, s_address from tpch1.soaeds.supplier, tpch1.soaeds.nation where s_supplierkey in ( select ps_supplierkey from tpch2.soaeds.partsupp where ps_partkey in ( select p_partkey from tpch2.soaeds.part where p_name like 'powder%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from tpch2.soaeds.lineitem where l_partkey = ps_partkey and l_supplierkey = ps_supplierkey and l_shipdate >= '1994-01-01' and l_shipdate < TIMESTAMPADD(SQL_TSI_YEAR,'1', '1994-01-01') ) ) and s_nationkey = n_nationkey and n_name = 'BRAZIL' order by s_name;";

        List<?>[] expected =
                new List<?>[] { Arrays.asList("x", "y")};

        doProcess(tm, sql,
                finder, dm, expected, DEBUG);

    }

}
