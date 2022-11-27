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
package org.teiid.translator.accumulo;

import static org.junit.Assert.*;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.GeometryType;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;

/*
 * does not consistently run in unit tests, should still be utilized with any translator changes
 */
@Ignore
@SuppressWarnings({"nls", "unchecked"})
public class TestAccumuloQueryExecution {
    private static AccumuloExecutionFactory translator;
    private static TranslationUtility utility;
    private static AccumuloConnection connection;

    private static AccumuloClient client;
    private static Connector connector;
    private static MiniAccumuloCluster cluster;

    @BeforeClass
    public static void setUp() throws Exception {
        translator = new AccumuloExecutionFactory();
        translator.start();

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("sampledb.ddl")), "sakila", "rental");
        utility = new TranslationUtility(metadata);

        connection = Mockito.mock(AccumuloConnection.class);
        File f = UnitTestUtil.getTestScratchFile("accumulo");
        FileUtils.removeDirectoryAndChildren(f);
        MiniAccumuloConfig cfg = new MiniAccumuloConfig(f, "password");
        cluster = new MiniAccumuloCluster(cfg);
        cluster.start();

        client = cluster.createAccumuloClient("root", new PasswordToken("password"));
        client.securityOperations().changeUserAuthorizations("root", new Authorizations("public"));

        connector = Connector.from(client);

        Mockito.stub(connection.getInstance()).toReturn(connector);
        Mockito.stub(connection.getAuthorizations()).toReturn(new Authorizations("public"));
        connector.tableOperations().create("customer", true, TimeType.LOGICAL);
        connector.tableOperations().create("rental", true, TimeType.LOGICAL);
    }

    @AfterClass
    public static void teardown() throws Exception {
        cluster.stop();
    }

    private Execution executeCmd(String sql) throws TranslatorException {
        Command cmd = TestAccumuloQueryExecution.utility.parseCommand(sql);
        Execution exec =  translator.createExecution(cmd, Mockito.mock(ExecutionContext.class),
                utility.createRuntimeMetadata(), TestAccumuloQueryExecution.connection);
        exec.execute();
        return exec;
    }

    @Test
    public void testExecution() throws Exception {
        executeCmd("delete from customer");
        executeCmd("insert into customer (customer_id, firstname, lastname) values (2, 'Joe', 'A')");
        executeCmd("insert into customer (customer_id, firstname, lastname) values (1, 'John', 'B')");
        executeCmd("insert into customer (customer_id, firstname, lastname) values (3, 'Jack', 'C')");

        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select * from customer");
        assertEquals(Arrays.asList(1, "John", "B"), exec.next());
        assertEquals(Arrays.asList(2, "Joe", "A"), exec.next());
        assertEquals(Arrays.asList(3, "Jack", "C"), exec.next());
        assertNull(exec.next());


        executeCmd("Update Customer set firstname = 'Jill' where customer_id = 2");
        executeCmd("Update Customer set firstname = 'Jay' where customer_id = 2");
        exec = (AccumuloQueryExecution)executeCmd("select customer_id, firstname from customer");
        assertEquals(Arrays.asList(1, "John"), exec.next());
        assertEquals(Arrays.asList(2, "Jay"), exec.next());
        assertEquals(Arrays.asList(3, "Jack"), exec.next());
        assertNull(exec.next());

        exec = (AccumuloQueryExecution)executeCmd("select customer_id, firstname from customer where customer_id = 2");
        assertEquals(Arrays.asList(2, "Jay"), exec.next());
        assertNull(exec.next());

        executeCmd("delete from Customer where customer_id = 2");

        exec = (AccumuloQueryExecution)executeCmd("select * from customer");
        assertEquals(Arrays.asList(1, "John", "B"), exec.next());
        assertEquals(Arrays.asList(3, "Jack", "C"), exec.next());
        assertNull(exec.next());
    }

    @Test
    public void testValueInCQ() throws Exception {
        executeCmd("delete from rental");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (1, 3.99, 5)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (2, 5.99, 2)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (3, 11.99, 1)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select * from rental");
        assertEquals(Arrays.asList(1, new BigDecimal("3.99"), 5), exec.next());
        assertEquals(Arrays.asList(2, new BigDecimal("5.99"), 2), exec.next());
        assertEquals(Arrays.asList(3, new BigDecimal("11.99"), 1), exec.next());
        assertNull(exec.next());
    }

    @Test
    public void testCountStar() throws Exception {
        executeCmd("delete from rental");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (1, 3.99, 5)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (2, 5.99, 2)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (3, 11.99, 1)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (4, 12.99, 1)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select count(*) from rental");
        assertEquals(Arrays.asList(4), exec.next());
        assertNull(exec.next());
    }

    @Test
    public void testIsNULL() throws Exception {
        executeCmd("delete from customer");
        executeCmd("insert into customer (customer_id, firstname, lastname) values (2, 'Joe', 'A')");
        executeCmd("insert into customer (customer_id, firstname, lastname) values (1, null, 'B')");
        executeCmd("insert into customer (customer_id, firstname, lastname) values (3, 'Jack', 'C')");


        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select * from customer where firstname IS NULL");
        assertEquals(Arrays.asList(1, null, "B"), exec.next());
        assertNull(exec.next());

        exec = (AccumuloQueryExecution)executeCmd("select * from customer where firstname IS NOT NULL");
        //assertEquals(Arrays.asList(2, "Joe", "A"), exec.next());
        //assertEquals(Arrays.asList(3, "Jack", "C"), exec.next());
        assertNotNull(exec.next());
        assertNotNull(exec.next());

        assertNull(exec.next());
    }

    @Test
    public void testINOnNonPKColumn() throws Exception {
        executeCmd("delete from customer");
        executeCmd("insert into customer (customer_id, firstname, lastname) values (2, 'Joe', 'A')");
        executeCmd("insert into customer (customer_id, firstname, lastname) values (1, 'John', 'B')");
        executeCmd("insert into customer (customer_id, firstname, lastname) values (3, 'Jack', 'C')");


        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select * from customer where "
                + "firstname IN('Joe', 'Jack') order by lastname");
        assertEquals(Arrays.asList(2, "Joe", "A"), exec.next());
        assertEquals(Arrays.asList(3, "Jack", "C"), exec.next());
//        assertNotNull(exec.next());
//        assertNotNull(exec.next());

        assertNull(exec.next());
    }

    @Test
    public void testComparisionOnNonPKColumn() throws Exception {
        executeCmd("delete from rental");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (1, 3.99, 5)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (2, 5.99, 2)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (3, 11.99, 1)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (4, 12.99, 1)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select rental_id, amount, "
                + "customer_id from rental where amount > 6.01");
        assertEquals(Arrays.asList(3, new BigDecimal("11.99"), 1), exec.next());
        assertEquals(Arrays.asList(4, new BigDecimal("12.99"), 1), exec.next());
        assertNull(exec.next());
    }

    @Test
    public void testANDOnNonPKColumn() throws Exception {
        executeCmd("delete from rental");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (1, 3.99, 5)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (2, 5.99, 2)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (3, 11.99, 1)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (4, 12.99, 1)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select rental_id, amount, "
                + "customer_id from rental where amount > 5.99 and amount < 12.99");
        assertEquals(Arrays.asList(3, new BigDecimal("11.99"), 1), exec.next());
        assertNull(exec.next());
    }
    @Test
    public void testOROnNonPKColumn() throws Exception {
        executeCmd("delete from rental");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (1, 3.99, 5)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (2, 5.99, 2)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (3, 11.99, 1)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (4, 12.99, 1)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select amount from rental "
                + "where amount > 5.99 or customer_id = 1");
        assertEquals(Arrays.asList(new BigDecimal("11.99")), exec.next());
        assertEquals(Arrays.asList(new BigDecimal("12.99")), exec.next());
        assertNull(exec.next());
    }

    @Test
    public void testPKColumn() throws Exception {
        executeCmd("delete from rental");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (1, 3.99, 5)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (2, 5.99, 2)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (3, 11.99, 1)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (4, 12.99, 1)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select amount from rental "
                + "where rental_id = 3");
        assertEquals(Arrays.asList(new BigDecimal("11.99")), exec.next());
        assertNull(exec.next());
    }

    @Test
    public void testNonPKColumn() throws Exception {
        executeCmd("delete from rental");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (1, 3.99, 5)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (2, 5.99, 2)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (3, 11.99, 1)");
        executeCmd("insert into rental (rental_id, amount, customer_id) values (4, 12.99, 1)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select amount from rental "
                + "where customer_id >= 1 and customer_id < 2");
        assertEquals(Arrays.asList(new BigDecimal("11.99")), exec.next());
        assertEquals(Arrays.asList(new BigDecimal("12.99")), exec.next());
        assertNull(exec.next());
    }

    @Test //TEIID-3933
    public void testNumericComparision() throws Exception {
        executeCmd("delete from smalla");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (1, 1,1.99, 1)");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (2, 2, 2.99, 2)");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (3, 3, 3.99, 3)");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (4, 4, 4.99, 4)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution) executeCmd("select ROWID from smalla "
                + "where LONGNUM > 2");
        assertEquals(Arrays.asList(new Integer(3)), exec.next());
        assertEquals(Arrays.asList(new Integer(4)), exec.next());
        assertNull(exec.next());

        exec = (AccumuloQueryExecution) executeCmd("select ROWID from smalla "
                + "where DOUBLENUM > 3");
        assertEquals(Arrays.asList(new Integer(3)), exec.next());
        assertEquals(Arrays.asList(new Integer(4)), exec.next());
        assertNull(exec.next());
    }

    @Test //TEIID-3930
    public void testSelectRowID() throws Exception {
        executeCmd("delete from smalla");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (1, 1,1.99, 1)");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (2, 2, 2.99, 2)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution)executeCmd("select ROWID from smalla");
        assertEquals(Arrays.asList(new Integer("1")), exec.next());
        assertEquals(Arrays.asList(new Integer("2")), exec.next());
        assertNull(exec.next());
    }

    @Test //TEIID-3944
    public void testRowIDNumericComparision() throws Exception {
        executeCmd("delete from smalla");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (1, 1,1.99, 1)");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (2, 2, 2.99, 2)");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (3, 3, 3.99, 3)");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (4, 4, 4.99, 4)");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (15, 15, 15.99, 15)");
        executeCmd("insert into smalla (ROWID, LONGNUM, DOUBLENUM, BIGINTEGERVALUE) values (16, 16, 16.99, 16)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution) executeCmd("select ROWID, LONGNUM from smalla "
                + "where ROWID > 2");
        assertEquals(Arrays.asList(new Integer(15), new Long(15)), exec.next());
        assertEquals(Arrays.asList(new Integer(16), new Long(16)), exec.next());
        assertEquals(Arrays.asList(new Integer(3), new Long(3)), exec.next());
        assertEquals(Arrays.asList(new Integer(4), new Long(4)), exec.next());
        assertNull(exec.next());

        exec = (AccumuloQueryExecution) executeCmd("select ROWID, LONGNUM from smalla "
                + "where ROWID >= 2");
        assertEquals(Arrays.asList(new Integer(15), new Long(15)), exec.next());
        assertEquals(Arrays.asList(new Integer(16), new Long(16)), exec.next());
        assertEquals(Arrays.asList(new Integer(2), new Long(2)), exec.next());
        assertEquals(Arrays.asList(new Integer(3), new Long(3)), exec.next());
        assertEquals(Arrays.asList(new Integer(4), new Long(4)), exec.next());
        assertNull(exec.next());


        exec = (AccumuloQueryExecution) executeCmd("select ROWID, LONGNUM from smalla "
                + "where ROWID < 3");
        assertEquals(Arrays.asList(new Integer(1), new Long(1)), exec.next());
        assertEquals(Arrays.asList(new Integer(2), new Long(2)), exec.next());
        assertNull(exec.next());

        exec = (AccumuloQueryExecution) executeCmd("select ROWID, LONGNUM from smalla "
                + "where ROWID <= 3");
        assertEquals(Arrays.asList(new Integer(1), new Long(1)), exec.next());
        assertEquals(Arrays.asList(new Integer(2), new Long(2)), exec.next());
        assertEquals(Arrays.asList(new Integer(3), new Long(3)), exec.next());
        assertNull(exec.next());

        exec = (AccumuloQueryExecution) executeCmd("select ROWID, LONGNUM from smalla "
                + "where ROWID != 3");
        assertEquals(Arrays.asList(new Integer(1), new Long(1)), exec.next());
        assertEquals(Arrays.asList(new Integer(15), new Long(15)), exec.next());
        assertEquals(Arrays.asList(new Integer(16), new Long(16)), exec.next());
        assertEquals(Arrays.asList(new Integer(2), new Long(2)), exec.next());
        assertEquals(Arrays.asList(new Integer(4), new Long(4)), exec.next());
        assertNull(exec.next());

        exec = (AccumuloQueryExecution) executeCmd("select ROWID, LONGNUM from smalla "
                + "where ROWID = 3");
        assertEquals(Arrays.asList(new Integer(3), new Long(3)), exec.next());
        assertNull(exec.next());
    }

    @Test //TEIID-3944
    public void testNullRowSelection() throws Exception {
        executeCmd("delete from smalla");
        executeCmd("insert into smalla (ROWID, LONGNUM, BIGINTEGERVALUE) values (1, null, 1)");
        executeCmd("insert into smalla (ROWID, LONGNUM, BIGINTEGERVALUE) values (2, null, null)");
        executeCmd("insert into smalla (ROWID, LONGNUM, BIGINTEGERVALUE) values (3, 3, null)");
        executeCmd("insert into smalla (ROWID, LONGNUM, BIGINTEGERVALUE) values (4, 4, 4)");

        AccumuloQueryExecution exec = (AccumuloQueryExecution) executeCmd(
                "select ROWID, LONGNUM, BIGINTEGERVALUE from smalla");
        assertEquals(Arrays.asList(new Integer(1), null, new BigInteger("1")), exec.next());
        assertEquals(Arrays.asList(new Integer(2), null, null), exec.next());
        assertEquals(Arrays.asList(new Integer(3), new Long(3), null), exec.next());
        assertEquals(Arrays.asList(new Integer(4), new Long(4), new BigInteger("4")), exec.next());
        assertNull(exec.next());


        exec = (AccumuloQueryExecution) executeCmd(
                "select LONGNUM from smalla");
        ArrayList<?> NULL_ARRAY = new ArrayList();
        NULL_ARRAY.add(null);

        assertEquals(NULL_ARRAY, exec.next());
        assertEquals(NULL_ARRAY, exec.next());
        assertEquals(Arrays.asList(new Long(3)), exec.next());
        assertEquals(Arrays.asList(new Long(4)), exec.next());
        assertNull(exec.next());

        exec = (AccumuloQueryExecution) executeCmd(
                "select LONGNUM as foo from smalla");
        assertEquals(NULL_ARRAY, exec.next());
        assertEquals(NULL_ARRAY, exec.next());
        assertEquals(Arrays.asList(new Long(3)), exec.next());
        assertEquals(Arrays.asList(new Long(4)), exec.next());
        assertNull(exec.next());

        exec = (AccumuloQueryExecution) executeCmd(
                "select ROWID, LONGNUM as foo from smalla where LONGNUM is null");
        assertEquals(Arrays.asList(new Integer(1), null), exec.next());
        assertEquals(Arrays.asList(new Integer(2), null), exec.next());
        assertNull(exec.next());
    }

    @Test public void testAccumuloDataTypeManager() throws SQLException {
        GeometryType gt = new GeometryType(new byte[10]);
        gt.setSrid(4000);
        byte[] bytes = AccumuloDataTypeManager.serialize(gt);
        GeometryType gt1 = (GeometryType) AccumuloDataTypeManager.deserialize(bytes, GeometryType.class);
        assertEquals(4000, gt1.getSrid());
        assertEquals(10, gt1.length());
    }
}
