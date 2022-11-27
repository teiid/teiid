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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.metadata.Column;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;


@SuppressWarnings("nls")
public class TestAccumuloQueryVisitor {
    private AccumuloExecutionFactory translator;
    private TranslationUtility utility;


    @Before
    public void setUp() throws Exception {
        this.translator = new AccumuloExecutionFactory();
        this.translator.start();

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(
                ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("sampledb.ddl")), "sakila", "rental");
        this.utility = new TranslationUtility(metadata);
    }

    @Test
    public void testSelectStar()  throws Exception {
        Command cmd = this.utility.parseCommand("select * from Customer");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertTrue(visitor.getRanges().isEmpty());
        assertNotNull(visitor.projectedColumns());

        List<Column> columns = visitor.projectedColumns();
        assertEquals("customer", visitor.getScanTable().getName());

        assertEquals(3, columns.size());
        Column rowid = columns.get(0);
        Column firstName = columns.get(1);

        assertEquals("customer_id", rowid.getName());
        assertEquals("rowid", rowid.getNameInSource());

        assertEquals("firstName", firstName.getName());
        assertEquals("customer", firstName.getProperty(AccumuloMetadataProcessor.CF, false));
        assertEquals("firstNameAttribute", firstName.getProperty(AccumuloMetadataProcessor.CQ, false));

    }

    @Test
    public void testSelectColumn() throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertTrue(visitor.getRanges().isEmpty());
        assertNotNull(visitor.projectedColumns());
        List<Column> columns = visitor.projectedColumns();
        assertEquals(1, columns.size());
        Column name = columns.get(0);
        assertEquals("firstName", name.getName());
        assertEquals("customer", name.getProperty(AccumuloMetadataProcessor.CF, false));
        assertEquals("firstNameAttribute", name.getProperty(AccumuloMetadataProcessor.CQ, false));
    }

    private AccumuloQueryVisitor buildVisitor(Command cmd) throws TranslatorException {
        AccumuloQueryVisitor visitor = new AccumuloQueryVisitor(this.translator);
        visitor.visitNode(cmd);
        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }
        return visitor;
    }

    @Test
    public void testSelectEquality()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id = 1");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(1, visitor.getRanges().size());
        Range range = visitor.getRanges().get(0);
        assertEquals(AccumuloQueryVisitor.singleRowRange(AccumuloQueryVisitor.buildKey(new Integer(1))), range);
    }

    @Test
    public void testWhereIN()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id IN (1,2)");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(2, visitor.getRanges().size());
        assertEquals(AccumuloQueryVisitor.singleRowRange(AccumuloQueryVisitor
                .buildKey(new Integer(2))), visitor.getRanges().get(0));
        assertEquals(AccumuloQueryVisitor.singleRowRange(AccumuloQueryVisitor
                .buildKey(new Integer(1))), visitor.getRanges().get(1));
    }

    @Test
    public void testWhereNOT_IN()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id NOT IN (1,2)");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(3, visitor.getRanges().size());
        assertEquals(new Range(AccumuloQueryVisitor.buildKey(new Integer(2)),
                false, null, true), visitor.getRanges().get(0));
        assertEquals(new Range(AccumuloQueryVisitor.buildKey(new Integer(1)),
                false, AccumuloQueryVisitor.buildKey(new Integer(2)), false),
                visitor.getRanges().get(1));
        assertEquals(new Range(null, true,AccumuloQueryVisitor.buildKey(new Integer(1)), false),
                visitor.getRanges().get(2));
    }

    @Test
    @Ignore
    public void testWhereComapreLE()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id < 2");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(1, visitor.getRanges().size());
        assertEquals(new Range(null, true, AccumuloQueryVisitor.buildKey(new Integer(2)), false), visitor.getRanges().get(0));
    }

    @Test
    @Ignore
    public void testWhereComapreLEEQ()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id <= 2");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(2, visitor.getRanges().size());
        Range r1 = new Range(null, true, AccumuloQueryVisitor.buildKey(new Integer(2)), false);
        Range r2 = AccumuloQueryVisitor.singleRowRange(AccumuloQueryVisitor.buildKey(new Integer(2)));
        assertEquals(r1, visitor.getRanges().get(0));
        assertEquals(r2, visitor.getRanges().get(1));
    }

    @Test
    @Ignore
    public void testWhereComapreGT()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id > 2");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(1, visitor.getRanges().size());
        assertEquals(new Range(AccumuloQueryVisitor.buildKey(new Integer(2)).followingKey(PartialKey.ROW),
                null, false, true, false, true),visitor.getRanges().get(0));
    }

    @Test
    @Ignore
    public void testWhereComapreGTEQ()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id >= 2");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(1, visitor.getRanges().size());
        assertEquals(new Range(AccumuloQueryVisitor.buildKey(new Integer(2)), true, null, true), visitor.getRanges().get(0));
    }

    @Test
    public void testWhereComapreNOTEQ()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id <> 2");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(2, visitor.getRanges().size());
        assertEquals(new Range(null, true, AccumuloQueryVisitor.buildKey(new Integer(2)), false),
                visitor.getRanges().get(0));
        assertEquals(new Range(AccumuloQueryVisitor.buildKey(new Integer(2)).followingKey(PartialKey.ROW),
                null, false, true, false, true), visitor.getRanges().get(1));
    }

    @Test
    @Ignore
    public void testWhereComapreAND1()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id < 2 and customer_id > 4");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(2, visitor.getRanges().size());
        Range r1 = new Range(null, true, AccumuloQueryVisitor.buildKey(new Integer(2)), false);
        Range r2 = new Range(AccumuloQueryVisitor.buildKey(new Integer(4)).followingKey(PartialKey.ROW),
                null, false, true, false, true);
        assertEquals(Range.mergeOverlapping(Arrays.asList(r1, r2)), visitor.getRanges());
    }

    @Test
    public void testWhereComapreAND2()  throws Exception {
        Command cmd = this.utility.parseCommand("select firstname from Customer where customer_id < 2 and customer_id != 4");
        AccumuloQueryVisitor visitor = buildVisitor(cmd);

        assertEquals("customer", visitor.getScanTable().getName());
        assertEquals(2, visitor.getRanges().size());
        Range r1 = new Range(null, true, AccumuloQueryVisitor.buildKey(new Integer(2)), false);
        Range r2 = new Range(null, true, AccumuloQueryVisitor.buildKey(new Integer(4)), false);
        Range r3 = new Range(AccumuloQueryVisitor.buildKey(new Integer(4)).followingKey(PartialKey.ROW),
                null, false, true, false, true);
        assertEquals(Range.mergeOverlapping(Arrays.asList(r1, r2, r3)), visitor.getRanges());
    }

}
