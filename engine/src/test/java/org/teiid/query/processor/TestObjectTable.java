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
import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.teiid.core.types.ArrayImpl;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.ObjectTable;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings({"nls", "unchecked"})
public class TestObjectTable {

    @Test public void testIterator() throws Exception {
        String sql = "select x.* from bqt1.smalla, objecttable('ov' passing objectvalue as ov COLUMNS x string 'teiid_row', y integer 'teiid_row_number') x"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("hello", 1),
                Arrays.asList("world", 2),
                Arrays.asList("x", 1),
                Arrays.asList("y", 2),
        };

        process(sql, expected);
    }

    @Test public void testProjection() throws Exception {
        String sql = "select y, z from bqt1.smalla, objecttable('ov' passing objectvalue as ov COLUMNS x string 'teiid_row', y integer 'teiid_row_number', z integer 'teiid_row.length') x order by x.x desc limit 1"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList(2, 1),
        };

        process(sql, expected);
    }

    @Test public void testContext() throws Exception {
        String sql = "select * from objecttable('teiid_context' COLUMNS y string 'teiid_row.userName') as X"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("user"),
        };

        process(sql, expected);
    }

    public static void process(String sql, List<?>[] expectedResults) throws Exception {
        process(sql, expectedResults, new List[] {Collections.singletonList(Arrays.asList("hello", "world")), Collections.singletonList(Arrays.asList("x", null, "y")), Collections.singletonList(null)});
    }

    public static void process(String sql, List<?>[] expectedResults, List<?>[] rows) throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT BQT1.SmallA.ObjectValue FROM BQT1.SmallA", rows);
        Properties p = new Properties();
        p.put(TransformationMetadata.ALLOWED_LANGUAGES, ObjectTable.DEFAULT_LANGUAGE);
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.exampleBQTCached().getMetadataStore(), "bqt", p);
        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata);
        helpProcess(plan, createCommandContext(), dataManager, expectedResults);
    }

    @Test public void testNull() throws Exception {
        String sql = "select * from objecttable('teiid_context' COLUMNS y string 'teiid_row.generatedKeys.toString') as X"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Collections.singletonList(null),
        };

        process(sql, expected);
    }

    @Test public void testClone() throws Exception {
        String sql = "select * from objecttable(language 'x' 'teiid_context' COLUMNS y string 'teiid_row.userName') as X"; //$NON-NLS-1$

        Command c = QueryParser.getQueryParser().parseCommand(sql);
        assertEquals("SELECT * FROM OBJECTTABLE(LANGUAGE 'x' 'teiid_context' COLUMNS y string 'teiid_row.userName') AS X", c.toString());
        assertEquals("SELECT * FROM OBJECTTABLE(LANGUAGE 'x' 'teiid_context' COLUMNS y string 'teiid_row.userName') AS X", c.clone().toString());
    }

    @Test public void testArray() throws Exception {
        String sql = "select x.* from bqt1.smalla, objecttable('ov' passing objectvalue as ov COLUMNS x string 'teiid_row', y integer 'teiid_row_number') x"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("hello", 1),
                Arrays.asList("world", 2),
                Arrays.asList("x", 1),
                Arrays.asList("y", 2),
        };

        process(sql, expected, new List<?>[] {Collections.singletonList(new String[] {"hello", "world"}), Arrays.asList(new ArrayImpl("x", "y"))});
    }

}
