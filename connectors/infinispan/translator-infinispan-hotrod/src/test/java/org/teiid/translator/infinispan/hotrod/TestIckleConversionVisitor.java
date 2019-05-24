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
package org.teiid.translator.infinispan.hotrod;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.translator.TranslatorException;

public class TestIckleConversionVisitor {

    private IckleConversionVisitor helpExecute(String query, String expected) throws Exception {
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);
        return helpExecute(mf, query, expected);
    }

    private IckleConversionVisitor helpExecute(MetadataFactory mf, String query, String expected) throws Exception {
        InfinispanExecutionFactory ef = new InfinispanExecutionFactory();
        TransformationMetadata metadata = TestProtobufMetadataProcessor.getTransformationMetadata(mf, ef);
        TranslationUtility utility = new TranslationUtility(metadata);
        Select cmd = (Select)utility.parseCommand(query);
        IckleConversionVisitor visitor = new IckleConversionVisitor(new RuntimeMetadataImpl(metadata), false);
        visitor.visitNode(cmd);
        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }
        String actual = visitor.getQuery();
        assertEquals(expected, actual);
        return visitor;
    }

    private void helpUpdate(String query, String expected) throws Exception {
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        //String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        helpUpdate(mf, query, expected);
    }

    private void helpUpdate(MetadataFactory mf, String query, String expected) throws Exception {
        InfinispanExecutionFactory ef = new InfinispanExecutionFactory();
        TransformationMetadata metadata = TestProtobufMetadataProcessor.getTransformationMetadata(mf, ef);
        TranslationUtility utility = new TranslationUtility(metadata);
        Command cmd = utility.parseCommand(query);
        InfinispanUpdateVisitor visitor = new InfinispanUpdateVisitor(new RuntimeMetadataImpl(metadata));
        visitor.append(cmd);
        String actual = null;
        if (cmd instanceof Update) {
            actual = visitor.getUpdateQuery();
        } else {
            actual = visitor.getDeleteQuery();
        }
        assertEquals(expected, actual);
    }

    @Test
    public void testSelectStar() throws Exception {
        helpExecute("select * from model.G1",
                "SELECT g1_0.e1, g1_0.e2, g1_0.e3, g1_0.e4, g1_0.e5 FROM pm1.G1 g1_0");
    }

    @Test
    public void testProjection() throws Exception {
        helpExecute("select e1, e2 from model.G1",
                "SELECT g1_0.e1, g1_0.e2 FROM pm1.G1 g1_0");
    }

    @Test
    public void testEqualityClause() throws Exception {
        helpExecute("select * from model.G1 where e1 = 1",
                "SELECT g1_0.e1, g1_0.e2, g1_0.e3, g1_0.e4, g1_0.e5 FROM pm1.G1 g1_0 WHERE g1_0.e1 = 1");
    }

    @Test
    public void testEqualityClauseWithAlias() throws Exception {
        helpExecute("select * from model.G1 as p where p.e1 = 1",
                "SELECT p.e1, p.e2, p.e3, p.e4, p.e5 FROM pm1.G1 p WHERE p.e1 = 1");
    }

    @Test
    public void testInClause() throws Exception {
        helpExecute("select e1, e2 from model.G1 where e2 IN ('foo', 'bar')",
                "SELECT g1_0.e1, g1_0.e2 FROM pm1.G1 g1_0 WHERE g1_0.e2 IN ('foo', 'bar')");
    }

    @Test
    public void testAggregate() throws Exception {
            IckleConversionVisitor v = helpExecute("select count(*) from model.G1", "SELECT COUNT(*) FROM pm1.G1 g1_0");
            assertEquals(v.getProjectedDocumentAttributes().size(), 1);
            v = helpExecute("select sum(e1) from model.G1", "SELECT SUM(g1_0.e1) FROM pm1.G1 g1_0");
            assertEquals(v.getProjectedDocumentAttributes().size(), 1);
            v = helpExecute("select min(e1) from model.G1", "SELECT MIN(g1_0.e1) FROM pm1.G1 g1_0");
            assertEquals(v.getProjectedDocumentAttributes().size(), 1);
    }

    @Test
    public void testHaving() throws Exception {
        helpExecute("select sum(e3) from model.G1 where e2 = '2' group by e1 having sum(e3) > 10",
                "SELECT SUM(g1_0.e3) FROM pm1.G1 g1_0 WHERE g1_0.e2 = '2' GROUP BY g1_0.e1 HAVING SUM(g1_0.e3) > 10.0");
    }

    @Test
    public void testOrderBy() throws Exception {
        helpExecute("select e1, e2, e3 from model.G1 where e2 IN ('foo', 'bar') order by e3",
                "SELECT g1_0.e1, g1_0.e2, g1_0.e3 FROM pm1.G1 g1_0 WHERE g1_0.e2 IN ('foo', 'bar') ORDER BY g1_0.e3");
    }

    @Test(expected=TranslatorException.class)
    public void testOrderByOnNested() throws Exception {
        helpExecute("select e1, e2 from model.G4 order by e1",
                "FROM pm1.G2 g2_1 ORDER BY g2_1.g4.e1");
    }

    @Test
    public void testUpdate() throws Exception {
        helpUpdate("update G1 set e2='bar' where e1 = 1 and e2 = 'foo'",
                "FROM pm1.G1 g1_0 WHERE g1_0.e1 = 1 AND g1_0.e2 = 'foo'");

        helpUpdate("update G4 set e2='bar' where e1 = 1 and e2 = 'foo'",
                "FROM pm1.G2 g2_1 WHERE g2_1.g4.e1 = 1 AND g2_1.g4.e2 = 'foo'");
    }

    @Test
    public void testDelete() throws Exception {
        helpUpdate("delete from G1",
                "SELECT g1_0.e1 FROM pm1.G1 g1_0");

        helpUpdate("delete from G1 where e1 > 1 or e2 = 'foo'",
                "SELECT g1_0.e1 FROM pm1.G1 g1_0 WHERE g1_0.e1 > 1 OR g1_0.e2 = 'foo'");

        helpUpdate("delete from G4 where e1 = 1 and e2 = 'foo'",
                "FROM pm1.G2 g2_1 WHERE g2_1.g4.e1 = 1 AND g2_1.g4.e2 = 'foo'");
    }

    @Test
    public void testIsNullClause() throws Exception {
        helpExecute("select e1 from model.G1 where e2 IS NULL", "SELECT g1_0.e1 FROM pm1.G1 g1_0 WHERE g1_0.e2 IS NULL");
        helpExecute("select e1 from model.G1 where e2 IS NOT NULL", "SELECT g1_0.e1 FROM pm1.G1 g1_0 WHERE g1_0.e2 IS NOT NULL");
    }

    @Test
    public void testWithEmbeddedChild() throws Exception {
        helpExecute("select * from model.G2", "FROM pm1.G2 g2_0");
        helpExecute("select * from model.G2 as p", "FROM pm1.G2 p");
        helpExecute("select * from model.G2 as p where g3_e1 = 2",
                "FROM pm1.G2 p WHERE p.g3.e1 = 2");
    }

    @Test
    public void testWithExternalChild() throws Exception {
        helpExecute("select * from model.G4", "FROM pm1.G2 g2_1");
        helpExecute("select * from model.G4 as p", "FROM pm1.G2 g2_0");
        helpExecute("select * from model.G4 where G2_e1 = 2", "FROM pm1.G2 g2_1 WHERE g2_1.e1 = 2");
        IckleConversionVisitor visitor = helpExecute("select * from model.G4 as p where p.G2_e1 = 2",
                "FROM pm1.G2 g2_0 WHERE g2_0.e1 = 2");
        assertArrayEquals(new String[] { "pm1.G2/pm1.G4/e1", "pm1.G2/pm1.G4/e2", "e1" },
                visitor.getProjectedDocumentAttributes().keySet().toArray(new String[2]));
    }

    @Test
    public void testJoins() throws Exception {
        helpExecute("select g2.e1, g4.e1 from model.G2 g2 JOIN model.G4 g4 ON g2.e1 = g4.g2_e1",
                "FROM pm1.G2 g2"); // where is not generated because both columns as one and same

        helpExecute("select g2.e1, g4.e1 from model.G2 g2 JOIN model.G4 g4 ON g2.e1 = g4.g2_e1 "
                + "WHERE g2.e2 = 'foo' AND g4.e2 = 'bar'",
                "FROM pm1.G2 g2 WHERE g2.e2 = 'foo' AND g2.g4.e2 = 'bar'");

    }
}
