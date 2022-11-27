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
package org.teiid.translator.jpa;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Select;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestJSelectJPQLVisitor {
    private JPA2ExecutionFactory jpaTranslator;
    private TranslationUtility utility;

    @Before
    public void setUp() throws Exception {
        jpaTranslator = new JPA2ExecutionFactory();
        jpaTranslator.start();

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("sakila.ddl")), "sakila", "sakila");
        utility = new TranslationUtility(metadata);

    }

    private void helpExecute(String query, String expected) throws Exception {
        Select cmd = (Select)this.utility.parseCommand(query);
        String jpaCommand = JPQLSelectVisitor.getJPQLString(cmd, jpaTranslator, utility.createRuntimeMetadata());
        assertEquals(expected, jpaCommand);
    }

    @Test
    public void testProjectionBasedJoin() throws Exception {
        helpExecute("select * from customer as c", "SELECT c.customer_id, J_0.store_id, c.first_name, c.last_name, c.email, J_1.address_id, c.active, c.create_date, c.last_update FROM customer AS c LEFT OUTER JOIN c.store AS J_0 LEFT OUTER JOIN c.address AS J_1");
    }

    @Test
    public void testExplicitJoinJoin() throws Exception {
        helpExecute("select c.first_name, c.last_name, a.address_id FROM customer c join address a on c.address_id=a.address_id",
                "SELECT c.first_name, c.last_name, a.address_id FROM customer AS c INNER JOIN c.address AS a");
    }

    @Test
    public void testSimpleSelect() throws Exception {
        helpExecute("select c.first_name, c.last_name FROM customer c order by last_name",
                "SELECT c.first_name, c.last_name FROM customer AS c ORDER BY c.last_name");
    }

    @Test
    public void testFunctionsSelect() throws Exception {
        helpExecute("select concat(lcase(first_name), last_Name) from customer as c",
                "SELECT concat(lower(c.first_name), c.last_name) FROM customer AS c");
    }

    @Test
    public void testRightJoinRewriteSelect() throws Exception {
        helpExecute("select c.first_name, c.last_name, c.address_id, a.phone from customer c right join address a on c.address_Id=a.address_Id",
                "SELECT c.first_name, c.last_name, a.address_id, a.phone FROM customer AS c RIGHT OUTER JOIN c.address AS a");
    }

    @Test
    public void testRightandinnerJoinRewriteSelect() throws Exception {
        helpExecute("select c.first_name, c.last_name, a.address_id, a.phone, ci.city from customer c join address a on c.address_Id=a.address_Id right join city ci on ci.city_id = a.city_id where c.last_Name='MYERS' OR c.last_Name='TALBERT' order by c.first_Name",
                "SELECT c.first_name, c.last_name, a.address_id, a.phone, ci.city FROM customer AS c INNER JOIN c.address AS a RIGHT OUTER JOIN a.city AS ci WHERE c.last_name IN ('TALBERT', 'MYERS') ORDER BY c.first_name");
    }

    @Test
    public void testRelationOverlappingIdsAndOverlappingTypes() throws Exception {
        helpExecute("select * from thing as t",
                "SELECT t.id, J_0.id, J_1.id, J_2.id FROM thing AS t LEFT OUTER JOIN t.parent AS J_0 LEFT OUTER JOIN t.thing_type AS J_1 LEFT OUTER JOIN t.thing_subtype AS J_2");
    }

    @Test
    public void testMultiLevelJoin() throws Exception {
        helpExecute("select t.id, t.thing_type_id, tp.id, tp.thing_type_id, tpp.id " +
                        "from thing as t " +
                        "join thing as tp on t.parent_id = tp.id " +
                        "join thing as tpp on tp.parent_id = tpp.id",
                "SELECT t.id, J_0.id, tp.id, J_1.id, tpp.id " +
                        "FROM thing AS t " +
                        "INNER JOIN t.parent AS tp " +
                        "INNER JOIN tp.parent AS tpp " +
                        "LEFT OUTER JOIN t.thing_type AS J_0 " +
                        "LEFT OUTER JOIN tp.thing_type AS J_1");
    }

    @Test
    public void testEmbedded() throws Exception {
        helpExecute("select * from thing_with_embedded as t",
                "SELECT t.id, t.embedded.prop1, t.embedded.prop2 " +
                        "FROM thing_with_embedded AS t");
    }

    // needs one with composite PK
}
