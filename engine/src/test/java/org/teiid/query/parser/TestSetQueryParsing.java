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

package org.teiid.query.parser;

import org.junit.Test;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;

public class TestSetQueryParsing {

    // ======================= UNION ================================================

    /** SELECT a FROM g UNION select b from h*/
    @Test public void testUnion(){
        SetQuery setQuery = exampleSetQuery(Operation.UNION);

        TestParser.helpTest("SELECT a FROM g UNION select b from h",  //$NON-NLS-1$
                 "SELECT a FROM g UNION SELECT b FROM h",  //$NON-NLS-1$
                 setQuery);
    }

    @Test public void testExcept(){
        SetQuery setQuery = exampleSetQuery(Operation.EXCEPT);

        TestParser.helpTest("SELECT a FROM g except select b from h",  //$NON-NLS-1$
                 "SELECT a FROM g EXCEPT SELECT b FROM h",  //$NON-NLS-1$
                 setQuery);
    }

    @Test public void testIntersect(){
        SetQuery setQuery = exampleSetQuery(Operation.INTERSECT);

        TestParser.helpTest("SELECT a FROM g intersect select b from h",  //$NON-NLS-1$
                 "SELECT a FROM g INTERSECT SELECT b FROM h",  //$NON-NLS-1$
                 setQuery);
    }

    @Test public void testIntersectPresedence(){

        SetQuery setQuery = new SetQuery(Operation.INTERSECT, false, createTestQuery("t2"), createTestQuery("t3")); //$NON-NLS-1$  //$NON-NLS-2$

        setQuery = new SetQuery(Operation.EXCEPT, false, createTestQuery("t1"), setQuery); //$NON-NLS-1$

        TestParser.helpTest("select * from t1 EXCEPT select * from t2 INTERSECT select * from t3",  //$NON-NLS-1$
                 "SELECT * FROM t1 EXCEPT (SELECT * FROM t2 INTERSECT SELECT * FROM t3)",  //$NON-NLS-1$
                 setQuery);
    }

    private Query createTestQuery(String group) {
        GroupSymbol g = new GroupSymbol(group);
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());

        Query query1 = new Query();
        query1.setSelect(select);
        query1.setFrom(from);
        return query1;
    }

    private SetQuery exampleSetQuery(Operation op) {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$

        Query query1 = new Query();
        query1.setSelect(select);
        query1.setFrom(from);

        g = new GroupSymbol("h"); //$NON-NLS-1$
        from = new From();
        from.addGroup(g);

        select = new Select();
        select.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$

        Query query2 = new Query();
        query2.setSelect(select);
        query2.setFrom(from);

        SetQuery setQuery = new SetQuery(op);
        setQuery.setAll(false);
        setQuery.setLeftQuery(query1);
        setQuery.setRightQuery(query2);
        return setQuery;
    }

    /** SELECT a FROM g UNION ALL select b from h*/
    @Test public void testUnionAll(){
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$

        Query query1 = new Query();
        query1.setSelect(select);
        query1.setFrom(from);

        g = new GroupSymbol("h"); //$NON-NLS-1$
        from = new From();
        from.addGroup(g);

        select = new Select();
        select.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$

        Query query2 = new Query();
        query2.setSelect(select);
        query2.setFrom(from);

        SetQuery setQuery = new SetQuery(Operation.UNION);
        setQuery.setAll(true);
        setQuery.setLeftQuery(query1);
        setQuery.setRightQuery(query2);

        TestParser.helpTest("SELECT a FROM g UNION ALL select b from h",  //$NON-NLS-1$
                 "SELECT a FROM g UNION ALL SELECT b FROM h",  //$NON-NLS-1$
                 setQuery);
    }

    /** select c1 from g1 union select c2 from g2 union select c3 from g3*/
    @Test public void testTwoUnions(){
        SetQuery setQuery = new SetQuery(Operation.UNION);
        setQuery.setAll(false);
        GroupSymbol g = new GroupSymbol("g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("c1")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        setQuery.setLeftQuery(query);

        g = new GroupSymbol("g2"); //$NON-NLS-1$
        from = new From();
        from.addGroup(g);

        select = new Select();
        select.addSymbol(new ElementSymbol("c2")); //$NON-NLS-1$

        query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        setQuery.setRightQuery(query);

        g = new GroupSymbol("g3"); //$NON-NLS-1$
        from = new From();
        from.addGroup(g);

        select = new Select();
        select.addSymbol(new ElementSymbol("c3")); //$NON-NLS-1$

        query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        setQuery = new SetQuery(Operation.UNION, false, setQuery, query);

        TestParser.helpTest("select c1 from g1 union select c2 from g2 union select c3 from g3",  //$NON-NLS-1$
                 "SELECT c1 FROM g1 UNION SELECT c2 FROM g2 UNION SELECT c3 FROM g3",  //$NON-NLS-1$
                 setQuery);
    }

    /** select c1 from g1 union select c2 from g2 union all select c3 from g3 union select c4 from g4 */
    @Test public void testThreeUnions(){
        SetQuery setQuery = new SetQuery(Operation.UNION);
        setQuery.setAll(false);
        GroupSymbol g = new GroupSymbol("g1"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("c1")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        setQuery.setLeftQuery(query);

        g = new GroupSymbol("g2"); //$NON-NLS-1$
        from = new From();
        from.addGroup(g);

        select = new Select();
        select.addSymbol(new ElementSymbol("c2")); //$NON-NLS-1$

        query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        setQuery.setRightQuery(query);

        g = new GroupSymbol("g3"); //$NON-NLS-1$
        from = new From();
        from.addGroup(g);

        select = new Select();
        select.addSymbol(new ElementSymbol("c3")); //$NON-NLS-1$

        query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        setQuery = new SetQuery(SetQuery.Operation.UNION, true, setQuery, query);

        g = new GroupSymbol("g4"); //$NON-NLS-1$
        from = new From();
        from.addGroup(g);

        select = new Select();
        select.addSymbol(new ElementSymbol("c4")); //$NON-NLS-1$

        query = new Query();
        query.setSelect(select);
        query.setFrom(from);

        setQuery = new SetQuery(SetQuery.Operation.UNION, false, setQuery, query);

        TestParser.helpTest("select c1 from g1 union select c2 from g2 union all select c3 from g3 union select c4 from g4",  //$NON-NLS-1$
                 "SELECT c1 FROM g1 UNION SELECT c2 FROM g2 UNION ALL SELECT c3 FROM g3 UNION SELECT c4 FROM g4",  //$NON-NLS-1$
                 setQuery);
    }

    @Test public void testUnionWithLimit(){
        SetQuery setQuery = exampleSetQuery(Operation.UNION);
        setQuery.setLimit(new Limit(null, new Constant(1)));

        TestParser.helpTest("SELECT a FROM g UNION select b from h LIMIT 1",  //$NON-NLS-1$
                 "SELECT a FROM g UNION SELECT b FROM h LIMIT 1",  //$NON-NLS-1$
                 setQuery);
    }

    @Test public void testMultipleValues(){
        SetQuery setQuery = new SetQuery(Operation.UNION);
        setQuery.setAll(true);

        Select select = new Select();
        select.addSymbol(new ElementSymbol("c1")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);

        setQuery.setLeftQuery(query);

        select = new Select();
        select.addSymbol(new ElementSymbol("c2")); //$NON-NLS-1$

        query = new Query();
        query.setSelect(select);

        setQuery.setRightQuery(query);

        TestParser.helpTest("values (c1), (c2)",  //$NON-NLS-1$
                 "SELECT c1 UNION ALL SELECT c2",  //$NON-NLS-1$
                 setQuery);
    }

    @Test public void testSingleValue(){
        Select select = new Select();
        select.addSymbol(new ElementSymbol("c1")); //$NON-NLS-1$
        select.addSymbol(new Constant("x")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);

        TestParser.helpTest("values (c1, 'x')",  //$NON-NLS-1$
                 "SELECT c1, 'x'",  //$NON-NLS-1$
                 query);
    }

}
