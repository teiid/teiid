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

import static org.teiid.query.parser.TestParser.*;

import java.util.Arrays;

import org.junit.Test;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.Reference;

public class TestLimitParsing {

    @Test public void testLimit() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(null, new Constant(new Integer(100))));
        helpTest("Select * from a limit 100", "SELECT * FROM a LIMIT 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLimitWithOffset() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(new Integer(50)), new Constant(new Integer(100))));
        helpTest("Select * from a limit 50,100", "SELECT * FROM a LIMIT 50, 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLimitWithReferences1() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Reference(0), new Constant(new Integer(100))));
        helpTest("Select * from a limit ?,100", "SELECT * FROM a LIMIT ?, 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLimitWithReferences2() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(new Integer(50)), new Reference(0)));
        helpTest("Select * from a limit 50,?", "SELECT * FROM a LIMIT 50, ?", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLimitWithReferences3() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Reference(0), new Reference(1)));
        helpTest("Select * from a limit ?,?", "SELECT * FROM a LIMIT ?, ?", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSetQueryLimit() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        SetQuery setQuery = new SetQuery(Operation.UNION, true, query, query);
        setQuery.setLimit(new Limit(new Reference(0), new Reference(1)));
        helpTest("Select * from a union all Select * from a limit ?,?", "SELECT * FROM a UNION ALL SELECT * FROM a LIMIT ?, ?", setQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testOffset() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Reference(0), null));
        helpTest("Select * from a offset ? rows", "SELECT * FROM a OFFSET ? ROWS", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFetchFirst() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(null, new Constant(2)));
        helpTest("Select * from a fetch first 2 rows only", "SELECT * FROM a LIMIT 2", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFetchFirstRow() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(null, new Constant(1)));
        helpTest("Select * from a fetch first row only", "SELECT * FROM a LIMIT 1", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testOffsetFetch() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(2), new Constant(5)));
        helpTest("Select * from a offset 2 rows fetch first 5 rows only", "SELECT * FROM a LIMIT 2, 5", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLimitWithOffsetKeyword() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new MultipleElementSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(new Integer(50)), new Constant(new Integer(100))));
        helpTest("Select * from a limit 100 offset 50", "SELECT * FROM a LIMIT 50, 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
