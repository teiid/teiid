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

package org.teiid.query.parser;

import static org.teiid.query.parser.TestParser.*;

import java.util.Arrays;

import org.junit.Test;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.AllSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;

public class TestLimitParsing {
	
    @Test public void testLimit() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(null, new Constant(new Integer(100))));
        helpTest("Select * from a limit 100", "SELECT * FROM a LIMIT 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testLimitWithOffset() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(new Integer(50)), new Constant(new Integer(100))));
        helpTest("Select * from a limit 50,100", "SELECT * FROM a LIMIT 50, 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testLimitWithReferences1() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Reference(0), new Constant(new Integer(100))));
        helpTest("Select * from a limit ?,100", "SELECT * FROM a LIMIT ?, 100", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testLimitWithReferences2() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(new Integer(50)), new Reference(0)));
        helpTest("Select * from a limit 50,?", "SELECT * FROM a LIMIT 50, ?", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testLimitWithReferences3() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Reference(0), new Reference(1)));
        helpTest("Select * from a limit ?,?", "SELECT * FROM a LIMIT ?, ?", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testSetQueryLimit() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        SetQuery setQuery = new SetQuery(Operation.UNION, true, query, query);
        setQuery.setLimit(new Limit(new Reference(0), new Reference(1)));
        helpTest("Select * from a union all Select * from a limit ?,?", "SELECT * FROM a UNION ALL SELECT * FROM a LIMIT ?, ?", setQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testOffset() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Reference(0), null));
        helpTest("Select * from a offset ? rows", "SELECT * FROM a OFFSET ? ROWS", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testFetchFirst() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(null, new Constant(2)));
        helpTest("Select * from a fetch first 2 rows only", "SELECT * FROM a LIMIT 2", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testFetchFirstRow() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(null, new Constant(1)));
        helpTest("Select * from a fetch first row only", "SELECT * FROM a LIMIT 1", query); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testOffsetFetch() {
        Query query = new Query();
        Select select = new Select(Arrays.asList(new AllSymbol()));
        From from = new From(Arrays.asList(new UnaryFromClause(new GroupSymbol("a")))); //$NON-NLS-1$
        query.setSelect(select);
        query.setFrom(from);
        query.setLimit(new Limit(new Constant(2), new Constant(5)));
        helpTest("Select * from a offset 2 rows fetch first 5 rows only", "SELECT * FROM a LIMIT 2, 5", query); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
