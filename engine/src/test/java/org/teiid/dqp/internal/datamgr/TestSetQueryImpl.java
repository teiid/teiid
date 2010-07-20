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

package org.teiid.dqp.internal.datamgr;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.Select;
import org.teiid.language.SetQuery;
import org.teiid.language.SortSpecification;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.query.sql.lang.SetQuery.Operation;



/** 
 * @since 4.2
 */
public class TestSetQueryImpl extends TestCase {

    public static org.teiid.query.sql.lang.SetQuery helpExampleSetQuery() {
        org.teiid.query.sql.lang.SetQuery setQuery = new org.teiid.query.sql.lang.SetQuery(Operation.UNION);
        setQuery.setAll(false);
        setQuery.setLeftQuery(TestQueryImpl.helpExample(true));
        setQuery.setRightQuery(TestQueryImpl.helpExample(true));
        setQuery.setOrderBy(TestOrderByImpl.helpExample());
        return setQuery;
    }
        
    public static SetQuery example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExampleSetQuery());
    }
    
    public static SetQuery example2() throws Exception {
        NamedTable group = new NamedTable("ted", null, null); //$NON-NLS-1$
        ColumnReference element = new ColumnReference(group, "nugent", null, String.class); //$NON-NLS-1$
        DerivedColumn symbol = new DerivedColumn(null,element); 
        List symbols = new ArrayList();
        symbols.add(symbol);
        List items = new ArrayList();
        items.add(group);
        
        NamedTable group2 = new NamedTable("dave", null, null); //$NON-NLS-1$
        ColumnReference element2 = new ColumnReference(group2, "barry", null, String.class); //$NON-NLS-1$
        DerivedColumn symbol2 = new DerivedColumn(null, element2);
        List symbols2 = new ArrayList();
        symbols2.add(symbol2);
        
        List items2 = new ArrayList();
        items2.add(group2);
        
        Select secondQuery = new Select(symbols2, false, items2, null, null, null, null);
        
        Select query = new Select(symbols, false, items, null, null, null, null);
        
        SetQuery setQuery = new SetQuery();
        setQuery.setOperation(SetQuery.Operation.UNION);
        setQuery.setAll(true);
        setQuery.setLeftQuery(query);
        setQuery.setRightQuery(secondQuery);
        
        return setQuery;
    }
    
    public static SetQuery example3() throws Exception {
        SetQuery union = example2();
        
        List<SortSpecification> items = new ArrayList<SortSpecification>();
        items.add(new SortSpecification(Ordering.ASC, new ColumnReference(null, "nugent", null, DataTypeManager.DefaultDataClasses.STRING))); //$NON-NLS-1$
        OrderBy orderBy = new OrderBy(items);
        
        union.setOrderBy(orderBy);
        return union;
    }
    
    public void testNestedSetQuery() throws Exception {
        org.teiid.query.sql.lang.SetQuery query = new org.teiid.query.sql.lang.SetQuery(org.teiid.query.sql.lang.SetQuery.Operation.EXCEPT, true, helpExampleSetQuery(), helpExampleSetQuery());
        
        SetQuery setQuery = TstLanguageBridgeFactory.factory.translate(query);
        assertTrue(setQuery.getLeftQuery() instanceof SetQuery);
        assertTrue(setQuery.getRightQuery() instanceof SetQuery);
    }
    
    public void testGetSelect() throws Exception {
        assertNotNull(example().getProjectedQuery().getDerivedColumns());
    }

    public void testGetFrom() throws Exception {
        assertNotNull(example().getProjectedQuery().getFrom());
    }

    public void testGetWhere() throws Exception {
        assertNotNull(example().getProjectedQuery().getWhere());
    }

    public void testGetGroupBy() throws Exception {
        assertNotNull(example().getProjectedQuery().getGroupBy());
    }

    public void testGetHaving() throws Exception {
        assertNotNull(example().getProjectedQuery().getHaving());
    }

    public void testGetOrderBy() throws Exception {
        assertNotNull(example().getOrderBy());
    }
    
    public void testGetUnionAllFlag() throws Exception {
        assertEquals(false, example().isAll());
    }

}
