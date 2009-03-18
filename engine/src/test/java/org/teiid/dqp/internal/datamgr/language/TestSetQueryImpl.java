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

package org.teiid.dqp.internal.datamgr.language;

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IOrderByItem;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.language.ISetQuery;
import org.teiid.dqp.internal.datamgr.language.ElementImpl;
import org.teiid.dqp.internal.datamgr.language.FromImpl;
import org.teiid.dqp.internal.datamgr.language.GroupImpl;
import org.teiid.dqp.internal.datamgr.language.OrderByImpl;
import org.teiid.dqp.internal.datamgr.language.OrderByItemImpl;
import org.teiid.dqp.internal.datamgr.language.QueryImpl;
import org.teiid.dqp.internal.datamgr.language.SelectImpl;
import org.teiid.dqp.internal.datamgr.language.SelectSymbolImpl;
import org.teiid.dqp.internal.datamgr.language.SetQueryImpl;

import junit.framework.TestCase;

import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.SetQuery.Operation;


/** 
 * @since 4.2
 */
public class TestSetQueryImpl extends TestCase {

    public static Query helpExampleQuery() {
        return new Query(TestSelectImpl.helpExample(true),
                         TestFromImpl.helpExample(),
                         TestCompoundCriteriaImpl.helpExample(CompoundCriteria.AND),
                         TestGroupByImpl.helpExample(),
                         TestCompoundCriteriaImpl.helpExample(CompoundCriteria.AND),
                         TestOrderByImpl.helpExample(),
                         null);
    }
    
    public static SetQuery helpExampleSetQuery() {
        SetQuery setQuery = new SetQuery(Operation.UNION);
        setQuery.setAll(false);
        setQuery.setLeftQuery(helpExampleQuery());
        setQuery.setRightQuery(helpExampleQuery());
        setQuery.setOrderBy(TestOrderByImpl.helpExample());
        return setQuery;
    }
        
    public static SetQueryImpl example() throws Exception {
        return (SetQueryImpl)TstLanguageBridgeFactory.factory.translate(helpExampleSetQuery());
    }
    
    public static ISetQuery example2() throws Exception {
        IGroup group = new GroupImpl("ted", null, null); //$NON-NLS-1$
        IElement element = new ElementImpl(group, "nugent", null, String.class); //$NON-NLS-1$
        ISelectSymbol symbol = new SelectSymbolImpl("nugent",element); //$NON-NLS-1$
        List symbols = new ArrayList();
        symbols.add(symbol);
        SelectImpl select = new SelectImpl(symbols, false);        
        List items = new ArrayList();
        items.add(group);
        FromImpl from = new FromImpl(items);
        
        IGroup group2 = new GroupImpl("dave", null, null); //$NON-NLS-1$
        IElement element2 = new ElementImpl(group2, "barry", null, String.class); //$NON-NLS-1$
        ISelectSymbol symbol2 = new SelectSymbolImpl("barry", element2); //$NON-NLS-1$
        List symbols2 = new ArrayList();
        symbols2.add(symbol2);
        SelectImpl select2 = new SelectImpl(symbols2, false);
        
        List items2 = new ArrayList();
        items2.add(group2);
        FromImpl from2 = new FromImpl(items2);
        
        IQuery secondQuery = new QueryImpl(select2, from2, null, null, null, null);
        
        IQuery query = new QueryImpl(select, from, null, null, null, null);
        
        ISetQuery setQuery = new SetQueryImpl();
        setQuery.setOperation(ISetQuery.Operation.UNION);
        setQuery.setAll(true);
        setQuery.setLeftQuery(query);
        setQuery.setRightQuery(secondQuery);
        
        return setQuery;
    }
    
    public static ISetQuery example3() throws Exception {
        ISetQuery union = example2();
        
        List items = new ArrayList();
        IElement element = (IElement) ((ISelectSymbol) union.getProjectedQuery().getSelect().getSelectSymbols().get(0)).getExpression();
        items.add(new OrderByItemImpl("ted.nugent", IOrderByItem.ASC, element)); //$NON-NLS-1$
        OrderByImpl orderBy = new OrderByImpl(items);
        
        union.setOrderBy(orderBy);
        return union;
    }
    
    public void testNestedSetQuery() throws Exception {
        SetQuery query = new SetQuery(SetQuery.Operation.EXCEPT, true, helpExampleQuery(), helpExampleQuery());
        
        ISetQuery setQuery = TstLanguageBridgeFactory.factory.translate(query);
        
    }
    
    public void testGetSelect() throws Exception {
        assertNotNull(example().getProjectedQuery().getSelect());
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
