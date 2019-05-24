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
