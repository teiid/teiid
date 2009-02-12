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

package com.metamatrix.query.sql.lang;

import java.util.ArrayList;

import junit.framework.TestCase;

import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.unittest.FakeMetadataStore;



/** 
 * @since 4.3
 */
public class TestBulkInsert extends TestCase{
    public TestBulkInsert(String name) { 
        super(name);
    }   

         
    public void test1() throws Exception{
        FakeMetadataObject pm1 = FakeMetadataFactory.createPhysicalModel("pm1"); //$NON-NLS-1$
        FakeMetadataObject pm1g = FakeMetadataFactory.createPhysicalGroup("pm1.g", pm1); //$NON-NLS-1$
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(pm1);
        store.addObject(pm1g);
        GroupSymbol g = new GroupSymbol("g");//$NON-NLS-1$
        g.setMetadataID(pm1g);
        
        BulkInsert bulkInsert = new BulkInsert(g, new ArrayList());
        assertEquals(2, bulkInsert.updatingModelCount(new FakeMetadataFacade(store)));
    }

}
