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

package org.teiid.dqp.internal.cache;

import java.util.ArrayList;
import java.util.List;

import org.teiid.dqp.internal.cache.CacheID;

import junit.framework.TestCase;

public class TestCacheID extends TestCase{
    public TestCacheID(final String name) {
        super(name);
    }
    
    public void testIDEquals1(){
    	CacheID id1 = new CacheID("12345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	CacheID id2 = new CacheID("12345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	assertEquals(id1, id2);
    }
    
    public void testIDEquals2(){
    	List variables1 = new ArrayList();
    	variables1.add(new Integer(6));
    	variables1.add("aaa"); //$NON-NLS-1$
    	CacheID id1 = new CacheID("12345", "select * from table1",variables1);  //$NON-NLS-1$//$NON-NLS-2$
    	List variables2 = new ArrayList();
    	variables2.add(new Integer(6));
    	variables2.add("aaa"); //$NON-NLS-1$
    	CacheID id2 = new CacheID("12345", "select * from table1",variables2);  //$NON-NLS-1$//$NON-NLS-2$
    	assertEquals(id1, id2);
    }
    
    public void testIDEquals3(){
    	CacheID id1 = new CacheID("12345", "select * from table1", null);  //$NON-NLS-1$//$NON-NLS-2$
    	CacheID id2 = new CacheID("12345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	assertEquals(id1, id2);
    }
    
    public void testIDNotEquals1(){
    	CacheID id1 = new CacheID("12345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	CacheID id2 = new CacheID("2345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	assertTrue(!id1.equals(id2));
    }
    
    public void testIDNotEquals2(){
    	CacheID id1 = new CacheID("12345", "select * from table1");  //$NON-NLS-1$//$NON-NLS-2$
    	CacheID id2 = new CacheID("12345", "select * from table2");  //$NON-NLS-1$//$NON-NLS-2$
    	assertTrue(!id1.equals(id2));
    }
    
    public void testIDNotEquals3(){
    	List variables1 = new ArrayList();
    	variables1.add(new Integer(6));
    	variables1.add("aba"); //$NON-NLS-1$
    	CacheID id1 = new CacheID("12345", "select * from table1",variables1);  //$NON-NLS-1$//$NON-NLS-2$
    	List variables2 = new ArrayList();
    	variables2.add(new Integer(6));
    	variables2.add("aaa"); //$NON-NLS-1$
    	CacheID id2 = new CacheID("12345", "select * from table1",variables2);  //$NON-NLS-1$//$NON-NLS-2$
    	assertTrue(!id1.equals(id2));
    }
    
    public void testIDNotEquals4(){
    	List variables1 = null;
    	CacheID id1 = new CacheID("12345", "select * from table1",variables1);  //$NON-NLS-1$//$NON-NLS-2$
    	List variables2 = new ArrayList();
    	variables2.add(new Integer(6));
    	variables2.add("aaa"); //$NON-NLS-1$
    	CacheID id2 = new CacheID("12345", "select * from table1",variables2);  //$NON-NLS-1$//$NON-NLS-2$
    	assertTrue(!id1.equals(id2));
    }
    
    public void testIDNotEquals5(){
    	List variables1 = new ArrayList();
    	variables1.add(new Integer(6));
    	variables1.add("aaa"); //$NON-NLS-1$
    	variables1.add("aaa"); //$NON-NLS-1$
    	CacheID id1 = new CacheID("12345", "select * from table1",variables1);  //$NON-NLS-1$//$NON-NLS-2$
    	List variables2 = new ArrayList();
    	variables2.add(new Integer(6));
    	variables2.add("aaa"); //$NON-NLS-1$
    	CacheID id2 = new CacheID("12345", "select * from table1",variables2);  //$NON-NLS-1$//$NON-NLS-2$
    	assertTrue(!id1.equals(id2));
    }
}
