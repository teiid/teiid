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
package org.teiid.translator.object;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.teiid.translator.object.testdata.annotated.TestObjectConnection;

@SuppressWarnings("nls")
public class TestCacheNameProxy {
		
	@Test public void testFactory() throws Exception {
		
		Map<Object, Object> alias = new HashMap<Object, Object>();
		CacheNameProxy cnp = new CacheNameProxy("pc", "sc", "ac");
		ClassRegistry cr = new ClassRegistry();

	
		TestObjectConnection toc = new TestObjectConnection(new HashMap<Object, Object>(), new HashMap<Object, Object>(), alias, cr, cnp);

		cnp.ensureCacheNames(toc);
		
		assertNotNull(cnp.getAliasCacheName());
		assertNotNull(cnp.getPrimaryCacheKey());
		assertNotNull(cnp.getStageCacheKey());
		
		assertEquals("pc",cnp.getPrimaryCacheAliasName(toc));
		assertEquals("sc",cnp.getStageCacheAliasName(toc));
		
		assertEquals("pc", alias.get("pc"));
		assertEquals("sc", alias.get("sc"));
		
		assertTrue(cnp.isMaterialized());
		
		cnp.swapCacheNames(toc);
		
		assertEquals("sc",cnp.getPrimaryCacheAliasName(toc));
		assertEquals("pc",cnp.getStageCacheAliasName(toc));

		cnp.swapCacheNames(toc);
		
		assertEquals("pc",cnp.getPrimaryCacheAliasName(toc));
		assertEquals("sc",cnp.getStageCacheAliasName(toc));
		
		cnp.ensureCacheNames(toc);

		assertEquals("pc",cnp.getPrimaryCacheAliasName(toc));
		assertEquals("sc",cnp.getStageCacheAliasName(toc));
	
	}	


}
