/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General public static
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General public static License for more details.
 * 
 * You should have received a copy of the GNU Lesser General public static
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.adminshell;

import java.io.FileNotFoundException;
import java.io.InputStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestAdminShell {
	
	private static Admin admin;

	@BeforeClass public static void oneTimeSetUp() {
		admin = Mockito.mock(Admin.class);
		AdminShell.internalAdmin = admin; 
	}
	
	@AfterClass public static void oneTimeTearDown() {
		AdminShell.internalAdmin = null;
	}
	
	@Test public void testDeployVDB() throws AdminException, FileNotFoundException {
		AdminShell.deploy(UnitTestUtil.getTestDataPath() + "/foo/bar.txt");
		Mockito.verify(admin).deploy(Mockito.eq("bar.txt"), (InputStream)Mockito.anyObject());
	}
	
}
