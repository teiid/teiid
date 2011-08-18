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

package org.teiid.resource.adapter.file;

import static org.junit.Assert.*;

import java.io.File;

import javax.resource.ResourceException;

import org.junit.Test;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.FileConnection;

@SuppressWarnings("nls")
public class TestFileConnection {
	
	@Test public void testFileMapping() throws Exception {
		FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
		fmcf.setParentDirectory("foo");
		fmcf.setFileMapping("x=y,z=a");
		BasicConnectionFactory bcf = fmcf.createConnectionFactory();
		FileConnection fc = (FileConnection)bcf.getConnection();
		File f = fc.getFile("x");
		assertEquals("foo/y", f.getPath());
		f = fc.getFile("n");
		assertEquals("foo/n", f.getPath());
	}
	
	@Test(expected=ResourceException.class) public void testParentPaths() throws Exception {
		FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
		fmcf.setParentDirectory("foo");
		fmcf.setAllowParentPaths(false);
		BasicConnectionFactory bcf = fmcf.createConnectionFactory();
		FileConnection fc = (FileConnection)bcf.getConnection();
		fc.getFile("../x");
	}
	
	@Test public void testParentPaths1() throws Exception {
		FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
		fmcf.setParentDirectory("foo");
		fmcf.setAllowParentPaths(true);
		BasicConnectionFactory bcf = fmcf.createConnectionFactory();
		FileConnection fc = (FileConnection)bcf.getConnection();
		fc.getFile("../x");
	}

}
