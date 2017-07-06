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
		assertEquals("foo" + File.separator + "y", f.getPath());
		f = fc.getFile("n");
		assertEquals("foo" + File.separator + "n", f.getPath());
	}
	
	@Test(expected=ResourceException.class) public void testParentPaths() throws Exception {
		FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
		fmcf.setParentDirectory("foo");
		fmcf.setAllowParentPaths(false);
		BasicConnectionFactory bcf = fmcf.createConnectionFactory();
		FileConnection fc = (FileConnection)bcf.getConnection();
		fc.getFile(".." + File.separator + "x");
	}
	
	@Test public void testParentPaths1() throws Exception {
		FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
		fmcf.setParentDirectory("foo");
		fmcf.setAllowParentPaths(true);
		BasicConnectionFactory bcf = fmcf.createConnectionFactory();
		FileConnection fc = (FileConnection)bcf.getConnection();
		fc.getFile(".." + File.separator + "x");
	}

}
