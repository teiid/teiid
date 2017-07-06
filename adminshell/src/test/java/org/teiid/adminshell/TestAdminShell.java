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
