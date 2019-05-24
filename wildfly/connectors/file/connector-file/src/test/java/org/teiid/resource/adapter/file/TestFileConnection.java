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

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.file.VirtualFile;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestFileConnection {

    @Test public void testFileMapping() throws Exception {
        FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
        fmcf.setParentDirectory("foo");
        fmcf.setFileMapping("x=y,z=a");
        BasicConnectionFactory bcf = fmcf.createConnectionFactory();
        FileConnectionImpl fc = (FileConnectionImpl)bcf.getConnection();
        File f = fc.getFile("x");
        assertEquals("foo" + File.separator + "y", f.getPath());
        f = fc.getFile("n");
        assertEquals("foo" + File.separator + "n", f.getPath());
    }

    @Test(expected=TranslatorException.class) public void testParentPaths() throws Exception {
        FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
        fmcf.setParentDirectory("foo");
        fmcf.setAllowParentPaths(false);
        BasicConnectionFactory bcf = fmcf.createConnectionFactory();
        FileConnectionImpl fc = (FileConnectionImpl)bcf.getConnection();
        fc.getFile(".." + File.separator + "x");
    }

    @Test public void testParentPaths1() throws Exception {
        FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
        fmcf.setParentDirectory("foo");
        fmcf.setAllowParentPaths(true);
        BasicConnectionFactory bcf = fmcf.createConnectionFactory();
        FileConnectionImpl fc = (FileConnectionImpl)bcf.getConnection();
        fc.getFile(".." + File.separator + "x");
    }

    @Test public void testFileGlob() throws Exception {
        FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
        fmcf.setParentDirectory(UnitTestUtil.getTestDataPath());
        BasicConnectionFactory bcf = fmcf.createConnectionFactory();
        FileConnectionImpl fc = (FileConnectionImpl)bcf.getConnection();
        VirtualFile[] files = fc.getFiles("*.txt");
        assertEquals(1, files.length);
        assertEquals("foo.txt", files[0].getName());

        files = fc.getFiles("*.911");
        assertEquals(0, files.length);
    }

    @Test public void testFileDoesntExist() throws Exception {
        FileManagedConnectionFactory fmcf = new FileManagedConnectionFactory();
        fmcf.setParentDirectory(UnitTestUtil.getTestDataPath()+"xyz");
        BasicConnectionFactory bcf = fmcf.createConnectionFactory();
        FileConnectionImpl fc = (FileConnectionImpl)bcf.getConnection();
        VirtualFile[] files = fc.getFiles("*.txt");
        assertNull(files);
    }

}
