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

package com.metamatrix.core.util;

import java.io.File;

import junit.framework.TestCase;

public class TestTempDirectory extends TestCase {
    private TempDirectory temp;
        
    private void assertFileExists(TempDirectory tempDirectory) {
        assertTrue( new File(tempDirectory.getPath()).exists() );        
    }
    
    private void assertFileDoesNotExist(TempDirectory tempDirectory) {
        assertFalse( new File(tempDirectory.getPath()).exists() );        
    }
    
    public void testGetPath() {
        String tempPath = temp.getPath();
        assertEquals( getTestPath(), tempPath);
    }
    
    public void testDirectoryNotCreatedInitially() {
        assertFileDoesNotExist(temp);        
    }
    
    public void testCreate() {
        temp.create();        
        assertFileExists(temp);
    }

    public void testRemove() {
        temp.create();
        temp.remove();
        assertFileDoesNotExist(temp);        
    }
    
    public void testRemoveChildrenFiles() {
        temp.create();
        new FileUtil(temp.getPath() + File.separator + "file1.txt").write("test data"); //$NON-NLS-1$ //$NON-NLS-2$
        temp.remove();
        assertFileDoesNotExist(temp);        
    }
    
    public void testRemoveChildrenDirectories() {
        temp.create();
        new File(temp.getPath() + File.separator + "subDirectory").mkdir(); //$NON-NLS-1$
        new FileUtil(temp.getPath()+ File.separator + "subDirectory" + File.separator + "file1.txt").write("test data"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        temp.remove();
        assertFileDoesNotExist(temp);        
    }
    
    public void testStaticTempDirectory() {
        TempDirectory newTmp = TempDirectory.getTempDirectory(UnitTestUtil.getTestDataPath());
        assertFileExists(newTmp);
        newTmp.remove();
        assertFileDoesNotExist(newTmp);        
    }
    
    public void testStaticTempDirectoryWithSubDirs() {
        TempDirectory newTmp = TempDirectory.getTempDirectory(UnitTestUtil.getTestScratchPath() + "/fred/joe/mary"); //$NON-NLS-1$
        assertFileExists(newTmp);
        newTmp.remove();
        assertFileDoesNotExist(newTmp);        
    }
    
    public void testStaticTempDirectoryWithSubDirsExists() {
        TempDirectory newTmp = TempDirectory.getTempDirectory(UnitTestUtil.getTestScratchPath() + "/fred/joe/mary"); //$NON-NLS-1$
        assertFileExists(newTmp);
        TempDirectory newTmp2 = TempDirectory.getTempDirectory(UnitTestUtil.getTestScratchPath() + "/fred/joe/mary"); //$NON-NLS-1$
        newTmp.remove();
        newTmp2.remove();
    }
    
    public void testStaticTempDirectoryDefaultLocation() {
        TempDirectory newTmp = TempDirectory.getTempDirectory(null);
        assertFileExists(newTmp);
        newTmp.remove();
        assertFileDoesNotExist(newTmp);        
    }
    
    private String getTestPath() {
        return StringUtilities.buildPath(System.getProperty("java.io.tmpdir"), "100_99"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /* 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        temp = new TempDirectory(100, 99);
    }

    /* 
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        new File(getTestPath()).delete();
    }
}
