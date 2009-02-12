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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.jar.Manifest;

import junit.framework.TestCase;

/**
 * @since 4.3
 */
public final class TestZipFileUtil extends TestCase {

    // ===========================================================================================================================
    // Constants

    private static final String PATH = UnitTestUtil.getTestDataPath();
    private static final String EMPTIED_ZIP_FILE_NAME = PATH + "/testEmptiedZipFile.zip"; //$NON-NLS-1$
    private static final String EMPTY_ZIP_FILE_NAME = PATH + "/testEmptyZipFile.zip"; //$NON-NLS-1$
    private static final String NEW_ZIP_FILE_NAME = UnitTestUtil.getTestScratchPath() + "/testNewZipFile.zip"; //$NON-NLS-1$
    private static final String ZIP_ENTRY_NAME = PATH + "/fakeFile"; //$NON-NLS-1$
    private static final String ZIP_FILE_NAME = PATH + "/testZipFile.zip"; //$NON-NLS-1$

    
    public void setUp() {
        try {
            new File(NEW_ZIP_FILE_NAME).delete();
        } catch (Exception e) {            
        }
    }
    
    // ===========================================================================================================================
    // Static Controller Methods

    /**
     * Adds a file to the specified zip file, verifies the results, 
     * removes the file, verifies the results, 
     * then deletes the zip file.
     * 
     * @param file
     *            The zip file on which to perform the add and remove test.
     * @since 4.3
     */
    private void addRemove(final File file) throws Exception {
        final long oldSize = file.length();
        try {
            //add
            assertTrue(ZipFileUtil.add(file, ZIP_ENTRY_NAME));
            assertTrue(file.exists());
            assertTrue(file.length() > oldSize);
            
            //remove
            assertTrue(ZipFileUtil.remove(file, ZIP_ENTRY_NAME));
            assertTrue(file.exists());
            assertEquals(oldSize, file.length());
            
        } finally {
            if (file.exists()) {
                file.delete();
            }
        }
    }
    
    
    
    
    
    
    /**
     * Adds a file to the specified zip file, verifies the results, then deletes the zip file.
     * 
     * @param file
     *            The zip file on which to perform the add and remove test.
     * @since 4.3
     */
    private void add(final File file) throws Exception {
        final long oldSize = file.length();
        try {
            assertTrue(ZipFileUtil.add(file, ZIP_ENTRY_NAME));
            assertTrue(file.exists());
            assertTrue(file.length() > oldSize);
            
        } finally {
            if (file.exists()) {
                file.delete();
            }
        }
    }

    /**
     * Creates a copy of the specified file.
     * 
     * @param file
     *            The file to copy.
     * @return
     * @throws IOException
     * @since 4.3
     */
    private static File copy(final String file) throws IOException {
        final File copy = File.createTempFile(ZipFileUtil.TMP_PFX, ZipFileUtil.TMP_SFX);
        final byte[] buf = new byte[ZipFileUtil.BUFFER];
        BufferedInputStream in = null;
        BufferedOutputStream out = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            out = new BufferedOutputStream(new FileOutputStream(copy));
            for (int count = in.read(buf); count >= 0; count = in.read(buf)) {
                out.write(buf, 0, count);
            }
        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
        }
        return copy;
    }

    // ===========================================================================================================================
    // Test Methods

    /**
     * @tests {@link ZipFileUtil#add(String, String)}, {@link ZipFileUtil#add(File, String)},
     *        {@link ZipFileUtil#remove(String, String)}, {@link ZipFileUtil#remove(File, String)}
     * @since 4.3
     */
    public void testAddRemove() throws Exception {
        final File file = new File(NEW_ZIP_FILE_NAME);
        assertTrue(!file.exists());
        add(file);
        addRemove(copy(ZIP_FILE_NAME));
        add(copy(EMPTIED_ZIP_FILE_NAME));
        add(copy(EMPTY_ZIP_FILE_NAME));
    }
    
    
    /**
     * Tests ZipFileUtil.addAll()
     * @since 4.3
     */
    public void testAddAll() throws Exception {
        final File file = new File(NEW_ZIP_FILE_NAME);
        
        assertEquals(0, file.length());
        assertTrue(ZipFileUtil.addAll(file, PATH));
        assertTrue(file.length() > 0);
        
    }
    
    /**
     * Tests ZipFileUtil.addAll()
     * @since 4.3
     */
    public void testAddAllPrefix() throws Exception {
        final File file = new File(NEW_ZIP_FILE_NAME);
        
        assertEquals(0, file.length());
        assertTrue(ZipFileUtil.addAll(file, PATH, "my/zip/path")); //$NON-NLS-1$
        assertTrue(file.length() > 0);
    }
    
    public void testGet() throws Exception {
        final File file = new File(ZIP_FILE_NAME);
        assertTrue(file.exists());
        
        
        InputStream is = ZipFileUtil.get(file, "META-INF/MANIFEST.MF", false); //$NON-NLS-1$
        
        assertNotNull(is);
        
        byte[] data = ObjectConverterUtil.convertToByteArray(is);

        assertNotNull(data);
        
        
        if (data.length == 0) {
            fail("No manifest file returned"); //$NON-NLS-1$
        }
        
    }  
    
    public void testGetManifest() throws Exception {
        final File file = new File(ZIP_FILE_NAME);
        assertTrue(file.exists());
        
        
        Manifest m = ZipFileUtil.getManifest(file);
        
        assertNotNull(m);
        
        
    }     
    
    
    public void testFind() throws Exception {
    	String jarname = PATH + "/extensionmodule/testjar.jar";
        final File file = new File(jarname);
        assertTrue(file.exists());
        
        
        List<String> found = ZipFileUtil.find(file, "Class.class$", false);
        assertNotNull(found);
        assertTrue(!found.isEmpty());
        
        
    }  
    
    public void testFindIgnoreCase() throws Exception {
    	String jarname = PATH + "/extensionmodule/testjar.jar";
        final File file = new File(jarname);
        assertTrue(file.exists());
        
        
        List<String> found = ZipFileUtil.find(file, "class.class$", true);
        assertNotNull(found);
        assertTrue(!found.isEmpty());
        
        
    }     
    
    public void testNotFind() throws Exception {
    	String jarname = PATH + "/extensionmodule/testjar.jar";
        final File file = new File(jarname);
        assertTrue(file.exists());
        
        
        List<String> found = ZipFileUtil.find(file, "^Class.class", false);
        assertNotNull(found);
        assertTrue(found.isEmpty());
        
        
    }     
    
}
