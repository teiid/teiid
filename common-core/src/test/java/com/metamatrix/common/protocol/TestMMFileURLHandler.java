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

package com.metamatrix.common.protocol;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;

import junit.framework.TestCase;

/**
 * Test class to test the read, write, delete files using the mmfile: URL protocol 
 * handler.
 * @since 4.4
 */
public class TestMMFileURLHandler extends TestCase {

    /** 
     * @param fooURL
     * @throws IOException
     * @since 4.3
     */
    private void readFile(URL fooURL) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(fooURL.openStream()));
        assertEquals("This is test file for testing mmfile: protocol", reader.readLine()); //$NON-NLS-1$
        assertEquals("This is the second line.", reader.readLine()); //$NON-NLS-1$
        reader.close();
    }

    /** 
     * @param foo
     * @throws IOException
     * @since 4.3
     */
    private void createFile(File foo) throws IOException {
        FileWriter fw = new FileWriter(foo);
        fw.write("This is test file for testing mmfile: protocol\n"); //$NON-NLS-1$
        fw.write("This is the second line.\n"); //$NON-NLS-1$
        fw.close();
    }
    


    /** 
     * @param foo
     * @throws FileNotFoundException
     * @throws IOException
     * @since 4.3
     */
    private void readFile(File foo) throws FileNotFoundException,
                                   IOException {
        BufferedReader reader = new BufferedReader (new FileReader(foo));
        assertEquals("This is test file for testing mmfile: protocol", reader.readLine()); //$NON-NLS-1$
        assertEquals("This is the second line.", reader.readLine()); //$NON-NLS-1$
        reader.close();
    }

    /** 
     * @param fooURL
     * @throws IOException
     * @since 4.3
     */
    private void createFile(URL fooURL) throws IOException {
        URLConnection conn = fooURL.openConnection();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream()));
        writer.write("This is test file for testing mmfile: protocol\n"); //$NON-NLS-1$
        writer.write("This is the second line.\n"); //$NON-NLS-1$
        writer.close();
    }
    
    public void testRead() throws Exception {
        
        
        String tmpDir = System.getProperty("java.io.tmpdir"); //$NON-NLS-1$
        File foo = new File(tmpDir+File.separator+"foo.txt"); //$NON-NLS-1$
        
        // make sure we do not have the file to begin with
        assertFalse(foo.exists());
        
        // Create File using regular files
        createFile(foo);
        
        assertTrue(foo.exists());
        
        URL fooURL = URLHelper.buildURL(foo.getAbsolutePath());
        assertTrue(fooURL.toString().startsWith("mmfile:")); //$NON-NLS-1$

        // read file using the URL
        readFile(fooURL);
        
        foo.delete();        
    }    
    
    public void testWrite() throws Exception {
        String tmpDir = System.getProperty("java.io.tmpdir"); //$NON-NLS-1$
        File foo = new File(tmpDir+File.separator+"foo.txt"); //$NON-NLS-1$

        // make sure we do not have the file to begin with
        assertFalse(foo.exists());
        
        URL fooURL = URLHelper.buildURL(foo.getAbsolutePath()+"?action=write"); //$NON-NLS-1$
        assertTrue(fooURL.toString().startsWith("mmfile:")); //$NON-NLS-1$
        assertTrue(fooURL.toString().endsWith("?action=write")); //$NON-NLS-1$
        
        // Create File using the URL
        createFile(fooURL);
        
        assertTrue(foo.exists());
        
        // Read file using the regular File
        readFile(foo);
        
        foo.delete();        
    }    
    
    public void testList() throws Exception {
        String tmpDir = System.getProperty("java.io.tmpdir"); //$NON-NLS-1$
        File foo = new File(tmpDir+File.separator+"foo.txt"); //$NON-NLS-1$
        File tmp = new File(tmpDir);
        
        assertFalse(foo.exists());
        
        // Create File using the File
        createFile(foo);
        
        assertTrue(foo.exists());
                
        URL fooURL = URLHelper.buildURL(tmp.getAbsolutePath()+"/?action=list&filter=.txt"); //$NON-NLS-1$
        assertTrue(fooURL.toString().startsWith("mmfile:")); //$NON-NLS-1$
        assertTrue(fooURL.toString().endsWith("?action=list&filter=.txt")); //$NON-NLS-1$
                
        ObjectInputStream in = new ObjectInputStream(fooURL.openStream());
        String[] list = (String[])in.readObject();
        in.close();
        
        boolean found = false;
        // we must find the foo.txt in the list..
        for (int i = 0; i < list.length; i++) {
            found = list[i].endsWith("foo.txt"); //$NON-NLS-1$
            if (found) {
                break;
            }
        }
        
        assertTrue("foo.txt must have been found", found); //$NON-NLS-1$
        
        foo.delete();
    }

    public void testDelete() throws Exception {

        String tmpDir = System.getProperty("java.io.tmpdir"); //$NON-NLS-1$
        File foo = new File(tmpDir+File.separator+"foo.txt"); //$NON-NLS-1$
        
        assertFalse(foo.exists());
        
        // Create File using the File
        createFile(foo);
        
        assertTrue(foo.exists());
                
        URL fooURL = URLHelper.buildURL(foo.getAbsolutePath()+"?action=delete"); //$NON-NLS-1$
        assertTrue(fooURL.toString().startsWith("mmfile:")); //$NON-NLS-1$
        assertTrue(fooURL.toString().endsWith("?action=delete")); //$NON-NLS-1$
                
        
        try {
            InputStream in = fooURL.openStream();
            if (in != null) {
                in.close();
                fail("opened the stream on the deleted file"); //$NON-NLS-1$
            }
            
        } catch (FileNotFoundException e) {
            // pass
        }
        
        // should have gone by now
        assertFalse(foo.exists());
    }    
    
    public void testGetPath() throws Exception {
        URL url = URLHelper.buildURL("mmfile:/foo.txt"); //$NON-NLS-1$
        assertEquals("/foo.txt", url.getPath()); //$NON-NLS-1$
        
        url = URLHelper.buildURL("mmfile:/d:/foo.txt");       //$NON-NLS-1$
        assertEquals("/d:/foo.txt", url.getPath()); //$NON-NLS-1$
        
        url = URLHelper.buildURL("mmfile:/c:/metamatrix/foo.txt");//$NON-NLS-1$
        assertEquals("/c:/metamatrix/foo.txt", url.getPath()); //$NON-NLS-1$
                
        url = URLHelper.buildURL("mmfile:c:/foo.txt");//$NON-NLS-1$
        assertEquals("c:/foo.txt", url.getPath()); //$NON-NLS-1$
        
    }
}
