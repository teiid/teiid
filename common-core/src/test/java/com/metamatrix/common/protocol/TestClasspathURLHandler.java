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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.common.classloader.PostDelegatingClassLoader;
import com.metamatrix.core.util.UnitTestUtil;


/** 
 * A test Class to test the "classpath:" protocol
 * @since 4.4
 */
public class TestClasspathURLHandler extends TestCase {

    ClassLoader previousClassLoader = null;
	
    public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(TestClasspathURLHandler.class);
		//return suite;
        return new TestSetup(suite){
            protected void setUp() throws Exception{
                System.setProperty("mm.io.tmpdir", System.getProperty("java.io.tmpdir")+"/metamatrix"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
            protected void tearDown() throws Exception{
            }
        };
	}
    
    protected void setUp() throws Exception {
        super.setUp();
        String path1 = "file:"+UnitTestUtil.getTestDataPath() + "/pathtest.jar"; //$NON-NLS-1$ //$NON-NLS-2$
        String path2 = "file:"+UnitTestUtil.getTestDataPath() + "/VersionImpl.jar"; //$NON-NLS-1$ //$NON-NLS-2$
        
        // get the current class loader
        previousClassLoader = Thread.currentThread().getContextClassLoader();
        
        // now set the new class loader into current thread's context
        URL url1 = new URL(path1);
        URL url2 = new URL(path2);
        ClassLoader jarClassPath = new URLClassLoader(new URL[] {url1, url2}, previousClassLoader);
        Thread.currentThread().setContextClassLoader(jarClassPath);
    }      
    
    protected void tearDown() throws Exception {
        super.tearDown();
        Thread.currentThread().setContextClassLoader(previousClassLoader);
    }      
    
    private void readFile(URL fooURL) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(fooURL.openStream()));
        assertEquals("This is test file to test classpath: protocal", reader.readLine()); //$NON-NLS-1$
        assertEquals("this is the second line to test", reader.readLine()); //$NON-NLS-1$
        reader.close();
    }
    
    private void createFile(URL fooURL) throws IOException {
        URLConnection conn = fooURL.openConnection();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream()));
        writer.write("This is test file to test classpath: protocal\n"); //$NON-NLS-1$
        writer.write("this is the second line to test\n"); //$NON-NLS-1$
        writer.close();
    }    
    
    public void testRead() throws Exception {
        URL fooURL = URLHelper.buildURL("classpath:pathtest/foo.txt"); //$NON-NLS-1$
        assertTrue(fooURL.toString().startsWith("classpath:")); //$NON-NLS-1$

        readFile(fooURL);
        
        fooURL = URLHelper.buildURL("classpath:Results.csv"); //$NON-NLS-1$
        BufferedReader reader = new BufferedReader(new InputStreamReader(fooURL.openStream()));
        assertEquals("\"INTKEY\",\"STRINGKEY\",\"INTNUM\",\"STRINGNUM\",\"FLOATNUM\",\"LONGNUM\",\"DOUBLENUM\",\"BYTENUM\",", reader.readLine()); //$NON-NLS-1$
        assertEquals("\"0\",\"0\",\"null\",\"-24\",\"-24.0\",\"-24\",\"-24\",\"-128\",", reader.readLine()); //$NON-NLS-1$
        reader.close();        
    }
    
    public void testWrite() throws Exception {
        URL fooURL = URLHelper.buildURL("classpath:pathtest/foo.txt"); //$NON-NLS-1$
        assertTrue(fooURL.toString().startsWith("classpath:")); //$NON-NLS-1$

        try {
            createFile(fooURL);
            fail("Should have failed to write the file using this protocol"); //$NON-NLS-1$
        }catch(IOException e) {
            //e.printStackTrace();
            // pass
        }
        
    }
        
    public void testDelete() throws Exception {
        URL fooURL = URLHelper.buildURL("classpath:pathtest/foo.txt?action=delete"); //$NON-NLS-1$
        assertTrue(fooURL.toString().startsWith("classpath:")); //$NON-NLS-1$
                
        try {
            readFile(fooURL);
            fail("must have failed to delete the resource"); //$NON-NLS-1$
        } catch (IOException e) {
            // pass
        }
    }

    public void testList() throws Exception {
        URL fooURL = URLHelper.buildURL("classpath:pathtest/foo.txt?action=list&filter=.txt"); //$NON-NLS-1$
        assertTrue(fooURL.toString().startsWith("classpath:")); //$NON-NLS-1$

        ObjectInputStream reader = new ObjectInputStream(fooURL.openStream());
        String[] urls = (String[])reader.readObject();
        assertTrue(urls[0].endsWith("/pathtest.jar!/pathtest/foo.txt")); //$NON-NLS-1$
        reader.close();        
    }
    
    public void testPatchingJar() throws Exception{
        String patchpath = "file:"+UnitTestUtil.getTestDataPath() + "/patch_pathtest.jar"; //$NON-NLS-1$ //$NON-NLS-2$
        String path = "file:"+UnitTestUtil.getTestDataPath() + "/pathtest.jar"; //$NON-NLS-1$ //$NON-NLS-2$
        
        // get the current class loader
        ClassLoader previousClsLoader= Thread.currentThread().getContextClassLoader();
        
        // now set the new class loader into current thread's context
        URL patchurl = new URL(patchpath);
        URL url = new URL(path);
        
        ClassLoader jarClassPath = new PostDelegatingClassLoader(new URL[] {patchurl, url}, previousClsLoader);
        Thread.currentThread().setContextClassLoader(jarClassPath);
    
        URL fooURL = URLHelper.buildURL("classpath:pathtest/foo.txt"); //$NON-NLS-1$
        BufferedReader reader = new BufferedReader(new InputStreamReader(fooURL.openStream()));
        assertEquals("This is updated file; I should have been found", reader.readLine()); //$NON-NLS-1$
        reader.close();
                
        Thread.currentThread().setContextClassLoader(previousClsLoader);
    }
    
    public void testPatchJar() throws Exception{
        String patchpath = "classpath:versionImpl-11.jar"; //$NON-NLS-1$ 
        String path = "classpath:versionImpl-10.jar"; //$NON-NLS-1$ 

        
        // get the current class loader
        ClassLoader previousClsLoader= Thread.currentThread().getContextClassLoader();
        
        // now set the new class loader into current thread's context
        URL patchurl = new URL(patchpath);
        URL url = new URL(path);
        
        // Right now there is an error with issuing patches with "classpath"
        // protocol (defect 21557), there is no fix just workaround, that is
        // to reverse the url path to the class loader.        
        ClassLoader jarClassLoader = new PostDelegatingClassLoader(new URL[] {url, patchurl}, previousClsLoader, new MetaMatrixURLStreamHandlerFactory());
        
        // this is how it is supposed to work!
        ClassLoader urlClassLoader = new URLClassLoader(new URL[] {patchurl, url}, previousClsLoader, new MetaMatrixURLStreamHandlerFactory());
        Thread.currentThread().setContextClassLoader(jarClassLoader);
        
        String className = "VersionImpl"; //$NON-NLS-1$
        
        // using delegating class loader
        Class clazz = urlClassLoader.loadClass(className);
        FakeVersion version = (FakeVersion)clazz.newInstance();
        assertEquals("version did not match", "1.1", version.getVersion());//$NON-NLS-1$ //$NON-NLS-2$
        
        // using non delegating class loader
        clazz = jarClassLoader.loadClass(className);
        version = (FakeVersion)clazz.newInstance();        
        assertEquals("version did not match", "1.1", version.getVersion());//$NON-NLS-1$ //$NON-NLS-2$        
        
        Thread.currentThread().setContextClassLoader(previousClsLoader);
    }
        
}
