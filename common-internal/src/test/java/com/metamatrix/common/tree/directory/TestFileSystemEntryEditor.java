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

package com.metamatrix.common.tree.directory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version 	1.0
 * @author
 */
public class TestFileSystemEntryEditor extends TestCase {
    
   	// Map of test files and folders under the root keyed by name
   	private static Map TEST_ENTRIES = new HashMap();
    
   	// Root java.io.File for creation of new views
   	private static File ROOT = TestFileSystemEntry.exampleTempFile();

	// ################################## FRAMEWORK ################################
	
	public TestFileSystemEntryEditor(String name) { 
		super(name);
	}	
	
	public static void oneTimeSetUp() {
		ROOT.mkdir();

	    // Create test files and folders under the root
        try {
			TEST_ENTRIES.put("testFileA", new FileSystemEntry(new File(ROOT,"testFileA"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$
			TEST_ENTRIES.put("testFileB", new FileSystemEntry(new File(ROOT,"testFileB"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$
			TEST_ENTRIES.put("testFileC", new FileSystemEntry(new File(ROOT,"testFileC"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$
			TEST_ENTRIES.put("testFileD", new FileSystemEntry(new File(ROOT,"testFileD"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$

			TEST_ENTRIES.put("testFolderX", new FileSystemEntry(new File(ROOT,"testFolderX"),DirectoryEntry.TYPE_FOLDER) ); //$NON-NLS-1$ //$NON-NLS-2$
			File folderX = ((FileSystemEntry) TEST_ENTRIES.get("testFolderX")).getFile(); //$NON-NLS-1$

			TEST_ENTRIES.put("testFileXA", new FileSystemEntry(new File(folderX,"testFileXA"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$
			TEST_ENTRIES.put("testFileXB", new FileSystemEntry(new File(folderX,"testFileXB"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$
			TEST_ENTRIES.put("testFileXC", new FileSystemEntry(new File(folderX,"testFileXC"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$

			TEST_ENTRIES.put("testFolderY", new FileSystemEntry(new File(ROOT,"testFolderY"),DirectoryEntry.TYPE_FOLDER) ); //$NON-NLS-1$ //$NON-NLS-2$
			File folderY = ((FileSystemEntry) TEST_ENTRIES.get("testFolderY")).getFile(); //$NON-NLS-1$

			TEST_ENTRIES.put("testFileYA", new FileSystemEntry(new File(folderY,"testFileYA"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$
			TEST_ENTRIES.put("testFileYB", new FileSystemEntry(new File(folderY,"testFileYB"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$
			TEST_ENTRIES.put("testFileYC", new FileSystemEntry(new File(folderY,"testFileYC"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$

			TEST_ENTRIES.put("testFolderZ", new FileSystemEntry(new File(ROOT,"testFolderZ"),DirectoryEntry.TYPE_FOLDER) ); //$NON-NLS-1$ //$NON-NLS-2$
			File folderZ = ((FileSystemEntry) TEST_ENTRIES.get("testFolderZ")).getFile(); //$NON-NLS-1$

			TEST_ENTRIES.put("testFileZA", new FileSystemEntry(new File(folderZ,"testFileZA"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$
			TEST_ENTRIES.put("testFileZB", new FileSystemEntry(new File(folderZ,"testFileZB"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$
			TEST_ENTRIES.put("testFileZC", new FileSystemEntry(new File(folderZ,"testFileZC"),DirectoryEntry.TYPE_FILE) ); //$NON-NLS-1$ //$NON-NLS-2$

        } catch (IOException e) {
            oneTimeTearDown();
            fail("Error creating new FileSystemEntry instance: "+e); //$NON-NLS-1$
        }
	}
	
	public static void oneTimeTearDown() {
	    // Delete all files first ...
	    for (Iterator i = TEST_ENTRIES.values().iterator(); i.hasNext();) {
            FileSystemEntry entry = (FileSystemEntry) i.next();
            if ( !entry.isFolder()) {
	            boolean success = entry.delete();
    			assertTrue("FileSystemEntry \""+entry+"\" was not deleted successfully", success); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
	    // Next delete all folders ...
	    for (Iterator i = TEST_ENTRIES.values().iterator(); i.hasNext();) {
            FileSystemEntry entry = (FileSystemEntry) i.next();
            if ( entry.isFolder()) {
	            boolean success = entry.delete();
    			assertTrue("FileSystemEntry \""+entry+"\" was not deleted successfully", success); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
	    // Finally delete the root
	  	ROOT.delete();  
	}
	
	// ################################## EXAMPLE OBJECTS ################################

    /**
     * Create a FileSystemView instance with the domain of 
     * a single root folder.
     */
	public static FileSystemView exampleView(boolean showRoot) {
	    return new FileSystemView(ROOT,showRoot);
	}

    /**
     * Create a FileSystemView instance with the domain of 
     * the entire file system
     */
	public static FileSystemView exampleView() {
	    return new FileSystemView();
	}
	
	// ################################## ACTUAL TESTS ################################
	
    /**
     * Test the creation of a FileSystemEntry instances of type FILE through
     * the editor.
     */
	public void testCreateEntry1() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntryEditor editor = (FileSystemEntryEditor) fsView.getDirectoryEntryEditor();
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);

		FileSystemEntry newEntry1 = (FileSystemEntry) editor.create(root, "testFileE", DirectoryEntry.TYPE_FILE); //$NON-NLS-1$
        FileSystemTestUtil.helpTestDefaultProperties(newEntry1,false);

		// Get the children of the root folder and compare to the expected results
        List results = fsView.getChildren(root);
        List expectedResults = new ArrayList(10);
        expectedResults.add( TEST_ENTRIES.get("testFileA") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileB") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileC") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileD") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFolderX") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFolderY") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFolderZ") ); //$NON-NLS-1$
        expectedResults.add( newEntry1 );
        FileSystemTestUtil.helpCompareResults(results,expectedResults);
        
		// clean up
		FileSystemTestUtil.helpTestDelete(newEntry1, true);
	}
	
    /**
     * Test the creation of a FileSystemEntry instances of type FOLDER through
     * the editor.
     */
	public void testCreateEntry2() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntryEditor editor = (FileSystemEntryEditor) fsView.getDirectoryEntryEditor();
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);

		FileSystemEntry newEntry1 = (FileSystemEntry) editor.create(root, "testFolderW", DirectoryEntry.TYPE_FOLDER); //$NON-NLS-1$
        FileSystemTestUtil.helpTestDefaultProperties(newEntry1,true);

		// Get the children of the root folder and compare to the expected results
        List results = fsView.getChildren(root);
        List expectedResults = new ArrayList(10);
        expectedResults.add( TEST_ENTRIES.get("testFileA") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileB") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileC") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileD") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFolderX") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFolderY") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFolderZ") ); //$NON-NLS-1$
        expectedResults.add( newEntry1 );
        FileSystemTestUtil.helpCompareResults(results,expectedResults);
        
		// clean up
		FileSystemTestUtil.helpTestDelete(newEntry1, true);
	}

    /**
     * Test the creation of a FileSystemEntry instances of type FOLDER through
     * the editor.
     */
    public void testCopy1() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntryEditor editor = (FileSystemEntryEditor) fsView.getDirectoryEntryEditor();
//        FileSystemEntry root  = 
        FileSystemTestUtil.helpGetActualRoot(fsView);

        FileSystemEntry folder1 = (FileSystemEntry) TEST_ENTRIES.get("testFolderX"); //$NON-NLS-1$
        FileSystemEntry newEntry1 = (FileSystemEntry) editor.create(folder1, "testFileE", DirectoryEntry.TYPE_FILE); //$NON-NLS-1$
        FileSystemTestUtil.helpTestDefaultProperties(newEntry1,false);
        
        // Copy the new entry under a new folder
        FileSystemEntry folder2 = (FileSystemEntry) TEST_ENTRIES.get("testFolderZ"); //$NON-NLS-1$
        FileSystemEntry newEntry2 = (FileSystemEntry) editor.copy(newEntry1,folder2);

        // Get the children of the root folder and compare to the expected results
        List folderContents  = fsView.getChildren(folder1);
        List expectedResults = new ArrayList(10);
        expectedResults.add( TEST_ENTRIES.get("testFileXA") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileXB") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileXC") ); //$NON-NLS-1$
        expectedResults.add( newEntry1 );
        FileSystemTestUtil.helpCompareResults(folderContents,expectedResults);
        
        folderContents  = fsView.getChildren(folder2);
        expectedResults.clear();
        expectedResults.add( TEST_ENTRIES.get("testFileZA") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileZB") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileZC") ); //$NON-NLS-1$
        expectedResults.add( newEntry2 );
        FileSystemTestUtil.helpCompareResults(folderContents,expectedResults);
        
        // clean up
        FileSystemTestUtil.helpTestDelete(newEntry1, true);
        FileSystemTestUtil.helpTestDelete(newEntry2, true);
    }

	// ################################## TEST SUITE ################################

    /** 
     * Construct the test suite, which uses a one-time setup call
     * and a one-time tear-down call.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestFileSystemEntryEditor.class);
        return new TestSetup(suite) {
            public void setUp() {
                oneTimeSetUp();
            }
            public void tearDown() {
                oneTimeTearDown();
            }
        };
    }

}
