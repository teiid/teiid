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
import java.util.Set;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @version 	1.0
 * @author
 */
public class TestFileSystemView extends TestCase {
    
   	// Map of test files and folders under the root keyed by name
   	private static Map TEST_ENTRIES = new HashMap();
    
   	// Root java.io.File for creation of new views
   	private static File ROOT = TestFileSystemEntry.exampleTempFile();

	// ################################## FRAMEWORK ################################
	
	public TestFileSystemView(String name) { 
		super(name);
	}	
	
	// ############################### SETUP AND TEAR-DOWN ############################
	
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

    /**
     * Return an existing FileSystemEntry instance of the specified type 
     * under the root folder.
     */
	public static FileSystemEntry exampleEntry(FileSystemView fsView, boolean returnFolder) {
        FileSystemEntry root = FileSystemTestUtil.helpGetActualRoot(fsView);
        return exampleEntry(fsView,root,returnFolder);
	}

    /**
     * Return an existing FileSystemEntry instance of the specified type under the
     * specified parent folder.
     */
	public static FileSystemEntry exampleEntry(FileSystemView fsView, FileSystemEntry parent, boolean returnFolder) {
	    assertNotNull("The FileSystemView reference should not be null", fsView); //$NON-NLS-1$
	    assertNotNull("The FileSystemEntry reference should not be null", parent); //$NON-NLS-1$
	    assertTrue("The FileSystemEntry reference must be a folder", parent.isFolder()); //$NON-NLS-1$

		for (Iterator i = fsView.getChildren(parent).iterator(); i.hasNext();) {
            FileSystemEntry entry = (FileSystemEntry) i.next();
            if ( entry.isFolder() && returnFolder) {
                return entry;
            } else if ( !entry.isFolder() && !returnFolder) {
                return entry;
            }
        }
        return null;
	}
	
	// ################################## TEST HELPERS ################################
	
    /**
     * Test the looking up an entry by path
     */
    public void helpTestLookup(FileSystemView fsView, String path, FileSystemEntry expectedResult) {        
		helpTestLookup(fsView,path,fsView.getSeparator(),expectedResult);
    }
	
    /**
     * Test the looking up an entry by path with a user defined separator
     */
    public void helpTestLookup(FileSystemView fsView, String path, String separator, FileSystemEntry expectedResult) {        
        //System.out.println("helpTestLookup: path ="+path);
        FileSystemEntry result = (FileSystemEntry) fsView.lookup(path,separator);
		assertTrue("Lookup returned \""+result+"\", expected \""+expectedResult+"\"",result == expectedResult);	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
	
    /**
     * Test the looking up an entry by path with a user defined separator
     */
    public void helpTestLookup(FileSystemView fsView, String path, String separator, File expectedResult) {        
        //System.out.println("helpTestLookup: path ="+path);
        FileSystemEntry result = (FileSystemEntry) fsView.lookup(path,separator);
		assertEquals("Lookup returned \""+result+"\", expected \""+expectedResult+"\"",expectedResult.getAbsolutePath(), result.getFile().getAbsolutePath());	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
	
	// ################################## ACTUAL TESTS ################################
	
    /**
     * Test the creation of a new FileSystemEntry instance.
     */
	public void testCreate1() {
        FileSystemView fsView = exampleView(true); // showRoot = true;
        
		// Check view actual roots ...
        List roots = fsView.getActualRoots();
	    assertTrue("FileSystemView has "+ roots.size()+" root(s)",roots.size()==1); //$NON-NLS-1$ //$NON-NLS-2$
		FileSystemEntry root = (FileSystemEntry) roots.iterator().next();
	    assertTrue("FileSystemView root has changed.",root.getFile() == ROOT); //$NON-NLS-1$
        
		// Check view's unhidden roots ...
        roots = fsView.getRoots();
	    assertTrue("FileSystemView has "+ roots.size()+" root(s)",roots.size()==1); //$NON-NLS-1$ //$NON-NLS-2$
		root = (FileSystemEntry) roots.iterator().next();
	    assertTrue("FileSystemView root has changed.",root.getFile() == ROOT); //$NON-NLS-1$
        
		// Check home entry in view
		FileSystemEntry home = (FileSystemEntry) fsView.getHome();
	    assertTrue("FileSystemView home has changed.",home.getFile() == ROOT); //$NON-NLS-1$
	}
	
    /**
     * Test the creation of a new FileSystemEntry instance.
     */
	public void testCreate2() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        
		// Check view actual roots ...
        List roots = fsView.getActualRoots();
	    assertTrue("FileSystemView has "+ roots.size()+" root(s)",roots.size()==1); //$NON-NLS-1$ //$NON-NLS-2$
		FileSystemEntry root = (FileSystemEntry) roots.iterator().next();
	    assertTrue("FileSystemView root has changed.",root.getFile() == ROOT); //$NON-NLS-1$
        
		// Check view's unhidden roots ...
        roots = fsView.getRoots();
	    assertTrue("FileSystemView has "+ roots.size()+" root(s)",roots.size()==3); //$NON-NLS-1$ //$NON-NLS-2$
        
		// Check home entry in view
		FileSystemEntry home = (FileSystemEntry) fsView.getHome();
	    assertTrue("FileSystemView home has changed.",home.getFile() == ROOT); //$NON-NLS-1$
	}
	
    /**
     * Test the roots returned for a view constructed from a 
     * single root that is hidden.  The expected results for
     * the list of returned roots should be the list of folders
     * under the hidden root.
     */
	public void testRoots1() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        List results = fsView.getRoots();

        List expectedResults = new ArrayList(5);
        expectedResults.add( TEST_ENTRIES.get("testFolderX") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFolderY") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFolderZ") ); //$NON-NLS-1$
        FileSystemTestUtil.helpCompareResults(results,expectedResults);
	}
	
    /**
     * Test the roots returned for a view constructed from a 
     * single root that is shown.  The expected results for
     * the list of returned roots should be the single root
     * used to construct the view.
     */
	public void testRoots2() {
        FileSystemView fsView = exampleView(true); // showRoot = true;
        List results = fsView.getRoots();

        List expectedResults = new ArrayList(1);
        expectedResults.add( FileSystemTestUtil.helpGetActualRoot(fsView) );
        FileSystemTestUtil.helpCompareResults(results,expectedResults);
	}
	
    /**
     * Test the roots returned for a view constructed for 
     * domain of the entire local file system.  The expected
     * results for the list of returned roots should be the
     * root mounts of the file system, such as the drives on
     * a Windows machine or the root directories on a Unix system.
     */
	public void testRoots3() {
        FileSystemView fsView = exampleView();
        List results = fsView.getRoots();
        assertTrue("The number of roots found is "+results.size()+", expected >= 1",results.size() >= 1); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
    /**
     * Test the setting of the HOME entry for the view
     */
	public void testHome() {
        FileSystemView fsView = exampleView(true); // showRoot = true;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);

        FileSystemEntry home = FileSystemTestUtil.helpLookupByName("testFolderY",fsView.getChildren(root)); //$NON-NLS-1$
        fsView.setHome(home);
	    assertTrue("FileSystemView home is "+fsView.getHome()+", expected "+home,fsView.getHome()==home); //$NON-NLS-1$ //$NON-NLS-2$
	}

    /**
     * Test getting the children of the root folder
     */
    public void testChildren1() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);

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
        FileSystemTestUtil.helpCompareResults(results,expectedResults);
    }

    /**
     * Test getting the children of a sub-folder of the root
     */
    public void testChildren2() {
        FileSystemView fsView  = exampleView(false); // showRoot = false;
        FileSystemEntry parent = (FileSystemEntry) TEST_ENTRIES.get("testFolderX"); //$NON-NLS-1$

		// Get the children of the root folder and compare to the expected results
        List results = fsView.getChildren(parent);
        List expectedResults = new ArrayList(5);
        expectedResults.add( TEST_ENTRIES.get("testFileXA") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileXB") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileXC") ); //$NON-NLS-1$
        FileSystemTestUtil.helpCompareResults(results,expectedResults);
    }

    /**
     * Test getting the parent of a file under the root folder
     * when the view has the root hidden
     */
    public void testParent1() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry child = (FileSystemEntry) TEST_ENTRIES.get("testFileC"); //$NON-NLS-1$

        FileSystemEntry parent = (FileSystemEntry) fsView.getParent(child);
        FileSystemEntry root   = FileSystemTestUtil.helpGetActualRoot(fsView);
		assertTrue("Entry \""+child+"\" should have parent \""+root+"\"",parent == root);	     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		FileSystemTestUtil.helpTestParentChildRelationship(fsView,parent,child);	    
    }

    /**
     * Test getting the parent of a file under the root folder
     * when the view has the root not hidden
     */
    public void testParent2() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry child = (FileSystemEntry) TEST_ENTRIES.get("testFileC"); //$NON-NLS-1$

        FileSystemEntry parent = (FileSystemEntry) fsView.getParent(child);
        FileSystemEntry root   = FileSystemTestUtil.helpGetActualRoot(fsView);
		assertTrue("Entry \""+child+"\" should have parent \""+root+"\"",parent == root); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		FileSystemTestUtil.helpTestParentChildRelationship(fsView,parent,child);	    
    }

    /**
     * Test getting the parent of a file under a sub-folder of the root
     */
    public void testParent3() {
        FileSystemView fsView  = exampleView(false); // showRoot = false;
        FileSystemEntry child = (FileSystemEntry) TEST_ENTRIES.get("testFileXB"); //$NON-NLS-1$

        FileSystemEntry parent  = (FileSystemEntry) fsView.getParent(child);
        FileSystemEntry control = (FileSystemEntry) TEST_ENTRIES.get("testFolderX"); //$NON-NLS-1$
		assertTrue("Entry \""+child+"\" should have parent \""+control+"\"",parent.equals(control));	     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		FileSystemTestUtil.helpTestParentChildRelationship(fsView,parent,child);	    
    }

    /**
     * Test the setting and retrieving of "marked" entries
     */
    public void testMarked() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);

		List childrenOfRoot = fsView.getChildren(root);
        FileSystemEntry folderX = FileSystemTestUtil.helpLookupByName("testFolderX",childrenOfRoot); //$NON-NLS-1$
        FileSystemEntry folderY = FileSystemTestUtil.helpLookupByName("testFolderY",childrenOfRoot); //$NON-NLS-1$
        FileSystemEntry folderZ = FileSystemTestUtil.helpLookupByName("testFolderZ",childrenOfRoot); //$NON-NLS-1$
        
        fsView.setMarked(folderZ, true);
        fsView.setMarked(FileSystemTestUtil.helpLookupByName("testFileA",childrenOfRoot), true); //$NON-NLS-1$
        fsView.setMarked(FileSystemTestUtil.helpLookupByName("testFileZA",fsView.getChildren(folderZ)), true); //$NON-NLS-1$
        fsView.setMarked(FileSystemTestUtil.helpLookupByName("testFileXB",fsView.getChildren(folderX)), true); //$NON-NLS-1$
        fsView.setMarked(FileSystemTestUtil.helpLookupByName("testFileYC",fsView.getChildren(folderY)), true); //$NON-NLS-1$

		// Get the marked entries of the view and compare to the expected results
        Set results = fsView.getMarked();
        List expectedResults = new ArrayList(5);
        expectedResults.add( TEST_ENTRIES.get("testFolderZ") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileA") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileZA") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileXB") ); //$NON-NLS-1$
        expectedResults.add( TEST_ENTRIES.get("testFileYC") ); //$NON-NLS-1$
        FileSystemTestUtil.helpCompareResults(results,expectedResults);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * NOT hidden resulting in the lookup path containing the root name
     * as a prefix.
     */
    public void testLookup1() {
        FileSystemView fsView = exampleView(true); // showRoot = true;
        FileSystemEntry folder= exampleEntry(fsView,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = fsView.getPath(file);
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * NOT hidden and the lookup path does not contain the root name
     * as a prefix and does not start with a separator character.
     */
    public void testLookup2() {
        FileSystemView fsView = exampleView(true); // showRoot = true;
        FileSystemEntry folder= exampleEntry(fsView,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = folder.getName() + fsView.getSeparator() + file.getName();
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * NOT hidden and the lookup path does not contain the root name
     * as a prefix.
     */
    public void testLookup3() {
        FileSystemView fsView = exampleView(true); // showRoot = true;
        FileSystemEntry folder= exampleEntry(fsView,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = fsView.getSeparator() + folder.getName() 
                    + fsView.getSeparator() + file.getName();
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * NOT hidden and the lookup path does contain the root name
     * as a prefix but does not start with a separator character.
     */
    public void testLookup4() {
        FileSystemView fsView = exampleView(true); // showRoot = true;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = root.getName()
        		    + fsView.getSeparator() + folder.getName() 
                    + fsView.getSeparator() + file.getName();
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * NOT hidden and the lookup path does contain the root name
     * as a prefix.
     */
    public void testLookup5() {
        FileSystemView fsView = exampleView(true); // showRoot = true;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = fsView.getSeparator() + root.getName()
        		    + fsView.getSeparator() + folder.getName() 
                    + fsView.getSeparator() + file.getName();
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden resulting in the lookup path containing the root name
     * as a prefix.
     */
    public void testLookup6() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry folder= exampleEntry(fsView,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = fsView.getPath(file);
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path does not contain the root name
     * as a prefix and does not start with a separator character.
     */
    public void testLookup7() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = folder.getName() + fsView.getSeparator() + file.getName();
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path does not contain the root name
     * as a prefix.
     */
    public void testLookup8() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = fsView.getSeparator() + folder.getName() 
                    + fsView.getSeparator() + file.getName();
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path does contain the root name
     * as a prefix but does not start with a separator character.
     */
    public void testLookup9() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = root.getName()
        		    + fsView.getSeparator() + folder.getName() 
                    + fsView.getSeparator() + file.getName();
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path does contain the root name
     * as a prefix.
     */
    public void testLookup10() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = fsView.getSeparator() + root.getName()
        		    + fsView.getSeparator() + folder.getName() 
                    + fsView.getSeparator() + file.getName();
        helpTestLookup(fsView,path,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path does not contain the root name
     * as a prefix and the separator character is user defined.
     */
    public void testLookup11() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String separator = "&"; //$NON-NLS-1$
        String path = separator + folder.getName() 
                    + separator + file.getName();
        helpTestLookup(fsView,path,separator,file);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path does contain the root name
     * as a prefix and the separator character is user defined.
     */
    public void testLookup12() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String separator = "&"; //$NON-NLS-1$
        String path = root.getName()
        	        + separator + folder.getName() 
                    + separator + file.getName();
        helpTestLookup(fsView,path,separator,file);
    }
    
    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path is poorly formatted - should fail.
     */
    public void testLookup13() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        FileSystemEntry file  = exampleEntry(fsView,folder,false);
        String path = root.getName() + "&" //$NON-NLS-1$
        	        + fsView.getSeparator() + folder.getName() 
                    + fsView.getSeparator() + file.getName();
        helpTestLookup(fsView,path,null);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path does not represent an existing
     * entry - should fail.
     */
    public void testLookup14() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        String path = fsView.getSeparator() + folder.getName() 
                    + fsView.getSeparator() + "nonExistentFile"; //$NON-NLS-1$
        helpTestLookup(fsView,path,null);
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path represents an existing entry that
     * initially is not found in the view's cache.
     */
    public void testLookup15() throws Exception {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        File file = new File(folder.getFile(),"existingFile"); //$NON-NLS-1$
    	file.createNewFile();
        String path = fsView.getSeparator() + folder.getName() 
                    + fsView.getSeparator() + "existingFile"; //$NON-NLS-1$
        helpTestLookup(fsView,path,fsView.getSeparator(),file);
        file.delete(); // clean up
    }

    /**
     * Test the looking up an entry by path when the view's root is 
     * hidden and the lookup path represents an existing entry that
     * initially is not found in the view's cache.
     */
    public void testLookup16() {
        FileSystemView fsView = exampleView(false); // showRoot = false;
        FileSystemEntry root  = FileSystemTestUtil.helpGetActualRoot(fsView);
        FileSystemEntry folder= exampleEntry(fsView,root,true);
        File file = new File(folder.getFile(),"existingFile"); //$NON-NLS-1$
        try {
        	file.createNewFile();
            String path = fsView.getSeparator() + root.getName()
            	        + fsView.getSeparator() + folder.getName() 
                        + fsView.getSeparator() + "existingFile"; //$NON-NLS-1$
            helpTestLookup(fsView,path,fsView.getSeparator(),file);
            file.delete(); // clean up
        } catch (IOException e) {
            fail("Failed to created test java.io.File instance"); //$NON-NLS-1$
        }
    }

	// ################################## TEST SUITE ################################

    /** 
     * Construct the test suite, which uses a one-time setup call
     * and a one-time tear-down call.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestFileSystemView.class);
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
