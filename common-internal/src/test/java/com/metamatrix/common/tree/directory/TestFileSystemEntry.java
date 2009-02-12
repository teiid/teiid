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
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;

/**
 * This test case creates and manipulates FileSystemEntry instances
 * @version 	1.0
 * @author
 */
public class TestFileSystemEntry extends TestCase {

    // Formatter used during the creation of unique java.io.File names
    public static final SimpleDateFormat FORMATTER = new SimpleDateFormat("MMddyyhhmmssSSS"); //$NON-NLS-1$
    
    // Suffix used when creating names for java.io.File instances
    public static final String FILE_PREFIX = "tmp"; //$NON-NLS-1$
    
    // Suffix used when creating names for java.io.File instances
    public static final String FILE_SUFFIX = ""; //$NON-NLS-1$

	// ################################## FRAMEWORK ################################
	
	public TestFileSystemEntry(String name) { 
		super(name);
	}	
	
	// ################################## EXAMPLE OBJECTS ################################
	
	public static File exampleTempFile() {
		Date currentTime = new Date();
		String timestamp = FORMATTER.format(currentTime);
		int counter      = 0;
		String filename = FILE_PREFIX + timestamp + Integer.toString(counter) + FILE_SUFFIX;
		File tmp = new File( UnitTestUtil.getTestScratchPath(), filename);
		
		// Create new File instances until we generate a non-existent file
		while (tmp.exists()) {
		    counter++;
		    filename = FILE_PREFIX + timestamp + Integer.toString(counter) + FILE_SUFFIX;
		    tmp = new File( UnitTestUtil.getTestScratchPath(), filename);
		}
		return tmp;
	}
	
//	private File exampleFile( String parent ) {
//	    assertNotNull("The parent string should not be null", parent);
//	    assertTrue("The parent string should not be zero-length", parent.length() != 0);
//
//		Date currentTime = new Date();
//		String filename = "test"+FORMATTER.format(currentTime)+FILE_EXTENSION;
//		return new File( parent, filename );
//	} 	
//
//	private File exampleFile( File parent ) {
//	    assertNotNull("The File reference should not be null", parent);
//
//		Date currentTime = new Date();
//		String filename = "test"+FORMATTER.format(currentTime)+FILE_EXTENSION;
//		return new File( parent, filename );
//	}
	
	// ################################## TEST HELPERS ################################
	
    /**
     * Test the creation of a new FileSystemEntry instance.
     */
	private FileSystemEntry helpTestCreate() {
	    File tmp = exampleTempFile();
	    return helpTestCreate(tmp);
	}
	
    /**
     * Test the creation of a new FileSystemEntry instance.
     */
	private FileSystemEntry helpTestCreate(File file) {
        FileSystemEntry fsEntry = null;
        try {
        	fsEntry = new FileSystemEntry(file);
        } catch (IOException e) {
            fail("Error creating new FileSystemEntry instance: "+e); //$NON-NLS-1$
        }
	    assertNotNull("Error creating new FileSystemEntry instance", fsEntry); //$NON-NLS-1$
	    return fsEntry;
	}
	
    /**
     * Test the creation of a new FileSystemEntry instance.
     */
	private FileSystemEntry helpTestCreate(boolean isFolder) {
	    File tmp = exampleTempFile();
	    return helpTestCreate(tmp,isFolder);
	}
	
    /**
     * Test the creation of a new FileSystemEntry instance.
     */
	private FileSystemEntry helpTestCreate(File file, boolean isFolder) {
        FileSystemEntry fsEntry = null;
        try {
            if (isFolder) {
        		fsEntry = new FileSystemEntry(file,DirectoryEntry.TYPE_FOLDER);
            } else {
                fsEntry = new FileSystemEntry(file,DirectoryEntry.TYPE_FILE);
            }
        } catch (IOException e) {
            fail("Error creating new FileSystemEntry instance: "+e); //$NON-NLS-1$
        }
	    assertNotNull("Error creating new FileSystemEntry instance", fsEntry); //$NON-NLS-1$
	    return fsEntry;
	}
	
	
	// ################################## ACTUAL TESTS ################################

    /**
     * Test the FileSystemEntry constructor.
     */
    public void testCreateEntry1() {
        FileSystemEntry fsEntry = helpTestCreate();
        FileSystemTestUtil.helpTestDefaultProperties(fsEntry,false);

		// clean up
        FileSystemTestUtil.helpTestDelete(fsEntry);
    }

    /**
     * Test the FileSystemEntry constructor.
     */
    public void testCreateEntry2() {
        FileSystemEntry fsEntry = helpTestCreate(false);
        FileSystemTestUtil.helpTestDefaultProperties(fsEntry,false);
        
		// clean up
        FileSystemTestUtil.helpTestDelete(fsEntry);
    }

    /**
     * Test the FileSystemEntry constructor.
     */
    public void testCreateEntry3() {
        FileSystemEntry fsEntry = helpTestCreate(true);
        FileSystemTestUtil.helpTestDefaultProperties(fsEntry,true);
        
		// clean up
        FileSystemTestUtil.helpTestDelete(fsEntry);
    }

    /**
     * Test the deletion of a FileSystemEntry instance of
     * type FILE.
     */
    public void testDeleteEntry1() {
        FileSystemEntry fsEntry = helpTestCreate(false);
        FileSystemTestUtil.helpTestDelete(fsEntry);
    }

    /**
     * Test the deletion of a FileSystemEntry instance of
     * type FOLDER.
     */
    public void testDeleteEntry2() {
        FileSystemEntry fsEntry = helpTestCreate(true);
        FileSystemTestUtil.helpTestDelete(fsEntry);
    }

    /**
     * Test the deletion of a FileSystemEntry instance of
     * type FOLDER that contains a file.
     */
    public void testDeleteEntry3() {
        FileSystemEntry folder = helpTestCreate(true);

		// Create a test file under the folder
	    File tmp = new File(folder.getFile(), exampleTempFile().getName());
        FileSystemEntry testFile = helpTestCreate(tmp,false);

		// Delete the parent folder before deleting its contents - should fail
        FileSystemTestUtil.helpTestDelete(folder,false);

		// Now delete the folder contents before deleting the folder - should succeed
        FileSystemTestUtil.helpTestDelete(testFile,true);
        FileSystemTestUtil.helpTestDelete(folder,true);
    }

    /**
     * Test the equivalence of a FileSystemEntry instance of 
     * type FILE to itself.
     */
	public void testEquivalence1() {
        FileSystemEntry fsEntry = helpTestCreate(false);
	    UnitTestUtil.helpTestEquivalence(0,fsEntry,fsEntry);

		// clean up
        FileSystemTestUtil.helpTestDelete(fsEntry);
	}

    /**
     * Test the equivalence of a FileSystemEntry instance of 
     * type FOLDER to itself.
     */
	public void testEquivalence2() {
        FileSystemEntry fsEntry = helpTestCreate(true);
	    UnitTestUtil.helpTestEquivalence(0,fsEntry,fsEntry);

		// clean up
        FileSystemTestUtil.helpTestDelete(fsEntry);
	}

    /**
     * Test the equivalence of a FileSystemEntry instance of 
     * type FOLDER a FileSystemEntry of type FOLDER.
     */
	public void testEquivalence3() {
        FileSystemEntry fsEntryA = helpTestCreate(false);
        FileSystemEntry fsEntryB = helpTestCreate(true);
	    UnitTestUtil.helpTestEquivalence(-1,fsEntryA,fsEntryB);

		// clean up
        FileSystemTestUtil.helpTestDelete(fsEntryA);
        FileSystemTestUtil.helpTestDelete(fsEntryB);
	}

    /**
     * Test the equivalence of a two FileSystemEntry instances created 
     * using the same java.io.File instance.
     */
	public void testEquivalence4() {
	    File testFile = exampleTempFile();
        FileSystemEntry fsEntryA = helpTestCreate(testFile,false);
        FileSystemEntry fsEntryB = helpTestCreate(testFile,false);
	    UnitTestUtil.helpTestEquivalence(0,fsEntryA,fsEntryB);

		// clean up
        FileSystemTestUtil.helpTestDelete(fsEntryA);
        FileSystemTestUtil.helpTestDelete(fsEntryB,false); // should not succeed since underlying java.io.File already deleted
	}

    /**
     * Test the equivalence of a two FileSystemEntry instances created 
     * using the two differnt java.io.File instances constructed from
     * the same abstract path
     */
	public void testEquivalence5() {
	    File testFile    = exampleTempFile();
	    File controlFile = new File(testFile.getParentFile(), testFile.getName());
        FileSystemEntry fsEntryA = helpTestCreate(testFile,false);
        FileSystemEntry fsEntryB = helpTestCreate(controlFile,false);
	    UnitTestUtil.helpTestEquivalence(0,fsEntryA,fsEntryB);

		// clean up
        FileSystemTestUtil.helpTestDelete(fsEntryA);
        FileSystemTestUtil.helpTestDelete(fsEntryB,false); // should not succeed since underlying java.io.File already deleted
	}

    /**
     * Test the moving of a FileSystemEntry under a new parent folder.
     */
	public void testMove1() {
	    // Create two parallel folders, A and B
        FileSystemEntry folderA = helpTestCreate(true);
        FileSystemEntry folderB = helpTestCreate(true);

		// Create a test file under folder A
	    File tmp = new File(folderA.getFile(), exampleTempFile().getName());
        FileSystemEntry testFile = helpTestCreate(tmp,false);

		// Move the test file from folder A to folder B
		boolean success = testFile.move(folderB);
		assertTrue("The FileSystemEntry \""+testFile+"\" was not successfully moved under \""+folderB.getFullName()+"\"",success); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		File newParent = testFile.getFile().getParentFile();
		assertTrue("The parent \""+newParent+"\" is not \""+folderB.getFullName()+"\"",newParent.equals(folderB.getFile())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		// Make sure the properties on the test file have not changed
        FileSystemTestUtil.helpTestDefaultProperties(testFile,false);

		// clean up
        FileSystemTestUtil.helpTestDelete(testFile);
        FileSystemTestUtil.helpTestDelete(folderB);
        FileSystemTestUtil.helpTestDelete(folderA);
	}

    /**
     * Test the moving of a FileSystemEntry under a new parent folder
     * and then back again. 
     */
	public void testMove2() {
	    // Create two parallel folders, A and B
        FileSystemEntry folderA = helpTestCreate(true);
        FileSystemEntry folderB = helpTestCreate(true);

		// Create a sub-folder C under folder A
	    File tmp = new File(folderA.getFile(), exampleTempFile().getName());
        FileSystemEntry folderC = helpTestCreate(tmp,true);

		// Create a test file under folder C
	    tmp = new File(folderC.getFile(), exampleTempFile().getName());
        FileSystemEntry testFile    = helpTestCreate(tmp,false);
        FileSystemEntry controlFile = helpTestCreate(tmp,false);

		// Move the test file from folder C to folder B
		boolean success = testFile.move(folderB);
		assertTrue("The FileSystemEntry \""+testFile+"\" was not successfully moved under \""+folderB.getFullName()+"\"",success); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		File newParent = testFile.getFile().getParentFile();
		assertTrue("The parent \""+newParent+"\" is not \""+folderB.getFullName()+"\"",newParent.equals(folderB.getFile())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		// Make sure the properties on the test file have not changed
        FileSystemTestUtil.helpTestDefaultProperties(testFile,false);

		// Move the test file back under folder C
	    success = testFile.move(folderC);
		assertTrue("The FileSystemEntry \""+testFile+"\" was not successfully moved under \""+folderC.getFullName()+"\"",success); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		File oldParent = testFile.getFile().getParentFile();
		assertTrue("The parent \""+oldParent+"\" is not \""+folderC.getFullName()+"\"",oldParent.equals(folderC.getFile())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		// Again make sure the properties on the test file have not changed
        FileSystemTestUtil.helpTestDefaultProperties(testFile,false);
	    UnitTestUtil.helpTestEquivalence(0,testFile,controlFile);

		// clean up
        FileSystemTestUtil.helpTestDelete(testFile);
        FileSystemTestUtil.helpTestDelete(folderC);
        FileSystemTestUtil.helpTestDelete(folderB);
        FileSystemTestUtil.helpTestDelete(folderA);
	}

    /**
     * Test the ability to rename a file entry.
     */
	public void testRenameTo() {
	    File testFile    = exampleTempFile();
	    File controlFile = new File(testFile.getParentFile(), testFile.getName());
        FileSystemEntry fsEntryA = helpTestCreate(testFile,false);
        FileSystemEntry fsEntryB = helpTestCreate(controlFile,false);
        
        // rename test file to a new name
        String oldName = testFile.getName();
        String newName = oldName+"Renamed"; //$NON-NLS-1$
        boolean success = fsEntryA.renameTo(newName);
		assertTrue("The FileSystemEntry \""+fsEntryA+"\" was not successfully renamed to \""+newName+"\"",success); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertTrue("The FileSystemEntry \""+fsEntryA+"\" was not successfully renamed to \""+newName+"\"",fsEntryA.getFile().getName().equals(newName)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
        // rename test file back to its old name
        success = fsEntryA.renameTo(oldName);
		assertTrue("The FileSystemEntry \""+fsEntryA+"\" was not successfully renamed to \""+oldName+"\"",success); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertTrue("The FileSystemEntry \""+fsEntryA+"\" was not successfully renamed to \""+oldName+"\"",fsEntryA.getFile().getName().equals(oldName)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

		// test equivalence back to control file
	    UnitTestUtil.helpTestEquivalence(0,fsEntryA,fsEntryB);

		// clean up
        FileSystemTestUtil.helpTestDelete(fsEntryA);
        FileSystemTestUtil.helpTestDelete(fsEntryB,false); // should not succeed since underlying java.io.File already deleted
	}
}
