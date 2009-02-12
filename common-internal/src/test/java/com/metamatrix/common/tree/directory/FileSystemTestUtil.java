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

import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import com.metamatrix.common.object.ObjectDefinition;

/**
 * @version 	1.0
 * @author
 */
public class FileSystemTestUtil {
    private FileSystemTestUtil() {
    }
	
    /**
     * Test the deletion of a new FileSystemEntry instance.
     */
	public static void helpTestDelete(FileSystemEntry fsEntry) {
	    helpTestDelete(fsEntry,true);
	}
	
    /**
     * Test the deletion of a new FileSystemEntry instance.
     */
	public static void helpTestDelete(FileSystemEntry fsEntry, boolean expectSuccess) {
	    Assert.assertNotNull("The FileSystemEntry reference should not be null", fsEntry); //$NON-NLS-1$
	    boolean success = fsEntry.delete();
    	Assert.assertTrue("FileSystemEntry \""+fsEntry+"\" deletion from the file system, success ="+success, success == expectSuccess); //$NON-NLS-1$ //$NON-NLS-2$
	    if (expectSuccess) {
    	    Assert.assertTrue("FileSystemEntry \""+fsEntry+"\" was not deleted from the file system", !fsEntry.exists()); //$NON-NLS-1$ //$NON-NLS-2$
    	    Assert.assertTrue("FileSystemEntry \""+fsEntry+"\" was not deleted from the file system", !fsEntry.getFile().exists()); //$NON-NLS-1$ //$NON-NLS-2$
	    }
	}
	
    /**
     * Test the deletion of a new FileSystemEntry instance.
     */
	public static void helpTestDefaultProperties(FileSystemEntry fsEntry, boolean isFolder) {
	    Assert.assertNotNull("The FileSystemEntry reference should not be null", fsEntry); //$NON-NLS-1$
        helpTestExistence(fsEntry,true);
        helpTestEntryType(fsEntry,isFolder);
        helpTestPrivileges(fsEntry,true,true);
        helpTestEmpty(fsEntry,true);
	}
	
    /**
     * Test the existence of the FileSystemEntry against 
     * the expected result.
     */
	public static void helpTestExistence( FileSystemEntry fsEntry, boolean exists ) {
	    Assert.assertNotNull("The FileSystemEntry reference should not be null", fsEntry); //$NON-NLS-1$
	    Assert.assertTrue("FileSystemEntry \""+fsEntry+"\", exists is "+fsEntry.exists(), fsEntry.exists() == exists); //$NON-NLS-1$ //$NON-NLS-2$
	} 	
	
    /**
     * Test the type (file or folder) of the FileSystemEntry against 
     * the expected result.
     */
	public static void helpTestEntryType( FileSystemEntry fsEntry, boolean isFolder ) {
	    Assert.assertNotNull("The FileSystemEntry reference should not be null", fsEntry); //$NON-NLS-1$
	    String entryType = (fsEntry.isFolder() ? "FOLDER" : "FILE"); //$NON-NLS-1$ //$NON-NLS-2$
	    Assert.assertTrue("FileSystemEntry \""+fsEntry+"\", type is "+entryType, fsEntry.isFolder() == isFolder); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    ObjectDefinition type = fsEntry.getType();
	    if (isFolder) {
	    	Assert.assertTrue("FileSystemEntry \""+fsEntry+"\", type is "+fsEntry.getType(), type == DirectoryEntry.TYPE_FOLDER); //$NON-NLS-1$ //$NON-NLS-2$
	    } else {
	    	Assert.assertTrue("FileSystemEntry \""+fsEntry+"\", type is "+fsEntry.getType(), type == DirectoryEntry.TYPE_FILE); //$NON-NLS-1$ //$NON-NLS-2$
	    }
	} 	
	
    /**
     * Test the read/write privileges of the FileSystemEntry against 
     * the expected result
     */
	public static void helpTestPrivileges( FileSystemEntry fsEntry, boolean canRead, boolean canWrite ) {
	    Assert.assertNotNull("The FileSystemEntry reference should not be null", fsEntry); //$NON-NLS-1$
        fsEntry.isFolder();
//	    String type = (fsEntry.isFolder() ? "FOLDER" : "FILE");
	    Assert.assertTrue("FileSystemEntry \""+fsEntry+"\", canRead is "+fsEntry.canRead(), fsEntry.canRead() == canRead); //$NON-NLS-1$ //$NON-NLS-2$
	    Assert.assertTrue("FileSystemEntry \""+fsEntry+"\", canRead is "+fsEntry.canWrite(), fsEntry.canWrite() == canWrite); //$NON-NLS-1$ //$NON-NLS-2$
	    Assert.assertTrue("FileSystemEntry \""+fsEntry+"\", isReadOnly is "+fsEntry.isReadOnly(), fsEntry.isReadOnly() == !canWrite); //$NON-NLS-1$ //$NON-NLS-2$
	} 	
	
    /**
     * Test if the FileSystemEntry is considered empty (0 KB) against 
     * the expected result
     */
	public static void helpTestEmpty( FileSystemEntry fsEntry, boolean empty ) {
	    Assert.assertNotNull("The FileSystemEntry reference should not be null", fsEntry); //$NON-NLS-1$
	    if (!fsEntry.isFolder()) {
	    	Assert.assertTrue("FileSystemEntry \""+fsEntry+"\", isEmpty is "+fsEntry.isEmpty(), fsEntry.isEmpty() == empty); //$NON-NLS-1$ //$NON-NLS-2$
	    }
	} 	
	
    /**
     * Returns the first FileSystemEntry instance representing a root
     * from the list of actual roots for the specified FileSystemView
     */
	public static FileSystemEntry helpGetActualRoot( FileSystemView fsView ) {
	    Assert.assertNotNull("The FileSystemView reference should not be null", fsView); //$NON-NLS-1$
        List roots = fsView.getActualRoots();
	    Assert.assertTrue("FileSystemView has "+ roots.size()+" roots",roots.size() >= 1); //$NON-NLS-1$ //$NON-NLS-2$
		return (FileSystemEntry) roots.iterator().next();
	}
	
    /**
     * Returns the FileSystemEntry instance from the Collection of
     * FileSystemEntry instances that matches the specified name or
     * null if no match was found.
     */
	public static FileSystemEntry helpLookupByName( String name, Collection entries ) {
	    Assert.assertNotNull("The String reference should not be null", name); //$NON-NLS-1$
	    Assert.assertNotNull("The List reference should not be null", entries); //$NON-NLS-1$
		for (Iterator i = entries.iterator(); i.hasNext();) {
            FileSystemEntry entry = (FileSystemEntry) i.next();
            if (entry != null && entry.getName().equals(name)) {
                return entry;
            }
        }
        return null;		
	}
	
    /**
     * Test the contents of the specified list of FileSystemEntry instances
     * against the expected result
     */
	public static void helpTestParentChildRelationship( FileSystemView fsView, FileSystemEntry parent, FileSystemEntry child ) {
	    Assert.assertNotNull("The FileSystemView reference should not be null", fsView); //$NON-NLS-1$
	    Assert.assertNotNull("The FileSystemEntry reference should not be null", parent); //$NON-NLS-1$
	    Assert.assertNotNull("The FileSystemEntry reference should not be null", child); //$NON-NLS-1$
		Assert.assertTrue("Entry \""+child+"\" should have parent \""+parent+"\"",fsView.getParent(child) == parent);	     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		Assert.assertTrue("Entry \""+parent+"\" should have child \""+child+"\"",fsView.getChildren(parent).contains(child));	     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	} 	
	
    /**
     * Test the contents of the specified list against the expected result
     */
	public static void helpCompareResults( Collection results, Collection expectedResults ) {
	    Assert.assertNotNull("The Collection reference should not be null", results); //$NON-NLS-1$
	    Assert.assertNotNull("The Collection reference should not be null", expectedResults); //$NON-NLS-1$
	    Assert.assertTrue("The number of entries in the result is "+results.size()+", expected "+expectedResults.size(), results.size() == expectedResults.size()); //$NON-NLS-1$ //$NON-NLS-2$
		for (Iterator i = results.iterator(); i.hasNext();) {
            FileSystemEntry entry = (FileSystemEntry) i.next();
			Assert.assertTrue("Entry \""+entry+"\" not found in expected results",expectedResults.contains(entry));	     //$NON-NLS-1$ //$NON-NLS-2$
        }		
	} 	

    public static void printCollection(Collection c, PrintStream stream) {
        Assert.assertNotNull("The Collection reference may not be null",c); //$NON-NLS-1$
        Assert.assertNotNull("The PrintStream reference may not be null",stream); //$NON-NLS-1$
        if (c.isEmpty()) {
            stream.println("<empty Collection>"); //$NON-NLS-1$
        }
        int counter = 0;
        for (Iterator i = c.iterator(); i.hasNext();) {
            stream.println("Collection[" + counter + "] = " + i.next()); //$NON-NLS-1$ //$NON-NLS-2$
            counter++;
        }
    }

}
