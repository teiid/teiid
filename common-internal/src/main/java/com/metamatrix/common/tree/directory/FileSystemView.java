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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.tree.PassThroughTreeNodeFilter;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.common.tree.TreeNodeFilter;
import com.metamatrix.common.tree.TreeNodeIterator;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.StringUtil;

/**
 * This interface defines a view of a hierarchy of DirectoryEntry instances.
 */
public class FileSystemView implements DirectoryEntryView {
    private static TreeNodeFilter DEFAULT_FILTER = new PassThroughTreeNodeFilter();
    private static Comparator DEFAULT_COMPARATOR = new DirectoryEntryNameAndTypeComparator();

    private File root = null;       // if null, then use FileSystemView's roots
    private boolean showRoot = false;
    private DirectoryEntry home = null;
    private TreeNodeFilter filter = DEFAULT_FILTER;
    private Comparator comparator = DEFAULT_COMPARATOR;
    private List propertyDefinitions = new ArrayList();
    private List unmodifiablePropertyDefinitions = Collections.unmodifiableList(this.propertyDefinitions);

    private Map dirEntries = new HashMap();

    /**
     * Construct a file system view with a domain of the entire file system, and where
     * the roots represent the root mounts of the file system, such as the drives on
     * a Windows machine or the root directories on a Unix system.
     */
    public FileSystemView() {
    }

    /**
     * Construct a file system view with a domain of the specified root folder and its
     * contents, and optionally include the specified root as an accessible folder.
     * This method also sets the home folder, which is a well known "bookmarked"
     * directory for this view, to the specified root.
     * @param root the folder that is to be the root of this view.
     * @param showRoot boolean flag that if true specifies that the root folder should
     * be accessible and returned as the single entry from the <code>getRoots</code>
     * method, or false if the child folders are to be returned as roots of thi
     * view.
     */
    public FileSystemView( File root, boolean showRoot ) {
        Assertion.isNotNull(root,"The root File reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(root.exists(),"The root File reference must exist"); //$NON-NLS-1$
        Assertion.assertTrue(root.isDirectory(),"The root File reference must be a folder"); //$NON-NLS-1$
        this.root = root;
        this.home = this.getFileSystemEntry( this.root, DirectoryEntry.TYPE_FOLDER );
        this.showRoot = showRoot;

    }

    /**
     * Return the marked state of the specified entry.
     * @return the marked state of the entry.
     */
    public boolean isMarked( TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        return fsEntry.isMarked();
    }
    
    /**
     * Update the dirEntries after a node has been moved by the FS Editor
     * @param the orignal File for the FileSystemEntry
     * @param the updated FileSystemEntry
     */
    public void entryMoved(File originalFile, TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        this.assertFileSystemEntry(entry);
        dirEntries.remove(originalFile.getAbsolutePath() );
        dirEntries.put(getAbsolutePath(entry), entry);
    }

    /**
     * Set the marked state of the specified entry.
     * @param true if the node is to be marked, or false if it is to be un-marked.
     */
    public void setMarked(TreeNode entry, boolean markedState) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        fsEntry.setMarked(markedState);
    }

    /**
     * Return the set of marked nodes for this view.
     * @param the unmodifiable set of marked nodes; never null
     */
    public Set getMarked() {
        Set result = new HashSet();
        Map.Entry entry = null;
        FileSystemEntry fsEntry = null;
        Iterator iter = this.dirEntries.entrySet().iterator();
        while ( iter.hasNext() ) {
            entry = (Map.Entry) iter.next();
            fsEntry = (FileSystemEntry) entry.getValue();
            if ( fsEntry.isMarked() ) {
                result.add(fsEntry);
            }
        }
        return Collections.unmodifiableSet( result );
    }

    /**
     * Set the filter that limits the set of DirectoryEntry instances
     * returned from this view.
     * @param filter the filter, or null if the default "pass-through" filter should be used.
     */
    public void setFilter( TreeNodeFilter filter ) {
        if ( filter == null ) {
            this.filter = DEFAULT_FILTER;
        } else {
            this.filter = filter;
        }
    }

    /**
     * Set the filter that limits the set of DirectoryEntry instances
     * returned from this view.
     * @return the current filter; never null
     */
    public TreeNodeFilter getFilter() {
        return this.filter;
    }

    /**
     * Set the comparator that should be used to order the children.
     * @param comparator the comparator, or null if entry name sorting should be used.
     */
    public void setComparator( Comparator comparator ) {
        if ( comparator == null ) {
            this.comparator = DEFAULT_COMPARATOR;
        } else {
            this.comparator = comparator;
        }
    }

    /**
     * Set the comparator that provides the order for children
     * returned from this view.
     * @return the current comparator; never null
     */
    public Comparator getComparator() {
        return this.comparator;
    }

    /**
     * Get the definitions of the properties for the DirectoryEntry instances
     * returned from this view.
     * @return the unmodifiable list of PropertyDefinition instances; never null
     */
    public List getPropertyDefinitions() {
        if ( this.propertyDefinitions.size() == 0 ) {
            this.propertyDefinitions.addAll( FileSystemEntry.getPropertyDefinitionList() );
        }
        return this.unmodifiablePropertyDefinitions;
    }

    /**
     * Returns all root partitians on this DirectoryEntry system.
     * @return the unmodifiable collection of DirectoryEntry instances
     * that represent the roots
     */
    public List getRoots() {
        // These aren't cached in case anything gets added at the root ...
        FileSystemEntry fsEntry = null;
        List roots = new ArrayList();
        if ( this.root != null ) {
            if ( this.showRoot ) {
                fsEntry = this.getFileSystemEntry(this.root, DirectoryEntry.TYPE_FOLDER);
                if (fsEntry != null) {
                    roots.add( fsEntry );
                }
            } else {
                File[] rootFiles = this.root.listFiles();
                for ( int i=0; i!=rootFiles.length; ++i ) {
                    File aFile = rootFiles[i];
                    if ( aFile.isDirectory() ) {
                        fsEntry = this.getFileSystemEntry(rootFiles[i], DirectoryEntry.TYPE_FOLDER);
                        if (fsEntry != null) {
                            roots.add( fsEntry );
                        }
                    }
                }
            }
        } else {
            File[] rootFiles = File.listRoots();
            for ( int i=0; i!=rootFiles.length; ++i ) {
                if (File.separatorChar == '\\' && rootFiles[i].getPath().startsWith("A:")) { //$NON-NLS-1$
                    //get rid of a: this takes a long time.
                    continue;
                }                
                fsEntry = this.getFileSystemEntry(rootFiles[i], DirectoryEntry.TYPE_FOLDER);
                if (fsEntry != null) {
                    roots.add( fsEntry );
                }
            }
        }

        // Order them ...
        Collections.sort(roots);

        // Filter them ...
        this.filter(roots);

        // Return unmodifiable form ...
        return Collections.unmodifiableList(roots);
    }

    /**
     * Returns all root partitians on this DirectoryEntry system regardless
     * of whether they are hidden.
     * @return the unmodifiable collection of DirectoryEntry instances
     * that represent the hidden and visible roots
     */
    protected List getActualRoots() {
        // These aren't cached in case anything gets added at the root ...
        FileSystemEntry fsEntry = null;
        List roots = new ArrayList();
        if ( this.root != null ) {
            fsEntry = this.getFileSystemEntry(this.root, DirectoryEntry.TYPE_FOLDER);
            if (fsEntry != null) {
                roots.add( fsEntry );
            }
        } else {
            File[] rootFiles = File.listRoots();
            for ( int i=0; i!=rootFiles.length; ++i ) {
                fsEntry = this.getFileSystemEntry(rootFiles[i], DirectoryEntry.TYPE_FOLDER);
                if (fsEntry != null) {
                    roots.add( fsEntry );
                }
            }
        }

        // Order them ...
        Collections.sort(roots);

        // Return unmodifiable form ...
        return Collections.unmodifiableList(roots);
    }

    protected void filter( List entries ) {
        if ( this.filter != DEFAULT_FILTER ) {
            Iterator iter = entries.iterator();
            while ( iter.hasNext() ) {
                if ( ! this.filter.accept( (DirectoryEntry) iter.next() ) ) {
                    iter.remove();
                }
            }
        }
    }

    /**
     * Determine whether the specified DirectoryEntry is a root of the underlying
     * system.
     * @param entry the DirectoryEntry instance that is to be checked; may
     * not be null
     * @return true if the entry is a root, or false otherwise.
     */
    public boolean isRoot(TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        if ( this.root != null ) {
            return entry.equals( this.root );       // can compare FileSystemEntry with a File
        }
        return this.getRoots().contains(fsEntry);
    }

    /**
     * Determine whether the specified DirectoryEntry is hidden.
     * @param entry the DirectoryEntry instance that is to be checked; may
     * not be null
     * @return true if the entry is hidden, or false otherwise.
     */
    public boolean isHidden(TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        return fsEntry.getFile().isHidden();
    }

    /**
     * Set the DirectoryEntryView root to use for the underlying system.
     * @param root the File that represents the root.
     * @param showRoot indicates if the root should be displays in the hierarchy
     */
    protected void setRoot(File root, boolean showRoot) {
        Assertion.isNotNull(root,"The root File reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(root.exists(),"The root File reference must exist"); //$NON-NLS-1$
        Assertion.assertTrue(root.isDirectory(),"The root File reference must be a folder"); //$NON-NLS-1$
        this.root = root;
        this.home = this.getFileSystemEntry( this.root, DirectoryEntry.TYPE_FOLDER );
        this.showRoot = showRoot;
    }

    /**
     * Set the DirectoryEntry that represents the home folder for this view.
     * The home is simply a folder that is widely known and can be treated as a
     * bookmark.
     * @param home the directory entry that represents the home folder; may not be null
     * and must exist within this view's domain of directory entries.
     * @throws AssertionError if <code>home</code> is null, if the entry does
     * not represent an existing folder, or if it is not contained within the domain
     * of this view.
     */
    public void setHome(TreeNode home) {
        Assertion.isNotNull(home,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(home.exists(),"The home TreeNode reference must represent an existing folder"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(home);
        Assertion.assertTrue(fsEntry.isFolder(),"The home TreeNode reference must represent a folder, not a file"); //$NON-NLS-1$
        if ( this.root != null ) {
            FileSystemEntry rootEntry = this.getFileSystemEntry(this.root,DirectoryEntry.TYPE_FOLDER);
            Assertion.assertTrue(this.isAncestorOf(rootEntry,home),"The specified TreeNode reference for the home is not contained within this view as specified by the root"); //$NON-NLS-1$
        }
        this.home = fsEntry;
    }

    /**
     * Obtain the DirectoryEntry that represents the folder within this view
     * that represents the single, well-known "bookmarked" folder for this view.
     * One example is for a FileSystemView accessing the whole the Windows file
     * system (i.e., no root is specified in the constructor), the home folder
     * represents the user's profile directory.
     * @return the entry that represents the home folder, or null if no home concept
     * is supported.
     */
    public TreeNode getHome() {
        if ( this.home != null ) {
            return this.home;
        }
        if ( this.root != null ) {
            return this.getFileSystemEntry( this.root, DirectoryEntry.TYPE_FOLDER );
        }
        return this.getFileSystemEntry(new File(System.getProperty("user.home")), DirectoryEntry.TYPE_FOLDER ); //$NON-NLS-1$
    }

    /**
     * Obtain the abstract path for this DirectoryEntry.
     * @return the string that represents the abstract path of this entry; never null
     */
    public String getPath(TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        String path = fsEntry.getFile().getPath();
        if ( this.root != null ) {
            String removePath = this.root.getPath();
            if ( ! this.showRoot ) {
                removePath = this.root.getPath();
            } else {
                removePath = this.root.getParent();
            }
            if ( path.startsWith(removePath) ) {
                path = path.substring(removePath.length());
            }
        }
        return path;
    }

    /**
     * Obtain the character that is used to separate names in a path sequence for
     * the abstract path.  This character is completely dependent upon the implementation.
     * @return the charater used to delimit names in the abstract path.
     */
    public char getSeparatorChar() {
        return File.separatorChar;
    }

    /**
     * Obtain the character (as a String) that is used to separate names in a path sequence for
     * the abstract path.
     * @return the string containing the charater used to delimit names in the abstract path; never null
     */
    public String getSeparator() {
        return File.separator;
    }

    /**
     * Obtain the absoluate path for this DirectoryEntry, which includes the
     * path of the root.  The separator character is used
     * between each of the names in the sequence.
     * @return the string that represents the absolute path of this entry.
     */
    public String getAbsolutePath(TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        return fsEntry.getFile().getPath();
    }

    /**
     * Determine the parent DirectoryEntry for the specified entry, or null if
     * the specified entry is a root.
     * @param entry the DirectoryEntry instance for which the parent is to be obtained;
     * may not be null
     * @return the parent entry, or null if there is no parent
     */
    public TreeNode getParent(TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        final File parentFile = fsEntry.getFile().getParentFile();
        if (parentFile == null) {
            return null;
        }
        if (parentFile.isDirectory()) {
            return getFileSystemEntry( parentFile, DirectoryEntry.TYPE_FOLDER );
        }
        return getFileSystemEntry( parentFile, DirectoryEntry.TYPE_FILE );
    }

    /**
     * Determine whether the specified TreeNode may contain children.
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry can contain children, or false otherwise.
     */
    public boolean allowsChildren(TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        return fsEntry.isFolder();
    }

    /**
     * Determine whether the specified parent TreeNode may contain the
     * specified child node.
     * @param parent the TreeNode instance that is to be the parent;
     * may not be null
     * @param potentialChild the TreeNode instance that is to be the child;
     * may not be null
     * @return true if potentialChild can be placed as a child of parent,
     * or false otherwise.
     */
    public boolean allowsChild(TreeNode parent, TreeNode potentialChild) {
        Assertion.isNotNull(parent,"The parent TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(potentialChild,"The potential child TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(parent);
        this.assertFileSystemEntry(potentialChild);
        return fsEntry.isFolder();
    }

    /**
     * Obtain the list of entries that are considered the children of the specified
     * DirectoryEntry.
     * @param parent the DirectoryEntry instance for which the child entries
     * are to be obtained; may not be null
     * @return the unmodifiable list of DirectoryEntry instances that are considered the children
     * of the specified entry; never null but possibly empty
     */
    public List getChildren(TreeNode parent) {
        Assertion.isNotNull(parent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(parent);
        File[] childFiles = this.getChildFiles(parent);
        if (childFiles != null && childFiles.length > 0) {
            List children = new ArrayList(childFiles.length);
            for ( int i=0; i!=childFiles.length; ++i ) {
                if (childFiles[i].isDirectory()) {
                    fsEntry = this.getFileSystemEntry(childFiles[i],DirectoryEntry.TYPE_FOLDER);
                    if (fsEntry != null) {
                        children.add( fsEntry );
                    }
                } else {
                    fsEntry = this.getFileSystemEntry(childFiles[i],DirectoryEntry.TYPE_FILE);
                    if (fsEntry != null) {
                        children.add( fsEntry );
                    }
                }
            }
    
            // Order them ...
            Collections.sort(children);
    
            // Filter them ...
            this.filter(children);
    
            // Return unmodifiable form ...
            return Collections.unmodifiableList(children);
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Return the array of <code>File</code> instances that are considered the
     * children of the specified DirectoryEntry.
     * @param parent the DirectoryEntry instance for which the child entries
     * are to be obtained; may not be null
     * @return the array of File instances that are considered the children
     * of the specified entry; never null but possibly empty
     */
    protected File[] getChildFiles(TreeNode parent) {
        Assertion.isNotNull(parent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(parent);
        return fsEntry.getFile().listFiles();
    }

    /**
     * Obtain an iterator for this whole view, which navigates the view's
     * nodes using pre-order rules (i.e., it visits a node before its children).
     * @return the view iterator
     */
    public Iterator iterator() {
        return new TreeNodeIterator(this.getRoots(),this);
    }

    /**
     * Obtain an iterator for the view starting at the specified node.  This
     * implementation currently navigates the subtree using pre-order rules 
     * (i.e., it visits a node before its children).
     * @param startingPoint the root of the subtree over which the iterator
     * is to navigate; may not be null
     * @return the iterator that traverses the nodes in the subtree starting
     * at the specified node; never null
     */
    public Iterator iterator(TreeNode startingPoint) {
        Assertion.isNotNull(startingPoint,"The TreeNode reference may not be null"); //$NON-NLS-1$
        return new TreeNodeIterator(startingPoint,this);
    }

    /**
     * Determine whether the specified node is a child of the given parent node.
     * @return true if the node is a child of the given parent node.
     */
    public boolean isParentOf(TreeNode parent, TreeNode child) {
        Assertion.isNotNull(parent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(child,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry parentEntry = this.assertFileSystemEntry(parent);
        FileSystemEntry childEntry  = this.assertFileSystemEntry(child);
        File parentFile = childEntry.getFile().getParentFile();
        return ( parentEntry.getFile().equals(parentFile) );
    }

    /**
     * Determine whether the specified node is a descendent of the given ancestor node.
     * @return true if the node is a descendent of the given ancestor node.
     */
    public boolean isAncestorOf(TreeNode ancestor, TreeNode descendent) {
        Assertion.isNotNull(ancestor,"The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(descendent,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry ancestorEntry   = this.assertFileSystemEntry(ancestor);
        FileSystemEntry descendentEntry = this.assertFileSystemEntry(descendent);

        File ancestorFile = descendentEntry.getFile().getParentFile();
        while( ancestorFile != null ) {
            if ( ancestorEntry.getFile().equals(ancestorFile) ) {
                return true;
            }
            ancestorFile = ancestorFile.getParentFile();
        }
        return false;
    }

    /**
     * Return the propertied object editor for this view.
     * @return the PropertiedObjectEditor instance
     */
    public PropertiedObjectEditor getPropertiedObjectEditor() {
        return this.getFileEntryEditor();
    }

    /**
     * Return the tree node editor for this view.
     * @return the TreeNodeEditor instance
     */
    public TreeNodeEditor getTreeNodeEditor() {
        return this.getFileEntryEditor();
    }

    /**
     * Return the directory entry editor for this view.
     * @return the DirectoryEntryEditor instance
     */
    public DirectoryEntryEditor getDirectoryEntryEditor() {
        return this.getFileEntryEditor();
    }

    /**
     * Return the file system editor for this view.
     * @return the FileSystemEntryEditor instance
     */
    public FileSystemEntryEditor getFileEntryEditor() {
        return new FileSystemEntryEditor(this);
    }

    /**
     * Lookup the node referenced by the relative path in this view.
     * Depending upon the implementation, this method may return a null
     * reference if a node with the specified path is not found.
     * <p>
     * This implementation never returns null if the specified path doesn't exist,
     * but in this case the implementation instead returns a
     * FileSystemEntry that will not exist.
     * @param path the path of the desired node specified in terms of this view
     * (i.e., the result of calling <code>getPath()</code> on this view with the
     * returned node as the parameter should result in the same value as <code>path</code>);
     * may not be null or zero-length
     * @return the node referenced by the specified path, or null if no such
     * node exists
     * @throws AssertionError if the path is null or zero-length
     */
    public DirectoryEntry lookup( String path ) {
        Assertion.isNotNull(path,"The path reference may not be null"); //$NON-NLS-1$
        Assertion.isNotZeroLength(path,"The path reference may not be zero-length"); //$NON-NLS-1$
        return lookup(path,this.getSeparator());
        
//        String fullPath = path;
//        if ( this.root != null ) {
//            fullPath = this.root.getAbsolutePath() + this.getSeparatorChar() + path;
//        }
//        File file = new File(fullPath);
//        FileSystemEntry fsEntry = null;
//        if (file.isDirectory()) {
//            fsEntry = this.getFileSystemEntry( file, DirectoryEntry.TYPE_FOLDER );
//        } else {
//            fsEntry = this.getFileSystemEntry( file, DirectoryEntry.TYPE_FILE );
//        }
//        return fsEntry;
    }


    /**
     * Lookup the node referenced by the relative path in this view, but
     * specify a separator.  This method allows the lookup of a path
     * with a different separator than used by this view.
     * Depending upon the implementation, this method may return a null
     * reference if a node with the specified path is not found.
     * @param path the path of the desired node specified in terms of this view
     * (i.e., the result of calling <code>getPath()</code> on this view with the
     * returned node as the parameter should result in the same value as <code>path</code>);
     * may not be null or zero-length
     * @param separater the string used to separate the components of a name.
     * @return the node referenced by the specified path, or null if no such
     * node exists
     * @throws AssertionError if the path is null or zero-length
     */
    public DirectoryEntry lookup( String path, String separator ) {
        Assertion.isNotNull(path,"The path reference may not be null"); //$NON-NLS-1$
        Assertion.isNotZeroLength(path,"The path reference may not be zero-length"); //$NON-NLS-1$
        Assertion.isNotNull(separator,"The separator may not be null"); //$NON-NLS-1$
        Assertion.isNotZeroLength(separator,"The separator may not be zero-length"); //$NON-NLS-1$

        // Replace all occurrences of 'separator' in path with 'this.getSeparatorChar()'
        String pathWithThisSeparator = path;
        if ( !this.getSeparator().equals(separator) ) {
            pathWithThisSeparator = StringUtil.replaceAll(path,separator,this.getSeparator());
        }
        
		// Lookup the entry using the path with the separator known by this view
        FileSystemEntry fsEntry = this.lookupByPath(pathWithThisSeparator);
        
        // If the entry was not found, check if the root node was prepended to 
        // the original path (this would occur if getPath(TreeNode) was called using
        // a FileSystemView created with showRoot==true).  If the root node name
        // is found in the start of the path, remove it and try the lookup again.
        if (fsEntry == null && this.root != null) {
            String modPath = pathWithThisSeparator;
            String prefix  = this.root.getName();
            if ( pathWithThisSeparator.startsWith(prefix) ) {
            	modPath = pathWithThisSeparator.substring(prefix.length());
            	fsEntry = this.lookupByPath(modPath);
            } 
            prefix = this.getSeparator() + this.root.getName();
            if ( pathWithThisSeparator.startsWith(prefix) ) {
            	modPath = pathWithThisSeparator.substring(prefix.length());
            	fsEntry = this.lookupByPath(modPath);
            }
        }
        return fsEntry;

//        File file = new File(fullPath);
//        FileSystemEntry fsEntry = null;
//        File file = new File(fullPath);
//        if (file.isDirectory()) {
//            fsEntry = this.getFileSystemEntry( file, DirectoryEntry.TYPE_FOLDER );
//        } else {
//            fsEntry = this.getFileSystemEntry( file, DirectoryEntry.TYPE_FILE );
//        }
//        return fsEntry;
    }

    /**
     * Lookup the node referenced by the relative path in this view. This 
     * method may return a null reference if a node with the specified path
     * is not found.
     */
    private FileSystemEntry lookupByPath( String path ) {
        // Prepend the path to the root folder to the relative path
        String fullPath = path;
        if ( this.root != null ) {
            if ( path.startsWith(this.getSeparator()) ) {
            	fullPath = this.root.getAbsolutePath() + path;
            } else {
            	fullPath = this.root.getAbsolutePath() + this.getSeparatorChar() + path;
            }
        }
        
        // Lookup the FileSystemEntry reference from the local cache
        FileSystemEntry fsEntry = null;
        synchronized( this.dirEntries ) {
            fsEntry = (FileSystemEntry) this.dirEntries.get(fullPath);
        }
        
        // If the local cache lookup failed but the file does exist
        // on the local file system, then add it to the cache
        File entry = new File(fullPath);
        if (fsEntry == null && entry.exists()) {
            if (entry.isDirectory()) {
            	fsEntry = this.getFileSystemEntry(entry, DirectoryEntry.TYPE_FOLDER);
            } else {
            	fsEntry = this.getFileSystemEntry(entry, DirectoryEntry.TYPE_FILE);
            }
        }
        return fsEntry;
    }

    
	// ########################## Implementation Methods ###################################

    protected File getFile(TreeNode entry) {
        Assertion.isNotNull(entry,"The TreeNode reference may not be null"); //$NON-NLS-1$
        FileSystemEntry fsEntry = this.assertFileSystemEntry(entry);
        return fsEntry.getFile();
    }

    private FileSystemEntry assertFileSystemEntry( TreeNode obj ) {
        Assertion.assertTrue((obj instanceof FileSystemEntry),"The type of TreeNode entry must be a FileSystemEntry"); //$NON-NLS-1$
        FileSystemEntry entry = (FileSystemEntry) obj;
        return entry;
    }

    protected FileSystemEntry getFileSystemEntry( File f, ObjectDefinition type ) {
        Assertion.isNotNull(f,"The File reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(type,"The ObjectDefinition reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue( ((type instanceof FileDefinition)||(type instanceof FolderDefinition)),"The ObjectDefinition must be of type FileDefinition or FolderDefinition"); //$NON-NLS-1$
        FileSystemEntry result = null;

        synchronized( this.dirEntries ) {
            result = (FileSystemEntry) this.dirEntries.get(f.getAbsolutePath());

            // If it is not found, create one ...
            if ( result == null ) {
                try {
                    result = new FileSystemEntry(f,type);
                    this.dirEntries.put(f.getAbsolutePath(),result);
                } catch (IOException e) {
                    return null;  // the entry could not be created
                }
            }

            // If it is found, make sure the properties are refreshed
            else {
                result.loadPreview();       // forces the reload of properties
            }
        }
        return result;
    }
}


