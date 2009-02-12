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

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
* A sloppy filter to be used for test purposes only.  Will work with a FileSystemView and will filter
* based on the extensions of the files.  The filter will return files matching the supplied extensions
* AND all directories.  As a side effect, it will also return files with no extensions.  But, hey, it's
* a testing tool.  If you need a real filter, build your own.
*/

public class FileSystemFilter implements DirectoryEntryFilter {

    /**
    * Return the extension portion of the file's name .
    *
    * @see #getExtension
    * @see FileFilter#accept
    */
    public static String getFileNameExtension(String s) {
        if(s != null) {
            int i = s.lastIndexOf('.');
            if(i>0 && i<s.length()-1) {
                return s.substring(i+1).toLowerCase();
            }
            return ""; //$NON-NLS-1$
        }
        return null;
    }

//    private TreeNodeEditor editor = null;
    private String description = "q"; //$NON-NLS-1$
    private String[] filterExtensions;
    private FileSystemView fileSystemView;

    public FileSystemFilter(FileSystemView fsv, String[] extensions, String description) {
        setDescription(description);
        filterExtensions = extensions;
        fileSystemView = fsv;
    }

    public void setTreeNodeEditor( TreeNodeEditor editor ) {
        if ( editor == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0014));
        }
//        this.editor = editor;
    }

    /**
     * Obtain a description for this filter.
     * @return the readable description.  For example: "JPG and GIF Images"
     */
    public void setDescription(String s){
        description = s;
    }

    /**
     * Obtain a description for this filter.
     * @return the readable description.  For example: "JPG and GIF Images"
     */
    public String getDescription(){
        return description;
    }

    /**
     * Determine whether the given TreeNode is accepted by this filter.
     * @return true if accepted, or false otherwise.
     */
    public boolean accept(TreeNode entry){
        String ext = getFileNameExtension(entry.getName());
        if (ext != null) {
            for (int i=0; i<filterExtensions.length; i++) {
//                if ( ext.equals(filterExtensions[i]) || ext.equals("")) {
                if ( ext.equals(filterExtensions[i]) || fileSystemView.allowsChildren(entry)) {
                    return true;
                }
            }
        }
        return false;
    }

    public String getExtension(int index) {
        return filterExtensions[index];
    }

    public int getExtensionCount() {
        return filterExtensions.length;
    }

}

