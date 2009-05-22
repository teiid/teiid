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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipFile;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.object.PropertyDefinitionImpl;
import com.metamatrix.common.object.PropertyType;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;

/**
 * This class represents a single resource on a hierarchical system, such as
 * a file system.
 */
public class FileSystemEntry implements DirectoryEntry {

    private static final DecimalFormat FORMATTER = new DecimalFormat("###,###,###,###,###"); //$NON-NLS-1$

    private boolean marked = false;
    private ObjectDefinition objectDefn;
    private static PropertyDefinitionImpl NAME_PROPERTY = null;
    private static PropertyDefinitionImpl SIZE_PROPERTY = null;
    private static PropertyDefinitionImpl LAST_MODIFIED_PROPERTY = null;
    private static PropertyDefinitionImpl READ_ONLY_PROPERTY = null;
    private static PropertyDefinitionImpl IS_HIDDEN_PROPERTY = null;
    private static List PROPERTY_DEFINITIONS = new ArrayList(5);
    private static final List UNMODIFIABLE_PROPERTY_DEFINITIONS = Collections.unmodifiableList( PROPERTY_DEFINITIONS );

    static {
        NAME_PROPERTY = new PropertyDefinitionImpl("name",PropertyType.STRING,true); //$NON-NLS-1$
        NAME_PROPERTY.setDisplayName("Name"); //$NON-NLS-1$
        NAME_PROPERTY.setPluralDisplayName("Name"); //$NON-NLS-1$
        NAME_PROPERTY.setShortDisplayName("Name"); //$NON-NLS-1$
        NAME_PROPERTY.setShortDescription("The name of the file or directory"); //$NON-NLS-1$
        NAME_PROPERTY.setDefaultValue(""); //$NON-NLS-1$
        NAME_PROPERTY.setExpert(false);
        NAME_PROPERTY.setModifiable(true);

        SIZE_PROPERTY = new PropertyDefinitionImpl("size",PropertyType.STRING,true); //$NON-NLS-1$
        SIZE_PROPERTY.setDisplayName("Size"); //$NON-NLS-1$
        SIZE_PROPERTY.setPluralDisplayName("Size"); //$NON-NLS-1$
        SIZE_PROPERTY.setShortDisplayName("Size"); //$NON-NLS-1$
        SIZE_PROPERTY.setShortDescription("The size of the file or directory"); //$NON-NLS-1$
        SIZE_PROPERTY.setDefaultValue("0KB"); //$NON-NLS-1$
        SIZE_PROPERTY.setExpert(false);
        SIZE_PROPERTY.setModifiable(false);

        LAST_MODIFIED_PROPERTY = new PropertyDefinitionImpl("modified",PropertyType.TIMESTAMP,true); //$NON-NLS-1$
        LAST_MODIFIED_PROPERTY.setDisplayName("Modified"); //$NON-NLS-1$
        LAST_MODIFIED_PROPERTY.setPluralDisplayName("Modified"); //$NON-NLS-1$
        LAST_MODIFIED_PROPERTY.setShortDisplayName("Modified"); //$NON-NLS-1$
        LAST_MODIFIED_PROPERTY.setShortDescription("The time of last modification"); //$NON-NLS-1$
        LAST_MODIFIED_PROPERTY.setDefaultValue(null);
        LAST_MODIFIED_PROPERTY.setExpert(false);
        LAST_MODIFIED_PROPERTY.setModifiable(false);

        READ_ONLY_PROPERTY = new PropertyDefinitionImpl("readOnly",PropertyType.BOOLEAN,true); //$NON-NLS-1$
        READ_ONLY_PROPERTY.setDisplayName("Read Only"); //$NON-NLS-1$
        READ_ONLY_PROPERTY.setPluralDisplayName("Read Only"); //$NON-NLS-1$
        READ_ONLY_PROPERTY.setShortDisplayName("RO"); //$NON-NLS-1$
        READ_ONLY_PROPERTY.setShortDescription("Whether the file is read-only"); //$NON-NLS-1$
        READ_ONLY_PROPERTY.setExpert(false);
        READ_ONLY_PROPERTY.setModifiable(false);
        READ_ONLY_PROPERTY.setAllowedValues( PropertyDefinitionImpl.BOOLEAN_ALLOWED_VALUES );
        READ_ONLY_PROPERTY.setDefaultValue( PropertyDefinitionImpl.BOOLEAN_ALLOWED_VALUES.get(0).toString() );

        IS_HIDDEN_PROPERTY = new PropertyDefinitionImpl("hidden",PropertyType.BOOLEAN,true); //$NON-NLS-1$
        IS_HIDDEN_PROPERTY.setDisplayName("Hidden"); //$NON-NLS-1$
        IS_HIDDEN_PROPERTY.setPluralDisplayName("Hidden"); //$NON-NLS-1$
        IS_HIDDEN_PROPERTY.setShortDisplayName("Hidden"); //$NON-NLS-1$
        IS_HIDDEN_PROPERTY.setShortDescription("Whether the file is hidden"); //$NON-NLS-1$
        IS_HIDDEN_PROPERTY.setExpert(false);
        IS_HIDDEN_PROPERTY.setModifiable(false);
        IS_HIDDEN_PROPERTY.setAllowedValues( PropertyDefinitionImpl.BOOLEAN_ALLOWED_VALUES );
        IS_HIDDEN_PROPERTY.setDefaultValue( PropertyDefinitionImpl.BOOLEAN_ALLOWED_VALUES.get(0).toString() );

    }

    /**
     * The type of DirectoryEntry that represents a folder or container.
     */
    private File file;
    private Map properties = new HashMap(3);
    private long lastTimePropertiesLoaded;

    public FileSystemEntry( File file ) throws IOException {
        if ( PROPERTY_DEFINITIONS.size() == 0 ) {
            PROPERTY_DEFINITIONS.addAll( getPropertyDefinitionList() );
        }
        ArgCheck.isNotNull(file);

        // Make an instance of the file on the file system if one does not exist
        this.makeExist(file,TYPE_FILE);
        this.file = file;
    }

    protected FileSystemEntry( File file, ObjectDefinition type ) throws IOException {
        if ( PROPERTY_DEFINITIONS.size() == 0 ) {
            PROPERTY_DEFINITIONS.addAll( getPropertyDefinitionList() );
        }
        ArgCheck.isNotNull(file);
        ArgCheck.isNotNull(type);
        Assertion.assertTrue( ((type instanceof FileDefinition)||(type instanceof FolderDefinition)),"The ObjectDefinition must be of type FileDefinition or FolderDefinition"); //$NON-NLS-1$

        // Make an instance of the file on the file system if one does not exist
        this.makeExist(file, type);
        this.file = file;
    }

    protected File getFile() {
        return this.file;
    }

    protected static List getPropertyDefinitionList() {
        List result = new ArrayList(3);
        result.add(NAME_PROPERTY);
        result.add(SIZE_PROPERTY);
        result.add(LAST_MODIFIED_PROPERTY);
        result.add(READ_ONLY_PROPERTY);
        result.add(IS_HIDDEN_PROPERTY);
        return result;
    }

    /**
     * Set the marked state of the TreeNode entry.
     * @param marked the marked state of the entry.
     */
    protected void setMarked( boolean marked) {
        this.marked = marked;
    }

    /**
     * Return the marked state of the specified entry.
     * @return the marked state of the entry.
     */
    protected boolean isMarked() {
        return this.marked;
    }

    /**
     * Get the definitions of the properties for the DirectoryEntry instances
     * returned from this view.
     * @return the unmodifiable set or PropertyDefinition instances; never null
     */
    protected List getPropertyDefinitions() {
        return UNMODIFIABLE_PROPERTY_DEFINITIONS;
    }

    /**
     * This method is used to determine which, if any, of the property definitions for this object
     * are used to access the name.
     * By default, this method returns null, meaning there is no name property definition.
     */
    protected PropertyDefinition getNamePropertyDefinition() {
        return NAME_PROPERTY;
    }

    /**
     * This method is used to determine which, if any, of the property definitions for this object
     * are used to access the description.
     * By default, this method returns null, meaning there is no description property definition.
     */
    protected PropertyDefinition getDescriptionPropertyDefinition() {
        return null;
    }

    /**
     * Return whether it is possible to write to this DirectoryEntry,
     * and whether an output stream can be obtained for this entry.
     * This method may return true even if this entry does not exist (i.e., <code>exists()<code>
     * returns true).
     * @return true if writing is possible, as well as whether an OutputStream instance
     * can be obtained by calling the <code>getOutputStream()</code> method.
     */
    public boolean canWrite() {
        if ( this.file.exists() ) {
            return this.file.canWrite();
        }
        return true;
    }

    /**
     * Return whether it is possible to read from this DirectoryEntry,
     * and whether an input stream can be obtained for this entry.
     * This method always returns false if this entry does not exist (i.e., <code>exists()<code>
     * returns false).
     * @return true if reading is possible, as well as whether an InputStream instance
     * can be obtained by calling the <code>getInputStream()</code> method.
     */
    public boolean canRead() {
        if ( this.file.exists() ) {
            return this.file.canRead();
        }
        return true;
    }

    /**
     * Helper method to find the path for the parent folder.  If this file is a folder,
     * return the path for this file.
     * @return String path for parent folder
     */
    public String getParentFolderPath(){
        if(file == null){
            return null;
        }

        if(isFolder() ){
            return file.getAbsolutePath();
        }

        return file.getParentFile().getAbsolutePath();
    }

    protected boolean isValidValue( PropertyDefinition definition, Object value ) {
    	ArgCheck.isNotNull(definition);

        // A null value is not allowed if multiplicity is 1 or more ...
        if ( value == null && definition.isRequired() ) {
            return false;
        }

        // If the property was not modifiable, then the value is always valid
        if ( ! definition.isModifiable() ) {
            return true;
        }

        if ( definition == NAME_PROPERTY ) {
            File newFile = new File(this.file.getParentFile(),value.toString());
            return ! newFile.exists();
        }
        if ( definition == SIZE_PROPERTY ) {
            return false;       // not modifiable
        }
        if ( definition == LAST_MODIFIED_PROPERTY ) {
            if ( ! this.file.exists() ) {
                return false;
            }
            long lastModified = this.file.lastModified();
            boolean result = false;
            try {
                long newValue = 0;
                if ( value instanceof Long ) {
                    Long longValue = (Long) value;
                    newValue = longValue.longValue();
                } else if ( value instanceof Date ) {
                    Date dateValue = (Date) value;
                    newValue = dateValue.getTime();
                } else {
                    newValue = Long.parseLong(value.toString());
                }
                result = this.file.setLastModified(newValue);
                this.file.setLastModified(lastModified);        // set it back
            } catch ( Exception e ) {
            }
            return result;
        }
        if ( definition == READ_ONLY_PROPERTY ) {
            boolean readOnlyValue = false;
            if ( value instanceof Boolean ) {
                Boolean roValue = (Boolean) value;
                readOnlyValue = roValue.booleanValue();
            } else {
                Boolean roValue = Boolean.valueOf(value.toString());
                readOnlyValue = roValue.booleanValue();
            }
            if ( !readOnlyValue && this.file.canWrite() == false ) {
                return false;       // can't change it to false (writable)
            }
            return true;
        }
        return false;
    }

    /**
     * Return whether it is possible to delete this DirectoryEntry.
     * @return true if deletion is possible, or false otherwise.
     */
    boolean delete() {
        return this.file.delete();
    }

    /**
     * Return whether this DirectoryEntry represents an existing resource.
     * @return true the entry exists, or false otherwise.
     */
    public boolean exists() {
        return this.file.exists();
    }

    /**
     * Obtain the name of this DirectoryEntry.
     * @return the name of this entry; never null
     */
    public String getName() {
        return this.file.getName();
    }

    /**
     * Obtain the full name of the DirectoryEntry which is unique within the DirectoryEntryView.
     * The full name is the concatenation of the namespace, separator, and
     * name representing the abstract path for this DirectoryEntry.
     * @return the fully qualified name of this entry; never null
     */
    public String getFullName() {
        return this.file.getPath();
    }

    /**
     * Obtain the namespace to which this DirectoryEntry belongs. The separator
     * character is used between each of the components of the namespace.  The namespace
     * for a DirectoryEntry represents the absolute path minus the entry name.
     * @return the string that represents the namespace of this entry; never null
     */
    public String getNamespace() {
        return this.file.getAbsolutePath();
    }

    /**
     * Obtain the character that is used to separate names in a path sequence for
     * the absolute path.  This character is completely dependent upon the implementation.
     * @return the charater used to delimit names in the absolute path.
     */
    public char getAbsoluteSeparatorChar() {
        return File.separatorChar;
    }

    /**
     * Obtain the character (as a String) that is used to separate names in a path sequence for
     * the abstract path.  This character is completely dependent upon the implementation.
     * @return the string containing the charater used to delimit names in the abstract path; never null
     */
    public String getAbsoluteSeparator() {
        return File.separator;
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
     * Load property values associated with this DirectoryEntry and return whether
     * the preview properties are now available.
     * @return if the properties have been loaded.
     */
    public boolean loadPreview() {
        this.loadProperties();
        return true;
    }

    /**
     * If this DirectoryEntry is readable, then return an InputStream instance to
     * the resource represented by this entry.
     * @return the InputStream for this entry.
     * @throws AssertionError if this method is called on a DirectoryEntry
     * for which <code>canRead()</code> returns false.
     * @throws IOException if there was an error creating the stream
     */
    public InputStream getInputStream() throws IOException {
        if ( ! this.canRead() ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0039, this.getFullName() ));
        }
        return new FileInputStream(this.file);
    }

    /**
     * If this DirectoryEntry is writable, then return an OutputStream instance to
     * the resource represented by this entry.
     * @return the OutputStream for this entry.
     * @throws AssertionError if this method is called on a DirectoryEntry
     * for which <code>canWrite()</code> returns false.
     * @throws IOException if there was an error creating the stream
     */
    public OutputStream getOutputStream() throws IOException {
        if ( ! this.canWrite() ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0040, this.getFullName() ));
        }
        return new FileOutputStream(this.file);
    }

    /**
     * Determine whether this DirectoryEntry is of type FolderDefinition, meaning
     * it represents a container that may have children.
     * @return true if <code>getType()<code> returns FolderDefinition, or false otherwise.
     */
    public boolean isFolder() {
        return this.file.isDirectory();
    }

	public boolean isFile() {
		return this.file.isFile();
	}

    public boolean isEmpty() {
        return this.file.length() == 0;
    }

    /**
     * Determine the ObjectDefinition type of this DirectoryEntry.
     * @return either FolderDefinition or FileDefinition.
     */
    public ObjectDefinition getType() {
        return this.objectDefn;
    }

    /**
     * Renames this DirectoryEntry to the specified new name.  If this entry
     * represents an existing resource, the underlying resource is changed.
     * @return true if this entry was renamed, or false otherwise.
     */
    protected boolean renameTo(String newName) {
    	ArgCheck.isNotNull(newName);
    	ArgCheck.isNotZeroLength(newName);

        //The java.io.File.renameTo() method will rename the actual file but not
        //change the value of the File object referenced by this FileSystemEntry
        //so it needs to be reset manually
        File newFile = new File(this.file.getParent(), newName);
        boolean result = this.file.renameTo(newFile);
        this.file = newFile;
        return result;
    }

    /**
     * Moves this DirectoryEntry under the specified folder.  If this entry
     * represents an existing resource, the underlying resource is changed.
     * @return true if this entry was moved, or false otherwise.
     */
    protected boolean move(FileSystemEntry newParent) {
    	ArgCheck.isNotNull(newParent);
        Assertion.assertTrue(newParent.exists(),"The parent FileSystemEntry reference must represent an existing folder"); //$NON-NLS-1$
        Assertion.assertTrue(newParent.isFolder(),"The parent FileSystemEntry reference must represent an existing folder"); //$NON-NLS-1$

        //The java.io.File.renameTo() method will rename the actual file but not
        //change the value of the File object referenced by this FileSystemEntry
        //so it needs to be reset manually
        File newFile = new File(newParent.getFile(), this.file.getName());
        boolean success = this.file.renameTo(newFile);
		if (success) {
		    this.file = newFile;
		}
        return success;
    }

    /**
     * Creates a copy of this DirectoryEntry under the specified folder.
     * If this entry represents an existing resource, the underlying
     * resource is copied.
     * @return a reference to the copied entry or null if the copy
     * operation was not successful.
     */
    protected TreeNode copy(FileSystemEntry newParent) {
    	ArgCheck.isNotNull(newParent);

        File newFile = new File(newParent.getFile(), this.file.getName());

        // Make sure the file exists, is a file, and is readable
        if ( !this.file.exists() || !this.file.isFile() || !this.file.canRead() ) {
            return null;
        }
        // Make sure the destination file does not exist - method will not overwrite
        if (newFile.exists()) {
            return null;
        }
        // Make sure the destination folder exists and is writable
        if ( !newParent.exists() || !newParent.isFolder() || !newParent.canWrite() ) {
            return null;
        }

        // Copy the underlying resource
	    FileInputStream istream  = null;
	    FileOutputStream ostream = null;
	    FileSystemEntry newEntry = null;
        try {
	        istream = (FileInputStream) this.getInputStream();
	        ostream = new FileOutputStream( newFile );
	        byte[] buffer = new byte[4096];
	        int bytesRead;
	        while ( (bytesRead = istream.read(buffer)) != -1) {
	            ostream.write(buffer,0,bytesRead);
	        }
	        newEntry = new FileSystemEntry(newFile,this.getType());
        } catch (IOException e) {
            e.printStackTrace(System.out);
            return null;
        } finally {
	        if (istream != null) {
	            try {
	                istream.close();
	            } catch (IOException e) {
	            }
	        }
	        if (ostream != null) {
	            try {
	                ostream.close();
	            } catch (IOException e) {
	            }
	        }
        }
        return newEntry;
    }

    /**
     * Converts this abstract pathname into a URL.  The exact form of the URL
     * is dependent upon the implementation. If it can be determined that the
     * file denoted by this abstract pathname is a directory, then the resulting
     * URL will end with a slash.
     * @return the URL for this DirectoryEntry.
     * @throws MalformedURLException if the URL is malformed.
     */
    public URL toURL() throws MalformedURLException {
        return this.file.toURL();
    }

    /**
     * Compares this object to another. If the specified object is
     * an instance of the DirectoryEntry class, then this
     * method compares the contents; otherwise, it throws a
     * ClassCastException (as instances are comparable only to
     * instances of the same
     *  class).
     * <p>
     * Note:  this method <i>is</i> consistent with
     * <code>equals()</code>, meaning that
     * <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        if ( obj == null ) {
            return 1;	// this is greater than null
        }
        FileSystemEntry entry = (FileSystemEntry) obj;      // will throw class cast exception
        return this.file.compareTo(entry.getFile());
    }
    /**
     * Returns true if the specified object is semantically equal
     * to this instance.
     *   Note:  this method is consistent with
     * <code>compareTo()</code>.
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        if ( obj == null ) {
            return false;
        }
        if (obj instanceof File ) {
            return this.file.equals(obj);
        }
        if (!(obj instanceof FileSystemEntry)) {
            return false;
        }
        FileSystemEntry entry = (FileSystemEntry) obj;
        return this.file.equals(entry.getFile());
    }

    /**
     * Returns the hash code value for this object.
     * @return a hash code value for this object.
     */
    public int hashCode() {
		return this.file.hashCode();
    }

    /**
     * Return the string form of this DirectoryEntry.
     * @return the stringified abstract path, equivalent to <code>getPath</code>
     */
    public String toString() {
        return this.file.toString();
    }

    /**
     * Load the properties for the file.  Return whether the file's modification time
     * has changed since the last time this method was called.
     * @return true if the file has been modified since the last time this
     * method was called.
     */
    protected boolean loadProperties() {
        long lastModified = this.file.lastModified();
        this.properties.put(NAME_PROPERTY,this.getName());
        this.properties.put(SIZE_PROPERTY,getFileSizeValue(this.file.length()));
        this.properties.put(LAST_MODIFIED_PROPERTY,new Date(lastModified));
        this.properties.put(READ_ONLY_PROPERTY,Boolean.valueOf(!this.file.canWrite()));
        this.properties.put(IS_HIDDEN_PROPERTY,Boolean.valueOf(!this.file.canRead()));

        boolean changedSinceLastTime = false;
        if ( this.lastTimePropertiesLoaded < lastModified ) {
            this.lastTimePropertiesLoaded = System.currentTimeMillis();
            changedSinceLastTime = true;
        }
        return changedSinceLastTime;
    }

    private void loadProperty( PropertyDefinition defn ) {
    	ArgCheck.isNotNull(defn);
        if ( defn == NAME_PROPERTY ) {
            this.properties.put(NAME_PROPERTY,this.getName());
        }
        if ( defn == SIZE_PROPERTY ) {
            this.properties.put(SIZE_PROPERTY,getFileSizeValue(this.file.length()));
        }
        if ( defn == LAST_MODIFIED_PROPERTY ) {
            this.properties.put(LAST_MODIFIED_PROPERTY,new Date(this.file.lastModified()));
        }
        if ( defn == READ_ONLY_PROPERTY ) {
            this.properties.put(READ_ONLY_PROPERTY,Boolean.valueOf(!this.file.canWrite()));
        }
        if ( defn == IS_HIDDEN_PROPERTY ) {
            this.properties.put(IS_HIDDEN_PROPERTY,Boolean.valueOf(!this.file.canRead()));
        }
    }

    /**
     * Obtain the property value for the file size using the most appropriate
     * unit (one of the following: "KB", "MB", "GB")
     * @param length the size of the file in bytes
     * @return the file size string
     */
    private String getFileSizeValue( long length ) {
        // If the length is zero (or less than zero) ...
        if ( length <= 0 ) {
            return "0KB"; //$NON-NLS-1$
        }

        // If the length is between 1 and 1024 bytes,
        // compute the number of kilobytes ...
        if ( length <= 1024 ) {
            return "1KB"; //$NON-NLS-1$
        }

        // If the length is greater than 1024 bytes,
        // compute the number of kilobytes ...
        double temp = 0;
        long result = 0;

        // Compute the number of KB ...
        temp = length / 1024.0f;
        result = Math.round(temp);
        return FORMATTER.format(result) + "KB"; //$NON-NLS-1$
    }

    protected boolean hasChildWithName( String name ) {
        File potentialChild = new File(this.file,name);
        return potentialChild.exists();
    }

    /**
     * Set this object's value for the property defined by the specified PropertyDefinition.
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be changed; may not be null
     * @param value the new value for the property; the cardinality and type
     * must conform PropertyDefinition
     * @throws AssertionError if the property definition reference
     * is null, or if the object is read only.
     * @throws IllegalArgumentException if the value does not correspond
     * to the PropertyDefinition requirements.
     */
    protected void setValue(PropertyDefinition def, Object value) {
    	ArgCheck.isNotNull(def);

        try {
            if ( def == NAME_PROPERTY ) {
                if ( this.renameTo(value.toString()) ) {
                    throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0044, this.getName(), value ));
                }
                //this.file.renameTo( new File( this.file.getParent(), value.toString() ) );
            }
            if ( def == SIZE_PROPERTY ) {
                throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0045));
            }
            if ( def == LAST_MODIFIED_PROPERTY ) {
                if ( ! this.file.exists() ) {
                    throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0046));
                }
                try {
                    long newValue = 0;
                    if ( value instanceof Long ) {
                        Long longValue = (Long) value;
                        newValue = longValue.longValue();
                    } else if ( value instanceof Date ) {
                        Date dateValue = (Date) value;
                        newValue = dateValue.getTime();
                    } else {
                        newValue = Long.parseLong(value.toString());
                    }
                    if ( this.file.setLastModified(newValue) ) {
                        throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0047, LAST_MODIFIED_PROPERTY.getDisplayName(), (new Date(newValue))) );
                    }
                } catch ( IllegalArgumentException e ) {
                    throw e;
                } catch ( Exception e ) {
                }
            }
            if ( def == READ_ONLY_PROPERTY ) {
                boolean readOnlyValue = false;
                if ( value instanceof Boolean ) {
                    Boolean roValue = (Boolean) value;
                    readOnlyValue = roValue.booleanValue();
                } else {
                    Boolean roValue = Boolean.valueOf(value.toString());
                    readOnlyValue = roValue.booleanValue();
                }
                if ( !readOnlyValue ) {
                    if ( this.file.canWrite() == false ) {
                        throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0047, READ_ONLY_PROPERTY.getDisplayName(),Boolean.valueOf(readOnlyValue) ));
                    }
                } else {
                    if ( this.file.canWrite() ) {
                        this.file.setReadOnly();
                    }
                }
            }
        } catch ( IllegalArgumentException e ) {
            throw e;
        } catch ( Exception e ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0048,def.getDisplayName(), value.toString()));
        }
        this.loadPreview();
    }

    /**
     * Return the value for this objects's property that corresponds to
     * the specified PropertyDefinition.  The return type and cardinality
     * depend upon the PropertyDefinition.
     * @param def the reference to the PropertyDefinition describing the
     * property whose value is to be returned; may not be null
     * @return the value for the property, which may be a collection if
     * the property is multi-valued, or may be null if the multiplicity
     * includes "0"
     * @throws AssertionError if the property definition reference
     * is null
     */
    protected Object getValue(PropertyDefinition def) {
    	ArgCheck.isNotNull(def);
        this.loadProperty(def);
        // If this entry is a folder and the definition is for the size, then return null
        if ( def == SIZE_PROPERTY && this.isFolder() ) {
            return null;
        }
        // Otherwise, return the property value
        return this.properties.get(def);
    }

    /**
     * Return whether this object is read only and may not be modified.
     * @return true if this object may not be modified, or false otherwise.
     */
    public boolean isReadOnly() {
        if ( this.exists() && this.canWrite() ) {
            return false;
        }
        return true;
    }

    /**
     * Return whether this node has undergone changes.  This method always returns false.
     * @return true if this TreeNode has changes, or false otherwise.
     */
    public boolean isModified() {
        return false;
    }

    /**
     * Return the last modification time property value
     * @return the date for the last modification to the file; never null
     */
    public Date getLastModifiedDate() {
        Object result = this.properties.get(LAST_MODIFIED_PROPERTY);
        if(result == null){
            this.properties.put(LAST_MODIFIED_PROPERTY, new Date(this.file.lastModified()));
        }
        return (Date)this.properties.get(LAST_MODIFIED_PROPERTY);
    }

    /**
     * Check for the existance of the specified <code>File</code> and creates the
     * underlying resource on the file system if one does not exist.
     * @param f the file to be created if it does not exist on the
     * file system; may not be null
     * @param type the type to create; must be either FileDefintion or
     * FolderDefinition and may not be null
     * @return true if the file was successfully created (made to exist); false
     * if the file already exists or if the file could not be created
     * @throws AssertionError if <code>obj</code>or <code>type</code> is null
     */
    private boolean makeExist(File f, ObjectDefinition type) throws IOException {
    	ArgCheck.isNotNull(f);
    	ArgCheck.isNotNull(type);
        if (f.exists()) {
            if ( f.isDirectory() && (type instanceof FolderDefinition)) {
                this.objectDefn = type;
            } else if (f.isFile() && (type instanceof FileDefinition)) {
                this.objectDefn = type;
            } else {
                throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0049, f.getPath()));
            }
            return false;
        }
        boolean result = false;
        try {
            if ( type instanceof FolderDefinition ) {
                this.objectDefn = type;
                result = f.mkdirs();
            } else if (type instanceof FileDefinition) {
                this.objectDefn = type;
                result = f.createNewFile();
            } else {
                result = false;
                throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0050, type.getName() ));
            }
        } catch ( IOException e ) {
            result = false;
            throw e;
        }
        return result;
    }

    /**
     * Set the last modification time property value
     * @return true if the last modification time is set successfully, false otherwise.
     */
    public boolean setLastModifiedDate(Date date) {
        if ( ! this.canWrite() ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0039, this.getFullName() ));
        }
        boolean result = this.file.setLastModified(date.getTime());
        if(result)
            this.properties.put(LAST_MODIFIED_PROPERTY, date);
        return result;
    }

    /**
     * Create a ZipFile instance if the entry represents a zip file
     * @return a ZipFile instance. null if the entry is a folder or the file size is 0.
     * @throws AssertionError if this method is called on a DirectoryEntry
     * for which <code>canRead()</code> returns false.
     * @throws IOException if there was an error creating the ZipFile.
     */
    public ZipFile getZipFile() throws IOException{
        if ( ! this.canRead() ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0040, this.getFullName() ));
        }
        if(isFolder() || (file.length() == 0))
            return null;

        ZipFile result = null;
        try{
            result =  new ZipFile(file);
        }catch(Exception e){
            throw new IOException(e.getMessage());
        }
        return result;
    }
}

