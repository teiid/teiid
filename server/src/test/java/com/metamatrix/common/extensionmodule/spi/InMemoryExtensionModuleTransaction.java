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

package com.metamatrix.common.extensionmodule.spi;

import java.io.Serializable;
import java.util.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.connection.*;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.exception.*;
import com.metamatrix.common.extensionmodule.spi.ExtensionModuleTransaction;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.DateUtil;

/**
 * This class is intended for <i>test purposes only</i>.
 */
public class InMemoryExtensionModuleTransaction extends BaseTransaction implements ExtensionModuleTransaction {

    private static final String LOG_CONTEXT = "IN_MEMORY_EXTENSION_SOURCE"; //$NON-NLS-1$

    //persistant caches across instances of this transaction
    private static Map sourceCache = new HashMap();
    private static Map descCache = new HashMap();
    private static List sourceNames = new ArrayList();

    private static class Desc extends ExtensionModuleDescriptor implements Serializable{
        private String name;
        private String type;
        private int position;
        private boolean enabled;
        private String desc;
        private String createdBy;
        private String creationDate;
        private String lastUpdatedBy;
        private String lastUpdatedDate;
        private long checksum;

        private Desc(){}
        private Desc(Desc aDesc){
            this.name = aDesc.name;
            this.type = aDesc.type;
            this.position = aDesc.position;
            this.enabled = aDesc.enabled;
            this.desc = aDesc.desc;
            this.createdBy = aDesc.createdBy;
            this.creationDate = aDesc.creationDate;
            this.lastUpdatedBy = aDesc.lastUpdatedBy;
            this.lastUpdatedDate = aDesc.lastUpdatedDate;
            this.checksum = aDesc.checksum;
        }

        public String getName(){return name;}
        public String getType(){return type;}
        public int getPosition(){return position;}
        public boolean isEnabled(){return enabled;}
        public String getDescription(){return desc;}
        public String getCreatedBy(){return createdBy;}
        public String getCreationDate(){return creationDate;}
        public String getLastUpdatedBy(){return lastUpdatedBy;}
        public String getLastUpdatedDate(){return lastUpdatedDate;}
        public long getChecksum(){return checksum;}

        /**
         * Currently compares according to just the int position - should
         * be unique for a snapshot in time of all extension sources
         * in the DB.  In this way, these descriptors can be easily ordered
         * according to their search position.
         * @param obj the object that this instance is to be compared to.
         * @return a negative integer, zero, or a positive integer as this object
         * is less than, equal to, or greater than the specified object, respectively.
         * @throws AssertionError if the specified object reference is null
         * @throws ClassCastException if the specified object's type prevents it
         * from being compared to this instance.
         */
        public int compareTo(Object obj){
            ExtensionModuleDescriptor that = (ExtensionModuleDescriptor) obj;     // May throw ClassCastException
            if(obj == null){
                Assertion.isNotNull(obj,"Attempt to compare null"); //$NON-NLS-1$
            }
            
            if ( obj == this ) {
                return 0;
            }
            return this.position - that.getPosition();

        }

        /**
         * Currently bases equality on just the int position - should
         * be unique for a snapshot in time of all extension sources
         * in the DB
         */
        public boolean equals(Object obj){
            // Check if instances are identical ...
            if ( this == obj ) {
                return true;
            }

            // Check if object can be compared to this one
            // (this includes checking for null ) ...
            if ( obj instanceof ExtensionModuleDescriptor ) {
                ExtensionModuleDescriptor that = (ExtensionModuleDescriptor) obj;
                return ( this.position == that.getPosition() );
            }

            // Otherwise not comparable ...
            return false;
        }

        public String toString(){
            return this.name;
        }
    }

    /**
     * Create a new instance of a transaction for a managed connection.
     * @param connection the connection that should be used and that was created using this
     * factory's <code>createConnection</code> method (thus the transaction subclass may cast to the
     * type created by the <code>createConnection</code> method.
     * @param readonly true if the transaction is to be readonly, or false otherwise
     * @throws ManagedConnectionException if there is an error creating the transaction.
     */
    InMemoryExtensionModuleTransaction( ManagedConnection connection, boolean readonly) throws ManagedConnectionException{
        super(connection, readonly);
    }

    //===================================================================
    //PUBLIC INTERFACE
    //===================================================================

    /**
     * Adds an extension source to the end of the list of sources
     * @param principalName name of principal requesting this addition
     * @param type one of the known types of extension file
     * @param sourceName name (e.g. filename) of extension source
     * @param source actual contents of source
	 * @param checksum Checksum of file contents
     * @param description (optional) description of the extension source
     * @param enabled whether source is enabled for search or not
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension source
     * @throws DuplicateExtensionModuleException if an extension source
     * with the same sourceName already exists
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public ExtensionModuleDescriptor addSource(String principalName, String type, String sourceName, byte[] source, long checksum, String description, boolean enabled)
    throws DuplicateExtensionModuleException, MetaMatrixComponentException{
        if (sourceNames.contains(sourceName)){
            throw new DuplicateExtensionModuleException(sourceName);
        }

        sourceNames.add(sourceName);

        String currentDate = DateUtil.getCurrentDateAsString();
        Desc desc = new Desc();
        desc.name = sourceName;
        desc.type = type;
        desc.position = sourceNames.size()-1;
        desc.enabled = enabled;
        desc.desc = description;
        desc.createdBy = principalName;
        desc.creationDate = currentDate;
        desc.lastUpdatedBy = principalName;
        desc.lastUpdatedDate = currentDate;
        desc.checksum = checksum;

        sourceCache.put(sourceName, source);
        descCache.put(sourceName, desc);

        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Added source", sourceName, "to cache."} ); //$NON-NLS-1$ //$NON-NLS-2$
        return new Desc(desc);
    }

    /**
     * Deletes a source from the list of sources
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension source
     * @throws ExtensionModuleNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public void removeSource(String principalName, String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        if (!sourceNames.remove(sourceName)){
            throw new ExtensionModuleNotFoundException(sourceName);
        }
        sourceCache.remove(sourceName);
        descCache.remove(sourceName);
        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Removed source", sourceName, "from cache."} ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Returns List (of Strings) of all extension source names, in order of
     * their search ordering
     * @return List (of Strings) of all extension source names, in order of
     * their search ordering
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public List getSourceNames() throws MetaMatrixComponentException{
        return new ArrayList(sourceNames);
    }

    /**
     * Returns List of ExtensionModuleDescriptor objects, in order
     * of their search ordering
     * @return List of ExtensionModuleDescriptor objects, in order
     * of their search ordering
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public List getSourceDescriptors() throws MetaMatrixComponentException{

        Iterator iter = sourceNames.iterator();
        ArrayList result = new ArrayList();
        Desc desc = null;
        for (; iter.hasNext(); ){
            desc = (Desc)descCache.get(iter.next());
            result.add(new Desc(desc));
        }
        return result;
    }

    /**
     * Returns List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering
     * @param type one of the known types of extension file
     * @param includeDisabled if "false", only descriptors for <i>enabled</i>
     * extension sources will be returned; otherwise all sources will be.
     * @return List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public List getSourceDescriptors(String type, boolean includeDisabled) throws MetaMatrixComponentException{
        Iterator iter = sourceNames.iterator();
        ArrayList result = new ArrayList();
        Desc desc = null;
        for (; iter.hasNext(); ){
            desc = (Desc)descCache.get(iter.next());
            if (desc.type.equals(type)){
                if (includeDisabled || desc.isEnabled() ){
                    result.add(new Desc(desc));

if (!includeDisabled){
LogManager.logTrace(LOG_CONTEXT, "<!><!><!>descriptor:"+ desc.getName()); //$NON-NLS-1$
LogManager.logTrace(LOG_CONTEXT, "<!><!><!>   enabled:"+ desc.isEnabled()); //$NON-NLS-1$
}



                }
            }
        }
        return result;
    }

    /**
     * Returns the ExtensionModuleDescriptor object for the extension
     * source indicated by sourceName
     * @param sourceName name (e.g. filename) of extension source
     * @return the ExtensionModuleDescriptor object for the extension
     * source indicated by sourceName
     * @throws ExtensionModuleNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public ExtensionModuleDescriptor getSourceDescriptor(String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        Desc desc = (Desc)descCache.get(sourceName);
        if (desc == null){
            throw new ExtensionModuleNotFoundException("No extension source named " + sourceName); //$NON-NLS-1$
        }
        return new Desc(desc);
    }

    /**
     * Sets the positions in the search order of all sources (all sources
     * must be included or an ExtensionModuleOrderingException will be thrown)
     * The sourceNames List parameter should indicate the new desired order.
     * @param principalName name of principal requesting this addition
     * @param sourceNames Collection of String names of all existing
     * extension sources whose search position is to be set
     * @return updated List of ExtensionModuleDescriptor objects, in order
     * of their search ordering
     * @throws ExtensionModuleOrderingException if the extension files could
     * not be ordered as requested because another administrator had
     * concurrently added or removed an extension file or files, or because
     * an indicated position is out of bounds.
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public void setSearchOrder(String principalName, List sourceNames)
    throws ExtensionModuleOrderingException, MetaMatrixComponentException{
        if (!InMemoryExtensionModuleTransaction.sourceNames.containsAll(sourceNames) || !sourceNames.containsAll(InMemoryExtensionModuleTransaction.sourceNames)){
            throw new ExtensionModuleOrderingException("The list of sourceNames provided does not match the list in storage."); //$NON-NLS-1$
        }
        InMemoryExtensionModuleTransaction.sourceNames = sourceNames;
     }

    /**
     * Sets the "enabled" (for searching) property of all of the indicated
     * extension sources.
     * @param principalName name of principal requesting this addition
     * @param sourceNames Collection of String names of existing
     * extension sources whose "enabled" status is to be set
     * @param enabled indicates whether each extension source is enabled for
     * being searched or not (for convenience, a source can be disabled
     * without being removed)
     * @return updated List of ExtensionModuleDescriptor objects, in order
     * of their search ordering
     * @throws ExtensionModuleNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public void setEnabled(String principalName, Collection sourceNames, boolean enabled)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        Iterator iter = sourceNames.iterator();
        ArrayList result = new ArrayList();
        Desc desc = null;
        String name = null;
        for (; iter.hasNext(); ){
            name = iter.next().toString();
            try{
                desc = (Desc)descCache.get(name);
                desc.enabled = enabled;
            } catch (NullPointerException e){
                throw new ExtensionModuleNotFoundException("No extension source named " + name); //$NON-NLS-1$
            }
            result.add(new Desc(desc));
        }
    }

    /**
     * Retrieves an extension source in byte[] form
     * @param sourceName name (e.g. filename) of extension source
     * @return actual contents of source in byte[] array form
     * @throws ExtensionModuleNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public byte[] getSource(String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Getting source", sourceName, "from cache..."} ); //$NON-NLS-1$ //$NON-NLS-2$
        byte[] source = (byte[])sourceCache.get(sourceName);
        if (source == null){
            throw new ExtensionModuleNotFoundException("No extension source named " + sourceName); //$NON-NLS-1$
        }
        return source;
    }

    public ExtensionModuleDescriptor setSource(String principalName, String sourceName, byte[] source, long checksum)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        Desc desc = (Desc)descCache.get(sourceName);
        try{
            desc.lastUpdatedBy = principalName;
            desc.lastUpdatedDate = DateUtil.getCurrentDateAsString();
            desc.checksum = checksum;
        } catch (NullPointerException e){
            throw new ExtensionModuleNotFoundException("No extension source named " + sourceName); //$NON-NLS-1$
        }

        sourceCache.put(sourceName, source);
        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Set source", sourceName, "to cache."} ); //$NON-NLS-1$ //$NON-NLS-2$
        return new Desc(desc);
    }

    /**
     * Updates the indicated extension source's source name
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension source
     * @param newName new name for the source
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension source
     * @throws ExtensionModuleNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public ExtensionModuleDescriptor setSourceName(String principalName, String sourceName, String newName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        Desc desc = null;
        try{
            desc = (Desc)descCache.remove(sourceName);
            desc.name = newName;
        } catch (NullPointerException e){
            throw new ExtensionModuleNotFoundException("No extension source named " + sourceName); //$NON-NLS-1$
        }
        descCache.put(newName, desc);

        Object source = sourceCache.remove(sourceName);
        sourceCache.put(newName, source);

        sourceNames.set(sourceNames.indexOf(sourceName), newName);

        LogManager.logTrace(LOG_CONTEXT, new Object[] {"Set new name", newName, "for source name", sourceName} ); //$NON-NLS-1$ //$NON-NLS-2$
        return new Desc(desc);
    }

    /**
     * Updates the indicated extension source's description
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension source
     * @param description (optional) description of the extension source.
     * <code>null</code> can be passed in to indicate no description.
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension source
     * @throws ExtensionModuleNotFoundException if no extension source with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public ExtensionModuleDescriptor setSourceDescription(String principalName, String sourceName, String description)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        Desc desc = null;
        try{
            desc = (Desc)descCache.get(sourceName);
            desc.desc = description;
            desc.lastUpdatedBy = principalName;
            desc.lastUpdatedDate = DateUtil.getCurrentDateAsString();
        } catch (NullPointerException e){
            throw new ExtensionModuleNotFoundException("No extension source named " + sourceName); //$NON-NLS-1$
        }
        return new Desc(desc);
    }
    
	/**
	 * Indicates that ExtensionModuleManager should clear its cache and refresh itself because
	 * the data this object fronts has changed (optional operation).  A service provider 
	 * is not required to keep track of whether data has changed by outside means, in fact
	 * it may not even make sense.
	 * @return whether data has changed since ExtensionModuleManager last accessed this data
	 * store.
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
	 * @throws UnsupportedOperationException - this operation is not supported by this Transaction.
	 */
    public boolean needsRefresh() throws MetaMatrixComponentException, UnsupportedOperationException{
        throw new UnsupportedOperationException();
    }    
    
    
    public boolean isNameInUse(String sourceName) throws MetaMatrixComponentException {
   		if (descCache.containsKey(sourceName)) {
   			return true;
   		}
   		return false;
    	
    }

}



