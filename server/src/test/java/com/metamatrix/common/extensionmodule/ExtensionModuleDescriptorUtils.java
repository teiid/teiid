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

package com.metamatrix.common.extensionmodule;

import com.metamatrix.core.util.Assertion;

/**
 * Common utilies for use with {@link ExtensionModuleDescriptor} implementations.
 */
public class ExtensionModuleDescriptorUtils {

	/*
	 * This class is not meant to be instantiated
	 */
	private ExtensionModuleDescriptorUtils(){}

	/**
	 * <p>Correct implementation of the <code>equals</code> method of the
	 * {@link ExtensionModuleDescriptor} interface, for all implementations 
	 * of that interface.</p>
	 * <p>Using this method thusly:
	 * <pre>
	 * ExtensionModuleDescriptor desc;
	 * Object that;
	 * boolean equal = ExtensionModuleDescriptorUtils.equals(desc, that);
	 * </pre>
 	 * is equivalent to the traditional use of the equals method:
	 * <pre>
	 * ExtensionModuleDescriptor desc;
	 * Object that;
	 * boolean equal = desc.equals(that);
	 * </pre></p>
	 * @param descriptor an ExtensionModuleDescriptor
	 * @param obj an Object to test for equality with descriptor
	 * @return true if the two Objects are semantically equal
	 * @see ExtensionModuleDescriptor#equals
	 */
	public static boolean equals(ExtensionModuleDescriptor descriptor, Object obj){
        // Check if instances are identical ...
        if ( descriptor == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if ( obj instanceof ExtensionModuleDescriptor ) {
            ExtensionModuleDescriptor that = (ExtensionModuleDescriptor) obj;
            return ( descriptor.getPosition() == that.getPosition() );
        }

        // Otherwise not comparable ...
        return false;
	}
	
	public static int compareTo(ExtensionModuleDescriptor descriptor, Object obj){
        ExtensionModuleDescriptor that = (ExtensionModuleDescriptor) obj;     // May throw ClassCastException
        Assertion.isNotNull(obj,"Attempt to compare null"); //$NON-NLS-1$
        if ( obj == descriptor ) {
            return 0;
        }
        return descriptor.getPosition() - that.getPosition();
	}

	public static boolean isEquivalent(ExtensionModuleDescriptor descriptor, Object obj){
		if (!equals(descriptor, obj))return false;
	    ExtensionModuleDescriptor that = (ExtensionModuleDescriptor)obj;
	    if (!descriptor.getName().equals(that.getName())) return false;
	    if (!descriptor.getType().equals(that.getType())) return false;
	    if (!descriptor.getCreatedBy().equals(that.getCreatedBy())) return false;
	    if (!descriptor.getLastUpdatedBy().equals(that.getLastUpdatedBy())) return false;
	    if (!descriptor.getCreationDate().equals(that.getCreationDate())) return false;
	    if (!descriptor.getLastUpdatedDate().equals(that.getLastUpdatedDate())) return false;
	    if (!(descriptor.getChecksum() == that.getChecksum())) return false;
	    if (!(descriptor.isEnabled() == that.isEnabled())) return false;
		if (descriptor.getDescription() == null){
			if (that.getDescription() != null){
			    return false;
			}
		} else {
		    if (that.getDescription() == null){
			    return false;
			}
		    return (descriptor.getDescription().equals(that.getDescription()));
		}
		return true;
	}

	public static String toVerboseString(ExtensionModuleDescriptor descriptor){
		StringBuffer s = new StringBuffer("ExtensionModuleDescriptor " + descriptor.getName() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("  type       : " + descriptor.getType() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("  description: " + descriptor.getDescription() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("  position   : " + descriptor.getPosition() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("  enabled    : " + descriptor.isEnabled() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("  created by : " + descriptor.getCreatedBy() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("  created    : " + descriptor.getCreationDate() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("  updated by : " + descriptor.getLastUpdatedBy() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("  updated    : " + descriptor.getLastUpdatedDate() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("  checksum   : " + descriptor.getChecksum() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
		s.append("(Class: " + descriptor.getClass().getName() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
		return s.toString();
	}

/*
	public static boolean isEquivalentIgnoreSearchOrder(ExtensionModuleDescriptor descriptor, Object obj){
	}

	public static boolean areEquivalent(Collection descriptors, Collection objects){
	}
	
	public static boolean areEquivalentIgnoreSearchOrder(Collection descriptors, Collection objects){
	}
	*/
}
