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

package com.metamatrix.platform.security.audit.config;

import java.io.Serializable;
import java.util.*;

import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.audit.AuditConfiguration;
import com.metamatrix.platform.security.audit.AuditLevel;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class BasicAuditConfiguration implements AuditConfiguration, Serializable {

    private Set discardedContexts = null;
    private Set unmodifiableContexts = null;
    private int auditLevel;

	public BasicAuditConfiguration( Collection contexts, int auditLevel ) {
        if ( ! AuditLevel.isAuditLevelValid(auditLevel) ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0005, auditLevel));
        }
        this.auditLevel = auditLevel;
        if ( contexts != null ) {
            this.discardedContexts = new HashSet(contexts);
        } else {
            this.discardedContexts = new HashSet();
        }
        this.unmodifiableContexts = Collections.unmodifiableSet(this.discardedContexts);
	}

	public BasicAuditConfiguration( AuditConfiguration config ) {
        this.setAuditLevel(config.getAuditLevel());
        this.discardedContexts = new HashSet( config.getDiscardedContexts() );
        this.unmodifiableContexts = Collections.unmodifiableSet(this.discardedContexts);
	}

	public BasicAuditConfiguration() {
        this.auditLevel = AuditLevel.DEFAULT_AUDIT_LEVEL;
        this.discardedContexts = new HashSet();
        this.unmodifiableContexts = Collections.unmodifiableSet(this.discardedContexts);
	}

	public BasicAuditConfiguration( Collection contexts ) {
        this(contexts,AuditLevel.DEFAULT_AUDIT_LEVEL);
	}

	public BasicAuditConfiguration( int auditLevel ) {
        this(null,auditLevel);
	}

    public boolean isContextDiscarded( String context ) {
        return ( context != null && this.discardedContexts.contains(context) );
    }

    public boolean isLevelDiscarded( int level ) {
        return ( level > auditLevel );
    }

    /**
     * Obtain the set of contexts for messages that are to be discarded.
     * If this method returns an empty set, then messages in all contexts
     * are being recorded; if not empty, then messages with a context in the
     * returned set are discarded and messages for all other contexts recorded.
     * @return the set of contexts for messages that are to be discarded
     */
    public Set getDiscardedContexts() {
        return this.unmodifiableContexts;
    }

    /**
     * Specify that messages with the input contexts should be discarded
     * and not recorded.
     * @param contexts the set of contexts that are to be ignored for logging;
     * this method does nothing if the reference is null or empty
     */
    public void discardContexts( Collection contexts ) {
        if ( contexts != null ) {
            Iterator iter = contexts.iterator();
            while ( iter.hasNext() ) {
                this.discardedContexts.add(iter.next().toString());
            }
        }
    }

    /**
     * Specify that messages with the input context should be discarded
     * and not recorded.
     * @param context the context to add to the set; this method does nothing
     * if the reference is null
     */
    public void discardContext( String context ) {
        if ( context != null ) {
            this.discardedContexts.add(context);
        }
    }

    /**
     * Specify that messages in all contexts are to be recorded.
     */
    public void recordAllContexts() {
        this.discardedContexts.clear();
    }

    /**
     * Specify that messages in the input context should be recorded rather than
     * discarded.
     * @param context the context for messages that should be recorded; this
     * method does nothing if the reference is null
     */
    public void recordContext( String context ) {
        if ( context != null ) {
            this.discardedContexts.remove(context);
        }
    }
     /**
     * Specify that messages in the input contexts should be recorded rather than
     * discarded.
     * @param contexts the set of contexts that are to be recorded;
     * this method does nothing if the reference is null or empty
     */
    public void recordContexts( Collection contexts ) {
        if ( contexts != null ) {
            Iterator iter = contexts.iterator();
            while ( iter.hasNext() ) {
                this.discardedContexts.remove(iter.next().toString());
            }
        }
    }

	/**
	 * Get the level of detail of messages that are currently being recorded.
	 * @return the level of detail
	 */
    public int getAuditLevel() {
        return auditLevel;
    }

    /**
     * Method to set the level of messages that are recorded for this VM.
     * @param newAuditLevel the new level; must be either
     *    <code>AuditLevel.NONE</code>,
     *    <code>AuditLevel.FULL</code>
     * @throws IllegalArgumentException if the level is out of range.
     */
    public void setAuditLevel( int newAuditLevel ) {
        if ( ! AuditLevel.isAuditLevelValid(newAuditLevel) ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0005, newAuditLevel));
        }
        auditLevel = newAuditLevel;
    }

    /**
     * Compares this object to another. If the specified object is an instance of
     * the MetadataID class, then this method compares the name; otherwise, it throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).
     * Note:  this method <i>is</i> consistent with <code>equals()</code>, meaning
     * that <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        AuditConfiguration that = (AuditConfiguration) obj;     // May throw ClassCastException
        if ( obj == null ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0012));
        }

        // Check the message level first ...
        int diff = this.getAuditLevel() - that.getAuditLevel();
        if ( diff != 0 ) {
            return diff;
        }

        // Check the contexts ...
        boolean sizesMatch = this.getDiscardedContexts().size() == that.getDiscardedContexts().size();
        boolean thisContainsThat = this.getDiscardedContexts().containsAll( that.getDiscardedContexts() );
        if ( thisContainsThat ) {
            if ( sizesMatch ) {
                return 0;   // they are equal
            }
            return 1;   // this has all of that plus more
        }
        return -1;
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        //if ( this.getClass().isInstance(obj) ) {
        if ( obj instanceof AuditConfiguration ) {
            AuditConfiguration that = (AuditConfiguration) obj;

            // Check the message level first ...
            int diff = this.getAuditLevel() - that.getAuditLevel();
            if ( diff != 0 ) {
                return false;
            }

            // Check the contexts ...
            boolean sizesMatch = this.getDiscardedContexts().size() == that.getDiscardedContexts().size();
            boolean thisContainsThat = this.getDiscardedContexts().containsAll( that.getDiscardedContexts() );
            if ( thisContainsThat && sizesMatch ) {
                return true;    // they are equal
            }
        }

        // Otherwise not equal ...
        return false;
    }

	/**
	 * String representation of logging configuration.
	 * @return String representation
	 */
	public String toString() {
		StringBuffer str = new StringBuffer("AuditConfiguration: {"); //$NON-NLS-1$
        str.append( AuditLevel.getLabelForLevel(auditLevel) );
		str.append(":DiscardedContexts["); //$NON-NLS-1$
        Iterator iter = this.discardedContexts.iterator();
        if ( iter.hasNext() ) {
			str.append(iter.next().toString());
        }
        while ( iter.hasNext() ) {
			str.append(',');
			str.append(iter.next().toString());
        }
		str.append("]}"); //$NON-NLS-1$
		return str.toString();
	}

    public Object clone() {
        return new BasicAuditConfiguration(this.discardedContexts,this.auditLevel);
    }
}
