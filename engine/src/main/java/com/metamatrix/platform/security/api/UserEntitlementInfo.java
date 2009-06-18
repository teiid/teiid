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

package com.metamatrix.platform.security.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Information about a particular entitled <i>Resource</i> (Group or table, Element or column),
 * this class specifies a collection of <i>Principals</i> (users or user groups) - <i>Grantee</i>s
 * that have been granted certain <i>Allowed Actions</i> (one or more of {CREATE, READ, UPDATE, DELETE})
 * on the given <i>Resource</i> by one or more <i>Grantor</i>s (granting authority).
 *
 * <p>There may be multiple <i>Grantee</i>s that have multiple <i>Allowed Actions</i> they are entitled
 * to perform on this Group or Element by multiple <i>Grantor</i>s. This means we have an independantly
 * varying triplet.  We take a user-centered approach so this object contains a <code>Set</code> of
 * {@link GranteeEntitlementEntry}s</p>
 */
public class UserEntitlementInfo implements Serializable {

    // Invariants
    private String VDBName;
    private String VDBVersion;
    private String groupName;
    private String eleName;

    // There may be multiple Grantees that have multiple Allowed Actions
    // entitled to this Group or Element by multiple Grantors.
    // We have an independantly varying triplet.
    // Map this triplet by uppercase name of grantee
    private Map granteeMap;

    /**
     * ctor
     */
    public UserEntitlementInfo(AuthorizationRealm realm, String groupName, String eleName) {
        this.VDBName = realm.getSuperRealmName();
        this.VDBVersion = realm.getSubRealmName();
        this.groupName = groupName;
        this.eleName = eleName;
    }

    /**
     * Does this represent a group or an element entitlement?
     * @return <code>true</code> if this object pertains to a group entitlement,
     * <code>false</code> if it's an element entitlement.
     */
    public boolean isGroupEntitlement() {
        return eleName == null;
    }

    /**
     * Get the name of the VDB this entitlement falls under.
     * @return The VDB name.
     */
    public String getVDBName() {
        return this.VDBName;
    }

    /**
     * Get the version of the VDB this entitlement falls under.
     * @return The VDB version.
     */
    public String getVDBVersion() {
        return this.VDBVersion;
    }

    /**
     * Get the group (table) of this entitlement.
     * @return The group name this entitlement pertains to.
     */
    public String getGroupName() {
        return this.groupName;
    }

    /**
     * Get the element (column) of this entitlement.
     * <p><i><b>Note</b></i>: Will be <code>null</code> if this is a group entitlement.
     * @return The element name this entitlement pertains to.
     */
    public String getElementName() {
        return this.eleName;
    }

    /**
     * Get the number of <i>Grantee</i>s in this entitlement.
     * @return The number of <i>Grantee</i>s this entitlement pertains to.
     */
    public int size() {
        return this.granteeMap.size();
    }

    /**
     * Is the given <i>grantee</i> entitled to perform <i><b>any</b></i>
     * action on the <i>Resource</i> represented by this object?
     * @param grantee The user (or user group) name of inquery.
     * @return <code>true</code> if the <i>grantee</i> can perform one or
     * more actions on this <i>Resource</i>, <code>false</code> if he can
     * perform none.
     */
    public boolean contains(String grantee) {
        if ( this.granteeMap != null && this.granteeMap.containsKey( grantee.toUpperCase()) ) {
            return true;
        }
        return false;
    }

    /**
     * Get the collection of <i>Grantee</i> names possessing this entitlement.
     *
     * <p>Elements of the collection are of type {@link GranteeEntitlementEntry}
     * and are sorted.</p>
     * @return The collection of <i>Grantee</i> names.
     */
    public Collection getGrantees() {
        List granteeList = new ArrayList();
        if ( this.granteeMap != null && this.granteeMap.size() > 0 ) {
            Iterator granteeItr = this.granteeMap.keySet().iterator();
            while ( granteeItr.hasNext() ) {
                Set granteeEntries = (Set) granteeMap.get(granteeItr.next());
                granteeList.addAll(granteeEntries);
            }
            Collections.sort(granteeList);
        }
        return granteeList;
    }

    /**
     * Get an iterator over the <i>Grantee</i>s possessing this entitlement.
     * <br>Note that when iterating over elements, sort order is not guaranteed.</br>
     *
     * <p>Elements of the iterator are of type {@link GranteeEntitlementEntry}.</p>
     * @return The Iterator of <i>Grantee</i>s.
     */
    public Iterator iterator() {
        if ( this.granteeMap == null || this.granteeMap.size() == 0 ) {
            return Collections.EMPTY_SET.iterator();
        }
        return this.getGrantees().iterator();
    }

    /**
     * Add a <i>Grantee</i> -> <i>Grantor</i> -> <i>Allowed Actions</i> triplet.
     * @param grantee A <i>Grantee</i>.
     * @param grantor A <i>Grantor</i>.
     * @param allowedActions The <i>Allowed Actions</i> on the resource granted
     * by the <i>Grantor</i> to the <i>Grantee</i>.
     */
    public void addTriplet(MetaMatrixPrincipalName grantee, String grantor, int allowedActions) {
        GranteeEntitlementEntry anEntry = new GranteeEntitlementEntry(grantee, grantor, allowedActions);
        this.addTriplet(anEntry);
    }

    /**
     * Add a {@link GranteeEntitlementEntry} object.
     * @param newEntry A new entry.
     */
    public void addTriplet(GranteeEntitlementEntry newEntry) {
        if ( this.granteeMap == null ) {
            this.granteeMap = new HashMap();
        }

        String granteeName = newEntry.getGrantee().toUpperCase();

        Set granteeEntries = (Set) this.granteeMap.get(granteeName);
        if ( granteeEntries == null ) {
            // This grantee (principal) has not had a an entry
            // (grantor, actions) added for this resource yet
            granteeEntries = new HashSet();
        } else {
            // This grantee (principal) has at least one entry
            // (grantor, actions) added for this resource already
            Iterator granteeEntryItr = granteeEntries.iterator();
            while ( granteeEntryItr.hasNext() ) {
                GranteeEntitlementEntry aCurrentEntry = (GranteeEntitlementEntry) granteeEntryItr.next();
                // Resource and grantee must be the same (or we wouldn't be here) so
                // only need to check that grantor is the same before adding (unioning)
                // the actions
                if ( newEntry.getGrantor().equalsIgnoreCase(aCurrentEntry.getGrantor()) ) {
                    // We need to ultimately return the union of all
                    // allowed actions (CRUD) granted to this grantee,
                    // by this grantor, on this resource
                    granteeEntries.remove(aCurrentEntry);
                    newEntry.addActions(aCurrentEntry.getActions());
                    break;
                }
            }
        }
        granteeEntries.add(newEntry);
        this.granteeMap.put(granteeName, granteeEntries);
    }

    /**
     * Pretty print this Object to a String.
     * <ol>
     *   <li value=1>VDB Name, VDB Version</li>
     *   <li>Group Name (fully qualified)</li>
     *   <li>Element Name (fully qualified)</li>
     *      <ul>
     *          <li>Grantee Name; Grantor Name; Allowed Actions (A <code>String[]</code> of one or more of {CREATE, READ, UPDATE, DELETE})</li>
     *          <li> ... </li>
     *      </ul>
     * </ol>
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append(this.VDBName + ", "); //$NON-NLS-1$
        buf.append(this.VDBVersion + "\n"); //$NON-NLS-1$
        buf.append(this.groupName + "\n"); //$NON-NLS-1$
        if ( this.eleName != null ) {
            buf.append(" " + this.eleName + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        Iterator granteeItr = this.getGrantees().iterator();
        while ( granteeItr.hasNext() ) {
            GranteeEntitlementEntry entry = (GranteeEntitlementEntry) granteeItr.next();
            buf.append("  " + entry.toString() + "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return buf.toString();
    }
}
