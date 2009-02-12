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

import java.util.Collection;

/**
 * The EntitlementAction interface encapsulates the methods that are required to identify the
 * set of actions associated with an entitlement.  This interface may be implemented by classes
 * (such as BasicEntitlementAction) that contain a closed set of entitlement actions instances.
 * Thus, specialized implementations of EntitlementAction can be provided, extending
 * the capabilities of the Authorization framework.
 * @see BasicEntitlementAction
 */
public interface AuthorizationActions extends Comparable {

    /**
     * Checks if the specified entitlement's actions are "implied by" this object's actions.
     * <p>
     * Essentially, the specified entitlement action is implied by this entitlement action
     * if all of the actions of <code>entitlement</code> are also actions of this object's set.
     * @param entitlement the entitlement to check against.
     * @returns true if the specified entitlement is implied by this object, false if not
     */
    public boolean implies(AuthorizationActions entitlement);

    /**
     * Return the value of this action.
     * @return the value of this action.
     */
    public int getValue();

    /**
     * Return the label of this action.
     * @return the label of this action.
     */
    public String getLabel();

    /**
     * Return the number of actions.
     * @return the number of actions.
     */
    public int getLabelCount();

    /**
     * Return the set of labels of this action.
     * @return the set of labels of this action.
     */
    public String[] getLabels();

    /**
     * Return whether this instance contains the specified label
     * @return true if this instance contains the specified label, or false otherwise
     */
    public boolean containsLabel(String label);

    /**
     * Return whether this instance contains all of the specified labels
     * @return true if this instance contains all of the specified labels, or false otherwise
     */
    public boolean containsLabels(String[] labels);

    /**
     * Return whether this instance contains all of the specified labels
     * @return true if this instance contains all of the specified labels, or false otherwise
     */
    public boolean containsLabels(Collection labels);

}




