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

package com.metamatrix.platform.admin.api;

import com.metamatrix.common.object.ObjectDefinition;

/**
 * This interface specifies an <code>ObjectDefinition</code> for the <code>PermissionDataNode</code>.
 * <br>In this interface we specify the various (sub) types of <code>PermissionDataNode</code>s.</br>
 * They are:
 * <ul>
 *  <li>Unknown</li>
 *  <li>Model</li>
 *  <li>Catagory</li>
 *  <li>Group</li>
 *  <li>Element</li>
 *  <li>Proceedure</li>
 *  <li>Document</li>
 * </ul>
 */
public interface PermissionDataNodeDefinition extends ObjectDefinition {

    /**
     * The <i>sub</i>type of the <code>PermissionDataNode</code>.
     * These are:
     * <ul>
     *  <li>Unknown</li>
     *  <li>Model</li>
     *  <li>Catagory</li>
     *  <li>Group</li>
     *  <li>Element</li>
     *  <li>Proceedure</li>
     *  <li>Document</li>
     * </ul>
     * Use these types as a comparison when calling {@link #getType() getType()}.
     */
    static final class TYPE {
        public static final int UNKOWN = 0;
        public static final int MODEL = 1;
        public static final int CATEGORY = 2;
        public static final int GROUP = 3;
        public static final int ELEMENT = 4;
        public static final int PROCEDURE = 5;
        public static final int DOCUMENT = 6;

        public static final int LOWER_BOUND = UNKOWN;
        public static final int UPPER_BOUND = DOCUMENT;
    }

    /**
     * Get the type of <code>PermissionDataNode</code>.
     * @return The type of this <code>PermissionDataNode</code>.
     * @see PermissionDataNodeDefinition.TYPE
     */
    int getType();
}

