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

package org.teiid.adminapi;

import java.io.Serializable;
import java.util.Properties;

/**
 * Base interface of client side admin objects. Specifies behaviors and properties common to all administrative objects.
 * <p>
 * Unique identifiers are available for all <code>AdminObject</code>s and their forms are specific to each object. See
 * the javadoc on the individual object for its particular identifier pattern required.
 * </p>
 * <p>
 * This interface need not be used directly by clients except when coding to constants.
 * </p>
 *
 * @since 4.3
 */
public interface AdminObject extends Serializable {

    /**
     * The character that delimits the atomic components of the identifier.
     * @see #DELIMITER
     */
    public static final char DELIMITER_CHAR = '|';

    /**
     * The character (as a <code>String</code>) that delimits the atomic components of the identifier.
     *
     * <p>It is <emph>strongly</emph> advisable to write admin client code using this <code>DELIMITER</code>
     * rather than hard-coding a delimiter character in admin code.  Doing this eliminates the possibility
     * of admin client code breaking if/when the delimiter character must be changed.</p>
     */
    public static final String DELIMITER = new String(new char[] {DELIMITER_CHAR});

    /**
     * The delimiter character as a <code>String</code> escaped.
     * @see #DELIMITER
     */
    public static final String ESCAPED_DELIMITER = "\\" + DELIMITER; //$NON-NLS-1$

    /**
     * The wildcard character (as a <code>String</code>) that can be used in may identifier patterns
     * to indicate <i>"anything"</i> or, more specifically, replace <i>"zero or more"</i>
     * identifier components.
     *
     * <p>It is <emph>strongly</emph> advisable to write admin client code using this <code>WILDCARD</code>
     * rather than hard-coding a wildcard character in admin code.  Doing this eliminates the possibility
     * of admin client code breaking if/when the wildcard character must be changed.</p>
     */
    public static final String WILDCARD = "*"; //$NON-NLS-1$

    /**
     * The wildcard character as a <code>String</code> escaped.
     * @see #WILDCARD
     */
    public static final String ESCAPED_WILDCARD = "\\" + WILDCARD; //$NON-NLS-1$

    /**
     * Get the name for this AdminObject, usually the last component of the identifier.
     *
     * @return String Name
     * @since 4.3
     */
    String getName();

    /**
     * Get the Configuration Properties that defines this process
     *
     * @return Properties
     * @since 4.3
     */
    Properties getProperties();

    /**
     * Searches for the property with the specified key in this Admin Object. If the key is not found the method returns
     * <code>null</code>.
     *
     * @param name
     *            the property key.
     * @return the value in this Admin Object with the specified key value.
     * @since 4.3
     */

    String getPropertyValue(String name);
}
