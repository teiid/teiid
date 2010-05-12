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

package org.teiid.translator.text;

/**
 * Property names used in the text descriptor file.
 */
public class TextDescriptorPropertyNames {
    public static final String LOCATION = "LOCATION"; //$NON-NLS-1$
    public static final String DELIMITER  = "DELIMITER"; //$NON-NLS-1$
    public static final String QUALIFIER  = "QUALIFIER"; //$NON-NLS-1$
    public static final String HEADER_LINES  = "SKIPHEADERLINES"; //$NON-NLS-1$
    public static final String COLUMNS  = "COLUMNS"; //$NON-NLS-1$
    public static final String TYPES  = "TYPES"; //$NON-NLS-1$
    
    /**
     * If HEADER_LINES is non-zero this property defines which row of HEADER_LINES 
     * can be used as the header row.  The value is 1-based and must be < HEADER_LINES.
     * @since 5.0.3
     */
    public static final String HEADER_ROW  = "HEADERLINE"; //$NON-NLS-1$
}
