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

package com.metamatrix.internal.core.xml;

import org.jdom.input.SAXBuilder;

/**
 * Utility class used to create a SAXBuilder
 */
public class SAXBuilderHelper {

	/** System property name used to get parser class */
    public static final String PARSER_PROPERTY_NAME = "metamatrix.xmlparser.class"; //$NON-NLS-1$
    
    private static String PARSER_NAME;

    public static String getParserClassName() {

        if (PARSER_NAME == null) {
            PARSER_NAME = System.getProperty(PARSER_PROPERTY_NAME);
        }       
        return PARSER_NAME;
    }

	/**
	 * Returns a SAXBuilder using the Parser class defined by the metamatrix.xmlparser.class
	 *         System property. If the System property does not exist, returns a SAXBuilder using
	 *          the org.apache.xerces.parsers.SAXParser.
	 * @param boolean validate
	 * @return org.jdom.input.SAXBuilder
	 */
	public static SAXBuilder createSAXBuilder() {
		return createSAXBuilder(false);
	}

	/**
	 * Returns a SAXBuilder using the Parser class defined by the metamatrix.xmlparser.class
	 *         System property. If the System property does not exist, returns a SAXBuilder using
	 *          the org.apache.xerces.parsers.SAXParser.
	 * @param boolean validate
	 * @return org.jdom.input.SAXBuilder
	 */
	public static SAXBuilder createSAXBuilder(boolean validate) {
		return new SAXBuilder(getParserClassName(), validate);
	}

	/**
	 * Returns a SAXBuilder
	 * @param boolean validate
	 * @return org.jdom.input.SAXBuilder
	 */
	public static SAXBuilder createSAXBuilder(String saxDriverClass, boolean validate) {
		return new SAXBuilder(saxDriverClass, validate);
	}

}
