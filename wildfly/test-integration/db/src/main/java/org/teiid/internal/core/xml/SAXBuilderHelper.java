/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.internal.core.xml;

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
