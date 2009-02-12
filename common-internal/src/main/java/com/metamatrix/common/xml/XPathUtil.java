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

package com.metamatrix.common.xml;

import java.util.*;
import org.jdom.*;
import com.metamatrix.core.util.Assertion;

public class XPathUtil {

    /** Delimiter for XPath */
    public static final String STEP_DELIMITER = "/"; //$NON-NLS-1$
    public static final char STEP_DELIMITER_CHAR = STEP_DELIMITER.charAt(0);

    private static final String DOCUMENT_ROOT = "/doc/"; //$NON-NLS-1$
//    private static final char ALL = '*';

    public static class Literal {
        public static final char APOSTROPHE            = '\'';
        public static final char QUOTE                 = '"';
    }

    public static class ExpressionToken {
        public static final char ATTRIBUTE_PREFIX      = '@';
        public static final char INDEX_OPEN            = '[';
        public static final char INDEX_CLOSE           = ']';
        public static final char PARAMETERS_OPEN       = '(';
        public static final char PARAMETERS_CLOSE      = ')';
        public static final char SELF                  = '.';
        public static final String PARENT              = ".."; //$NON-NLS-1$
        public static final String AXIS_SUFFIX         = "::"; //$NON-NLS-1$
    }

    public static class Operator {
        public static final char OR                    = '|';
        public static final char ADD                   = '+';
        public static final char MINUS                 = '-';
        public static final char EQUAL                 = '=';
        public static final char LESS_THAN             = '<';
        public static final char MULTIPLY              = '*';
        public static final char GREATER_THAN          = '>';
        public static final String NOT_EQUAL           = "!="; //$NON-NLS-1$
        public static final String LESS_THAN_EQUAL     = "<="; //$NON-NLS-1$
        public static final String GREATER_THAN_EQUAL  = ">="; //$NON-NLS-1$
        public static final String AND_NAME            = "and"; //$NON-NLS-1$
        public static final String OR_NAME             = "or"; //$NON-NLS-1$
        public static final String MODULUS_NAME        = "mod"; //$NON-NLS-1$
        public static final String DIVIDE_NAME         = "div"; //$NON-NLS-1$
        public static final String DECENDENT_OR_SELF   = "//"; //$NON-NLS-1$
    }

    /**
     * Get the absolute path using the abbreviated format of XPath 1.0
     * for the specified element.  For example, the following XPath:
     * <p><code>/doc/chapter[5]/section[2]</code></p>
     * specifies the second section of the fifth chapter of the document.
     * @return the XPath of the specified element.
     * @see <a href="http://www.w3c.org/TR/xpath#path-abbrev">XPath</a>
     */
    public static String getAbsolutePath( Element tag ) {
        Assertion.isNotNull(tag,"The JDOM Element reference may not be null"); //$NON-NLS-1$
        return addToXPath( tag, new StringBuffer(DOCUMENT_ROOT) ).toString();
    }

    /**
     * Get the absolute path using the abbreviated format of XPath 1.0
     * for the specified attribute.  For example, the following XPath:
     * <p><code>/doc/chapter[5]/section[2]/@title</code></p>
     * specifies the title attribute of the second section of the fifth chapter of the document.
     * @return the XPath of the specified element.
     * @see <a href="http://www.w3c.org/TR/xpath#path-abbrev">XPath</a>
     */
    public static String getAbsolutePath( Attribute attribute ) {
        Assertion.isNotNull(attribute,"The JDOM Attribute reference may not be null"); //$NON-NLS-1$
        Element tag = attribute.getParent();
        StringBuffer sb = new StringBuffer(DOCUMENT_ROOT);
        addToXPath( tag, sb );
        sb.append(STEP_DELIMITER_CHAR);
        sb.append(ExpressionToken.ATTRIBUTE_PREFIX);
        sb.append(attribute.getName());
        sb.append(Operator.EQUAL);
        sb.append(Literal.QUOTE);
        sb.append(attribute.getValue());
        sb.append(Literal.QUOTE);
        return sb.toString();
    }

    protected static StringBuffer addToXPath(Element tag, StringBuffer sb) {
        StringBuffer result = sb;
        // Add the parent's name first ...
        // The parent could be an Element or Document
        Parent parent = tag.getParent();
        int index = 1;
        if ( parent != null && parent instanceof Element ) {
            List parentsChildren = ((Element)parent).getChildren();
            index = parentsChildren.indexOf(tag) + 1;
            result = addToXPath((Element)parent,result);
            result.append(STEP_DELIMITER_CHAR);
        }
        result.append(tag.getName());
        result.append(ExpressionToken.INDEX_OPEN);
        result.append(index);
        result.append(ExpressionToken.INDEX_CLOSE);
        return result;
    }

}
