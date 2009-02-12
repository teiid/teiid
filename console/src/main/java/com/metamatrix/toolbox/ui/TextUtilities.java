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

//################################################################################################################################
package com.metamatrix.toolbox.ui;

// JDK imports
import java.text.StringCharacterIterator;

/**
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class TextUtilities {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

//    private static final String PROPERTIES_FILE = "com/metamatrix/toolbox/ui/text";

    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

//    private static ResourceBundle text = null;

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @param id The ID of the text to retrieve
    @return The locale-specific text associated with id
    @since 2.0
    *//*
    public static String getText(final String id) {
        if (text == null)
        {
            try {
                text = ResourceBundle.getBundle(PROPERTIES_FILE);
            } catch (final Exception err) {
                if (err instanceof RuntimeException) {
                    throw (RuntimeException)err;
                } else {
                    throw new RuntimeException(err.getMessage());
                }
            }
        }
        return text.getString(id);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @param text Text that is potentially formatted with new-line (\n) characters.
    @return The text without new-line characters
    @since 2.0
    */
    public static String getUnformattedText(final String text) {
        if (text == null) {
            return null;
        }
        final StringBuffer buf = new StringBuffer();
        final StringCharacterIterator iterator = new StringCharacterIterator(text);
        char chr = iterator.current();
        while (chr != StringCharacterIterator.DONE) {
            if (chr == '\n') {
                if (iterator.getIndex() < iterator.getEndIndex()  &&  (buf.length() == 0  ||
                                                                       buf.charAt(buf.length() - 1) != ' ')) {
                    buf.append(' ');
                }
            } else {
                buf.append(chr);
            }
            chr = iterator.next();
        }
        return buf.toString();
    }
}
