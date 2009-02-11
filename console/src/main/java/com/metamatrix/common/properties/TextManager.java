/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.properties;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * In addition to the feautures provided by PropertyManager, TextManager adds support for language translation from simple words
 * to entire strings of text.  Messages that would normally be hardcoded as strings can be replaced with simple calls to one of
 * TextManager's getText methods, which use keys to lookup locale-specific text in one or more registered resource bundles.  The
 * text within bundles may contain indexed placeholders in the form described by {@link MessageFormat}, which is basically index
 * numbers (relative to zero) surrounded by curly braces.  All but one of the getText methods take additional arguments that will
 * be dynamically inserted into the text so that the <em>n</em><sup>th</sup> parameter will be inserted into the placeholder with
 * an index of <em>n</em>.
 * <p>
 * <strong>Example:</strong>
 * </p><dl>
 *     <dd><p><u>Resource Bundle (com/metamatrix/common/logmessages.properties)</u></p></dd>
 *     <code><dl>
 *         <dd>MSG.001.001.0001 = {0} \"{1}\" not found in {2}</dd>
 *     </dl></code>
 *     <dd><p><u>Code</u></p></dd>
 *     <code><dl>
 *         <dd>CommonPlugin.Util.getString("MSG.001.001.0001", "File", "java.exe", "directory /bin")</dd>
 *     </dl></code>
 *     <dd><p><u>Resulting value</u></p></dd>
 *     <code><dl>
 *         <dd>File "java.exe" not found in directory /bin</dd>
 *     </dl></code>
 * </dl><p>
 * Each of the getText methods simply returns the key surrounded by angle brackets ('<' and '>') if no translation can be found in
 * any of the registered namespaces.  This provides an application with the ability to use getText methods everywhere that text
 * may need to be translated.  For single words, the single-parameter getText(String) method can be used to pass the word itself
 * as the key.  If no translation is found, the word is simply returned (without angle brackets).
 * </p><p>
 * TextManager is a singleton with a {@link #DEFAULT_PROPERTY_FILE_NAME default registered namespace}.  Subclasses my remove this
 * namespace and/or provide additional namespaces.
 * </p>
 * @since 2.1
 */
public class TextManager extends PropertyManager {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    /**
     * The default namespace searched by TextManager.
     * @since 2.1
     */
    public static final String[] DEFAULT_NAMESPACES = {
        "com/metamatrix/common/properties/text", //$NON-NLS-1$
        "com/metamatrix/api/logmessage", //$NON-NLS-1$
        "com/metamatrix/common/logmessage", //$NON-NLS-1$
        "com/metamatrix/connector/logmessage", //$NON-NLS-1$
        "com/metamatrix/jdbc/logmessage", //$NON-NLS-1$
        "com/metamatrix/metadata/logmessage", //$NON-NLS-1$
        "com/metamatrix/platform/logmessage", //$NON-NLS-1$
        "com/metamatrix/server/logmessage", //$NON-NLS-1$
        "com/metamatrix/query/logmessage", //$NON-NLS-1$
        "com/metamatrix/metaviewer/logmessage", //$NON-NLS-1$
        "com/metamatrix/odbc/logmessage", //$NON-NLS-1$
        "com/metamatrix/soap/logmessage", //$NON-NLS-1$
		"com/metamatrix/jdbc2/logmessage", //$NON-NLS-1$
		"com/metamatrix/dqp/logmessage", //$NON-NLS-1$
		"com/metamatrix/admin/logmessage", //$NON-NLS-1$
        "com/metamatrix/api/errormessage", //$NON-NLS-1$
        "com/metamatrix/common/errormessage", //$NON-NLS-1$
        "com/metamatrix/connector/errormessage", //$NON-NLS-1$
        "com/metamatrix/jdbc/errormessage", //$NON-NLS-1$
        "com/metamatrix/metadata/errormessage", //$NON-NLS-1$
        "com/metamatrix/metaviewer/errormessage", //$NON-NLS-1$
        "com/metamatrix/odbc/errormessage", //$NON-NLS-1$
        "com/metamatrix/platform/errormessage", //$NON-NLS-1$
        "com/metamatrix/query/errormessage", //$NON-NLS-1$
        "com/metamatrix/server/errormessage", //$NON-NLS-1$
        "com/metamatrix/soap/errormessage", //$NON-NLS-1$
		"com/metamatrix/jdbc2/errormessage", //$NON-NLS-1$
		"com/metamatrix/dqp/errormessage", //$NON-NLS-1$
		"com/metamatrix/admin/errormessage" //$NON-NLS-1$
    };

    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    public static TextManager INSTANCE = new TextManager();

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Creates an instance of TextManager with a default registered resource bundle used by other Toolbox classes.
     * @since 2.1
     */
    protected TextManager() {
        super(DEFAULT_NAMESPACES);
    }

    /**
     * Creates an instance of TextManager that will retrieve property values from the specified namespace.
     * @param namespace The namespace identifier
     * @since 3.1
     */
    protected TextManager(final String namespace) {
        super(namespace);
    }

    /**
     * Creates an instance of TextManager that will retrieve property values from the specified list of namespaces.  The
     * namespaces will be searched in ascending order, starting with the first namespace registered.
     * @param namespaces The list of namespace identifiers
     * @since 3.1
     */
    protected TextManager(final String[] namespaces) {
        super(namespaces);
    }

    /**
     * Creates an instance of TextManager that will retrieve property values from the specified list of namespaces.  The
     * namespaces will be searched in ascending order, starting with the first namespace registered.
     * @param namespaces The list of namespace identifiers
     * @since 3.1
     */
    protected TextManager(final List namespaces) {
        super(namespaces);
    }

    /**
     * Creates an instance of TextManager that will retrieve property values from the specified namespace.  Properties from
     * the resource bundle identified by the namespace will be loaded into the specified property map.
     * @param namespace   The namespace identifier
     * @param propertyMap The map into which the property values are loaded
     * @since 3.1
     */
    protected TextManager(final String namespace, final Map propertyMap) {
        super(namespace, propertyMap);
    }

    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
     * @since 2.1
     */
    private String convertValueToString(final String key, final Object value) {
        if (value != null  &&  !(value instanceof String)) {
            throw new ClassCastException(CommonPlugin.Util.getString(ErrorMessageKeys.PROPERTIES_ERR_0005, new Object[]{ key, String.class,value.getClass().getName()}));
        }
        return (String)value;
    }

    /**
     * Same as superclass method, but doesn't translate error messages.  Necessary to prevent infinite loop on errors.
     * @since 2.1
     */
    private String getStringInternal(final String key) {
        return convertValueToString(key, get(key));
    }

    /**
     * Same as superclass method, but doesn't translate error messages.  Necessary to prevent infinite loop on errors.
     * @since 2.1
     */
    private String getStringInternal(final String key, final String defaultValue) {
        final Object val = get(key);
        if (val == null) {
          return defaultValue;
        }
        return convertValueToString(key, val);
    }

    /**
     * Convenience method that calls {@link #getText(String, Object)} with no substitution parameters.
     * @since 2.1
     */
    public String getText(final String key) {
        return getText(key, (List)null);
    }

    /**
     * Convenience method that wraps the specified parameter in a Boolean object before calling {@link #getText(String, Object)}.
     * @since 2.1
     */
    public String getText(final String key, final boolean parameter) {
        return getText(key, Boolean.valueOf(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Byte object before calling {@link #getText(String, Object)}.
     * @since 2.1
     */
    public String getText(final String key, final byte parameter) {
        return getText(key, new Byte(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Character object before calling {@link #getText(String, Object)}.
     * @since 2.1
     */
    public String getText(final String key, final char parameter) {
        return getText(key, new Character(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Short object before calling {@link #getText(String, Object)}.
     * @since 2.1
     */
    public String getText(final String key, final short parameter) {
        return getText(key, new Short(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Integer object before calling {@link #getText(String, Object)}.
     * @since 2.1
     */
    public String getText(final String key, final int parameter) {
        return getText(key, new Integer(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Long object before calling {@link #getText(String, Object)}.
     * @since 2.1
     */
    public String getText(final String key, final long parameter) {
        return getText(key, new Long(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Float object before calling {@link #getText(String, Object)}.
     * @since 2.1
     */
    public String getText(final String key, final float parameter) {
        return getText(key, new Float(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Double object before calling {@link #getText(String, Object)}.
     * @since 2.1
     */
    public String getText(final String key, final double parameter) {
        return getText(key, new Double(parameter));
    }

    /**
     * Convenience method that adds the specified parameter to a list before calling {@link #getText(String, List)}.
     * @since 2.1
     */
    public String getText(final String key, final Object parameter) {
        return getText(key, Arrays.asList(new Object[] {parameter}));
    }

    /**
     * Convenience method that adds the specified parameters to a list before calling {@link #getText(String, List)}.
     * @since 2.1
     */
    public String getText(final String key, final Object parameter1, final Object parameter2) {
        return getText(key, Arrays.asList(new Object[] {parameter1, parameter2}));
    }

    /**
     * Convenience method that adds the specified parameters to a list before calling {@link #getText(String, List)}.
     * @since 2.1
     */
    public String getText(final String key, final Object parameter1, final Object parameter2, final Object parameter3) {
        return getText(key, Arrays.asList(new Object[] {parameter1, parameter2, parameter3}));
    }

    /**
     * Gets text substituting values from the given array into the proper place.
     * @param key the property key
     * @param parameters the data placed into the text
     * @return the locale-specific text
     * @since 3.1
     */
    public String getText(final String key, final Object[] parameters) {
        return getText(key, Arrays.asList(parameters));
    }

    /**
     * Retrieves the text identified by the specified key, replacing indexed placeholders with values from the specified list of
     * parameters so that the <em>n</em><sup>th</sup> parameter will be inserted into the placeholder with an index of <em>n</em>.
     * @param key          The key identifying the text
     * @param parameters   The list of parameters used to replace placeholders in text
     * @return The text
     * @since 2.1
     */
    public String getText(final String key, final List parameters) {
        final String text = getStringInternal(key);
        if (text == null) {
            return '<' + key + '>';
        }
        if (parameters == null  ||  parameters.size() == 0) {
            return text;
        }
        final Object[] array = new String[parameters.size()];
        final Iterator iter = parameters.iterator();
        for (int ndx = 0;  iter.hasNext();  ++ndx) {
            final Object obj = iter.next();
            final String objString = (obj != null ? obj.toString() : "null"); //$NON-NLS-1$
            array[ndx] = translate(objString);
        }
        return MessageFormat.format(text, array);
    }

    /**
     * Retrieves the text identified by the specified key, or if not found, returns the key itself.  This method is intended for
     * single word translations.
     * @param key The key identifying the text
     * @return The text, or the key if not found
     * @since 3.1
     */
    public String translate(final String key) {
        return getStringInternal(key, key);
    }
}
