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

package com.metamatrix.core;

import java.text.MessageFormat;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.StringUtil;

public class BundleUtil {
    /**
     * The product properties used to override default localized text.
     * @since 5.0.2
     */
    static protected ResourceBundle productProps;

    /**
     * The name of the resource bundle.
     */
    private final String bundleName;

    /**
     * The {@link ResourceBundle}for localization; initialized in the constructor.
     */
    private final ResourceBundle bundle;

    protected final String pluginId;

    /**
     * Construct an instance of this class by specifying the plugin ID.
     *
     * @param pluginId
     *            the identifier of the plugin for which this utility is being instantiated
     * @param bundleName
     *            the name of the resource bundle; used for problem reporting purposes only
     * @param bundle
     *            the resource bundle
     */
    public BundleUtil(final String pluginId,
                          final String bundleName,
                          final ResourceBundle bundle) {
    	this.pluginId = pluginId;
        this.bundleName = bundleName;
        this.bundle = bundle;
    }

    /**
     * Get the string identified by the given key and localized to the current locale.
     *
     * @param key
     *            the key in the resource file
     * @return the localized String, or <code>
     *    "Missing message: " + key + " in: " + this.bundleName
     * </code> if the string could
     *         not be found in the current locale, or <code>
     *    "No message available"
     * </code> if the <code>key</code> is null.
     */
    public String getString(final String key) {
        try {
            // Since this string has no parameters, it will not be run through MessageFormat.
            // MessageFormat eliminates double ticks, so the next two lines replace double ticks
            // with single ticks. This is only needed if the localized string contains double ticks
            // (the policy is that localized strings without parameters should not).
            // COMMENTED OUT BECAUSE OF POLICY
            //char[] messageWithNoDoubleQuotes = CharOperation.replace(text.toCharArray(), DOUBLE_QUOTES, SINGLE_QUOTE);
            //text = new String(messageWithNoDoubleQuotes);

            String value = getProductValue(key);
            return ((value == null) ? this.bundle.getString(key) : value);
        } catch (final Exception err) {
            String msg;

            if (err instanceof NullPointerException) {
                msg = "<No message available>"; //$NON-NLS-1$
            } else if (err instanceof MissingResourceException) {
                msg = "<Missing message for key \"" + key + "\" in: " + this.bundleName + '>'; //$NON-NLS-1$ //$NON-NLS-2$
            } else {
                msg = err.getLocalizedMessage();
            }

            // RMH: See DataAccessPlugin.ResourceLocator.getString(...) method, which tries one bundle before
            // delegating to another.  Therefore, this will happen normally in some situations.
            //log(msg);

            return msg;
        }
    }

    /**
     * Obtains the value that is overriding the default value.
     * @param theKey the key whose product value is being requested
     * @return the value or <code>null</code> if not overridden by the product
     */
    private String getProductValue(String theKey) {
        String result = null;

        if ((productProps != null) && !StringUtil.isEmpty(theKey)) {
            String key = this.pluginId + '.' + theKey;

            try {
                result = productProps.getString(key);
            } catch (MissingResourceException theException) {
                // not found in product properties
            }
        }

        return result;
    }

    /**
     * Determines if the given key exists in the resource file.
     *
     * @param key
     *            the key in the resource file
     * @return True if the key exists.
     * @since 4.0
     */
    public boolean keyExists(final String key) {
        try {
            return ((getProductValue(key) != null) || (this.bundle.getString(key) != null));
        } catch (final Exception err) {
            return false;
        }
    }

    /**
     * Get the string identified by the given key and localized to the current locale, and replace placeholders in the localized
     * string with the string form of the parameters.
     *
     * @param key
     *            the key in the resource file
     * @param parameters
     *            the list of parameters that should replace placeholders in the localized string (e.g., "{0}", "{1}", etc.)
     * @return the localized String, or <code>
     *    "Missing message: " + key + " in: " + this.bundleName
     * </code> if the string could
     *         not be found in the current locale, or <code>
     *    "No message available"
     * </code> if the <code>key</code> is null.
     */
    public String getString(final String key,
                            final List parameters) {
        if (parameters == null) {
            return getString(key);
        }
        return getString(key, parameters.toArray());
    }

    /**
     * Convenience method that wraps the specified parameter in a Boolean object before calling
     * {@link #getString(String, Object)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final boolean parameter) {
        return getString(key, new Boolean(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Byte object before calling {@link #getString(String, Object)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final byte parameter) {
        return getString(key, new Byte(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Character object before calling
     * {@link #getString(String, Object)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final char parameter) {
        return getString(key, new Character(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Double object before calling {@link #getString(String, Object)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final double parameter) {
        return getString(key, new Double(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Float object before calling {@link #getString(String, Object)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final float parameter) {
        return getString(key, new Float(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Integer object before calling
     * {@link #getString(String, Object)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final int parameter) {
        return getString(key, new Integer(parameter));
    }

    /**
     * Convenience method that wraps the specified parameter in a Long object before calling {@link #getString(String, Object)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final long parameter) {
        return getString(key, new Long(parameter));
    }

    /**
     * Convenience method that adds the specified parameter to a list before calling {@link #getString(String, List)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final Object parameter) {
        if (parameter != null) {
            if (parameter.getClass().isArray()) {
                return getString(key, (Object[])parameter);
            }
            if (parameter instanceof List) {
                return getString(key, (List)parameter);
            }
        }
        return getString(key, new Object[] {parameter});
    }

    /**
     * Convenience method that adds the specified parameters to a list before calling {@link #getString(String, List)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final Object parameter1,
                            final Object parameter2) {
        return getString(key, new Object[] {parameter1, parameter2});
    }

    /**
     * Convenience method that adds the specified parameters to a list before calling {@link #getString(String, List)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final Object parameter1,
                            final Object parameter2,
                            final Object parameter3) {
        return getString(key, new Object[] {parameter1, parameter2, parameter3});
    }

    /**
     * Get the string identified by the given key and localized to the current locale, and replace placeholders in the localized
     * string with the string form of the parameters.
     *
     * @param key
     *            the key in the resource file
     * @param parameters
     *            the list of parameters that should replace placeholders in the localized string (e.g., "{0}", "{1}", etc.)
     * @return the localized String, or <code>
     *    "Missing message: " + key + " in: " + this.bundleName
     * </code> if the string could
     *         not be found in the current locale, or <code>
     *    "No message available"
     * </code> if the <code>key</code> is null.
     */
    public String getString(final String key,
                            final Object[] parameters) {
        String text = getProductValue(key);

        if (text == null) {
            text = getString(key);
        }

        // Check the trivial cases ...
        if (text == null) {
            return '<' + key + '>';
        }
        if (parameters == null || parameters.length == 0) {
            return text;
        }

        return MessageFormat.format(text, parameters);
    }

    /**
     * Convenience method that wraps the specified parameter in a Short object before calling {@link #getString(String, Object)}.
     *
     * @since 2.1
     */
    public String getString(final String key,
                            final short parameter) {
        return getString(key, new Short(parameter));
    }

    /**
     * @see com.metamatrix.core.PluginUtil#getStringOrKey(java.lang.String)
     * @since 4.1
     */
    public String getStringOrKey(final String key) {
        ArgCheck.isNotNull(key);

        String value = getProductValue(key);

        if (value == null) {
            try {
                return this.bundle.getString(key);
            } catch (final MissingResourceException err) {
                return key;
            }
        }

        return value;
    }


}
