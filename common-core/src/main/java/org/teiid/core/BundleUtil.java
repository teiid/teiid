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

package org.teiid.core;

import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.teiid.core.util.ArgCheck;

public class BundleUtil {
    /**
     * The name of the resource bundle.
     */
    private final String bundleName;

    /**
     * The {@link ResourceBundle}for localization; initialized in the constructor.
     */
    private final ResourceBundle bundle;

    protected final String pluginId;

    public interface Event {
        //String id();
    }

    /**
     * Return the {@link BundleUtil} for the class.  The bundle must be in the same package or a parent package of the class.
     * @param clazz
     */
    public static BundleUtil getBundleUtil(Class<?> clazz) {
        String packageName = clazz.getPackage().getName();

        while (true) {
            //scan up packages until found
            String bundleName = packageName + ".i18n"; //$NON-NLS-1$
            try {
                ResourceBundle bundle = ResourceBundle.getBundle(bundleName, Locale.getDefault(), clazz.getClassLoader());
                return new BundleUtil(packageName, bundleName, bundle);
            } catch (MissingResourceException e) {
                int index = packageName.lastIndexOf('.');
                if (index < 0) {
                    throw e;
                }
                packageName = packageName.substring(0, index);
            }
        }
    }

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

            return this.bundle.getString(key);
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
     * Determines if the given key exists in the resource file.
     *
     * @param key
     *            the key in the resource file
     * @return True if the key exists.
     * @since 4.0
     */
    public boolean keyExists(final String key) {
        try {
            return (this.bundle.getString(key) != null);
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
                            final Object... parameters) {
        String text = getString(key);

        // Check the trivial cases ...
        if (text == null) {
            return '<' + key + '>';
        }
        if (parameters == null || parameters.length == 0) {
            return text;
        }

        return MessageFormat.format(text, parameters);
    }

    public String gs(final String key, final Object... parameters) {
        return getString(key, parameters);
    }

    public String gs(final Event key, final Object... parameters) {
        StringBuilder sb = new StringBuilder();
        sb.append(key);
        sb.append(" "); //$NON-NLS-1$
        sb.append(getString(key.toString(), parameters));
        return sb.toString();
    }

    public String getStringOrKey(final String key) {
        ArgCheck.isNotNull(key);

        try {
            return this.bundle.getString(key);
        } catch (final MissingResourceException err) {
            return key;
        }
    }

}
