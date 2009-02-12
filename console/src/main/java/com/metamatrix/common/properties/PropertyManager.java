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

package com.metamatrix.common.properties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.Set;

import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.util.ArgCheck;

/**
 * PropertyManager provides the ability to manage application properties stored in {@link ResourceBundle resource bundles}.  This
 * allows for locale-specific retrieval of property values.  Instances of PropertyManager retrieve property values from registered
 * namespaces, which ultimately map to a properties file or class in the same manner as a resource bundle, and in fact must
 * conform to resource bundle naming conventions.  Multiple namespaces can be used to retrieve property values, such that
 * if a value is not found in a particular namespace, the previously registered namespace will be searched next, and so on until
 * the value is found or all registered namespaces have been exhausted.
 * <p>
 * To use PropertyManager, an instance of PropertyManager must be created, and the namespaces to be searched must be registered
 * with this instance in the reverse order that they should be searched (In other words, the first name-space registered will be
 * searched last).  The property values within the resource bundles can then be retrieved using the getter methods provided,
 * passing appropriate property keys as arguments.
 * </p><p>
 * Notes:<ul>
 * <li>
 * Namespaces, which actually represent file paths, must be specified relative to the current classpath and with no extensions or
 * periods in the file name.
 * </li>
 * </ul>
 * </p><p>
 * <strong>Example:</strong>
 * </p><dl>
 *     <dd><p><u>Resource Bundle (com/metamatrix/someproject/some.properties)</u></p></dd>
 *     <code><dl>
 *         <dd>Key1 = Value1</dd>
 *         <dd>Key2 = Value2</dd>
 *     </dl></code>
 *     <dd><p><u>Code</u></p></dd>
 *     <code><dl>
 *         <dd>public static void main() {<dl></dd>
 *             <dd>final PropertyManager propMgr = new PropertyManager("com/metamatrix/someproject/some");</dd>
 *             <dd>final Object val = propMgr.get("Key1");</dd>
 *         </dl><dd>}</dd>
 *     </dl></code>
 * </dl>
 * @see ResourceBundle
 * @since 3.1
 */
public class PropertyManager
implements LogCommonConstants {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################
    
    private static final Map loadedNamespaces = new HashMap();
    
    //############################################################################################################################
	//# Static Methods                                                                                                           #
	//############################################################################################################################
    
    /**
     * Returns The set of namespaces that have been loaded
     * @return The set of loaded namespaces
     * @since 3.1
     */
    public static Set getLoadedNamespaces() {
        return loadedNamespaces.keySet();
    }
    
    /**
     * Returns whether the specified namespace has been loaded.
     * @param namespace The namespace identifier
     * @return True if the namespace has been loaded
     * @since 3.1
     */
    public static boolean isNamespaceLoaded(final String namespace) {
        return loadedNamespaces.containsKey(namespace);
    }
    
    /**
     * Loads the resource bundle with the specified name and stores the name as a namespace.  The name, which represents a file
     * path, must be specified with a path relative to the current classpath and with no extensions or periods in the filename.
     * @param name The name of the resource bundle
     * @throws MissingResourceException If the resource associated with the namespace could not be found.
     * @since 3.1
     */
    protected static void load(final String name) {
        load(name, false, null);
    }
    
    /**
     * Loads the resource bundle with the specified name into the specified map and stores the name as a namespace.  The name,
     * which represents a file path, must be specified with a path relative to the current classpath and with no extensions or
     * periods in the filename.
     * @param name         The name of the resource bundle
     * @param propertyMap  The map into which the property values are loaded
     * @throws MissingResourceException If the resource associated with the namespace could not be found.
     * @since 3.1
     */
    protected static void load(final String name, final Map propertyMap) {
        load(name, false, propertyMap);
    }
    
    /**
     * Loads or reloads, as specified, the resource bundle with the specified name into the specified map and stores the name as
     * a namespace.  The name, which represents a file path, must be specified with a path relative to the current classpath and
     * with no extensions or periods in the filename.
     * @param name         The name of the resource bundle
     * @param reload       True if the bundle should be reloaded
     * @param propertyMap  The map into which the property values are loaded
     * @throws MissingResourceException If the resource associated with the namespace could not be found.
     * @since 3.1
     */
    protected static void load(final String name, final boolean reload, final Map propertyMap) {
        // Check for existing namespace property map
        Map propMap = (Map)loadedNamespaces.get(name);
        // Exit if map already exists and not reloading
        if (!reload  &&  propMap != null) {
            return;
        }
        // Set map to passed in map if not null (replacing any existing map)
        if (propertyMap != null) {
            propMap = propertyMap;
        }
        // If map null, create one
        if (propMap == null) {
            propMap = new HashMap();
        } else if (reload) {
            // Clear map if reloading
            propMap.clear();
        }
        // Load all values from property file as UnevaluatedValues
        final ResourceBundle bundle = ResourceBundle.getBundle(name);
        final Enumeration iter = bundle.getKeys();
        String key, val;
        while (iter.hasMoreElements()) {
            key = (String)iter.nextElement();
            val = bundle.getString(key);
            propMap.put(key, new UnevaluatedValue(val));
        }
        // Save fileName in available namespace list
        loadedNamespaces.put(name, propMap);
    }
    
    /**
     * Reloads the resource bundle with the specified name.
     * @param name The name of the resource bundle
     * @throws MissingResourceException If the resource associated with the namespace could not be found.
     * @since 3.1
     */
    public static void reload(final String name) {
        load(name, true, null);
    }
    
    /**
     * Reloads the resource bundle with the specified name into the specified map.
     * @param name         The name of the resource bundle
     * @param propertyMap  The map into which the property values are loaded
     * @throws MissingResourceException If the resource associated with the namespace could not be found.
     * @since 3.1
     */
    public static void reload(final String name, final Map propertyMap) {
        load(name, true, propertyMap);
    }
  
    //############################################################################################################################
    //# Static Inner Class: UnevaluatedValue                                                                                     #
    //############################################################################################################################
  
    /**
     * @since 3.1
     */
    private static class UnevaluatedValue {
        //# UnevaluatedValue #####################################################################################################
        //# Variables                                                                                                            #
        //########################################################################################################################
        
        private String val;
        
        //# UnevaluatedValue #####################################################################################################
        //# Constructors                                                                                                         #
        //########################################################################################################################
        
        /// UnevaluatedValue
        /**
         * @since 3.1
         */
        private UnevaluatedValue(final String value) {
            val = value;
        }
        
        //# UnevaluatedValue #####################################################################################################
        //# Methods                                                                                                              #
        //########################################################################################################################
        
        /// UnevaluatedValue
        /**
         * @since 3.1
         */
        private String get() {
            return val;
        }
    }
    
    //############################################################################################################################
    //# Variables                                                                                                                #
    //############################################################################################################################
    
    private List namespaces;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    /**
     * Creates an instance of PropertyManager with no registered namespaces.
     * @since 3.1
     */
    public PropertyManager() {
        constructPropertyManager(null, null);
    }
    
    /**
     * Creates an instance of PropertyManager that will retrieve property values from the specified namespace.
     * @param namespace The namespace identifier
     * @since 3.1
     */
    public PropertyManager(final String namespace) {
        this(namespace, null);
    }
    
    /**
     * Creates an instance of PropertyManager that will retrieve property values from the specified list of namespaces.  The
     * namespaces will be searched in ascending order, starting with the first namespace registered.
     * @param namespaces The list of namespace identifiers
     * @since 3.1
     */
    public PropertyManager(final String[] namespaces) {
        this(Arrays.asList(namespaces));
    }
    
    /**
     * Creates an instance of PropertyManager that will retrieve property values from the specified list of namespaces.  The
     * namespaces will be searched in ascending order, starting with the first namespace registered.
     * @param namespaces The list of namespace identifiers
     * @since 3.1
     */
    public PropertyManager(final List namespaces) {
        constructPropertyManager(namespaces, null);
    }
    
    /**
     * Creates an instance of PropertyManager that will retrieve property values from the specified namespace.  Properties from
     * the resource bundle identified by the namespace will be loaded into the specified property map.
     * @param namespace   The namespace identifier
     * @param propertyMap The map into which the property values are loaded
     * @since 3.1
     */
    public PropertyManager(final String namespace, final Map propertyMap) {
        ArgCheck.isNotNull(namespace);
        constructPropertyManager(Arrays.asList(new String[] {namespace}), propertyMap);
    }
    
    //############################################################################################################################
	//# Methods                                                                                                                  #
	//############################################################################################################################

    /**
     * Adds the specified namespace to the list of registered namespaces to search when retrieving property values.  The order
     * that namespaces are registered is important, whereas the last namespace registered will be the first to be searched.
     * @param namespace The namespace identifier
     * @return True if the namespace was successfully registered, false if it was previously registered or its associated
     *          resource bundle was not found.
     * @since 3.1
     */
    public boolean addNamespace(final String namespace) {
        return addNamespace(namespace, null);
    }

    /**
     * Adds the specified namespace to the list of registered namespaces to search when retrieving property values.  The order
     * that namespaces are registered is important, whereas the last namespace registered will be the first to be searched.
     * Properties from the resource bundle identified by the namespace will be loaded into the specified property map.
     * @param namespace    The namespace identifier
     * @param propertyMap  The map into which the property values are loaded
     * @return True if the namespace was successfully registered, false if it was previously registered or its associated
     *          resource bundle was not found.
     * @since 3.1
     */
    public boolean addNamespace(final String namespace, final Map propertyMap) {
        if (namespaces.contains(namespace)) {
            return false;
        }
        try {
            load(namespace, false, propertyMap);
            return namespaces.add(namespace);
        } catch (final MissingResourceException err) {
            return false;
        }
    }

    /**
     * Adds the specified list of namespaces to the list of registered namespaces to search when retrieving property values.  The
     * order that namespaces are registered is important, whereas the last namespace registered will be the first to be searched.
     * @param namespaces The list of namespace identifiers
     * @return True if all namespaces were successfully registered, false if any were previously registered or any of the
     *          associated resource bundles were not found.
     * @since 3.1
     */
    public boolean addNamespaces(final String[] namespaces) {
        return addNamespaces(Arrays.asList(namespaces));
    }

    /**
     * Adds the specified list of namespaces to the list of registered namespaces to search when retrieving property values.  The
     * order that namespaces are registered is important, whereas the last naemspace registered will be the first to be searched.
     * @param namespaces The list of namespace identifiers
     * @return True if all namespaces were successfully registered, false if any were previously registered or any of the
     *          associated resource bundles were not found.
     * @since 3.1
     */
    public boolean addNamespaces(final List namespaces) {
        final Iterator iter = namespaces.iterator();
        boolean allAdded = true;
        while (iter.hasNext()) {
            if (!addNamespace(iter.next().toString())) {
                allAdded = false;
            }
        }
        return allAdded;
    }
    
    /**
     * Removes all naemspaces from the list of registered naemspaces.
     * @since 3.1
     */
    public void clearNamespaces() {
        namespaces.clear();
    }
    
    /**
     * Called by each constructor to initialize the namespaces list and register any namespaces passed in the constructor.
     * @param namespaces  The list of namespace identifiers passed in the constructor
     * @param propertyMap The map into which the property values are loaded if a single namespace is specified
     * @since 3.1
     */
    protected void constructPropertyManager(final List namespaces, final Map propertyMap) {
        this.namespaces = new ArrayList(namespaces == null ? 0 : namespaces.size());
        initializeNamespaces(namespaces, propertyMap);
    }
        
    /**
     * Provided for subclasses to control the evaluation of retrieved property values.
     * @param key The property's case-sensitive key; never null.
     * @param The property value; never null.
     * @return The evaluated value.
	 * @since 3.1
	 */
	protected Object evaluateValue(final String key, final String value) {
        return value;
	}
    
    /**
     * Retrieves the value of the property identified by the specified case-sensitive key.
     * @param key The property's case-sensitive key
     * @return The property value; may be null
     * @since 2.1
     */
    public Object get(final String key) {
        final Iterator iter = namespaces.iterator();
        Object val;
        Map propMap;
        while (iter.hasNext()) {
            propMap = (Map)loadedNamespaces.get(iter.next());
            val = propMap.get(key);
            if (val != null) {
                // If unevaluated, evaluate the value and store in property map
                if (val instanceof UnevaluatedValue) {
                    val = evaluateValue(key, ((UnevaluatedValue)val).get());
                    propMap.put(key, val);
                }
                return val;
            }
        }
        return null;
    }

    /**
     * Returns The list of namespaces registered to be searched for property values
     * @return The list of registered namespaces
     * @since 3.1
     */
    public List getNamespaces() {
        return Collections.unmodifiableList(namespaces);
    }
    
    /**
     * Called by {@link #initializePropertyManager(List, Map)} to register any namespaces passed in the constructor.
     * @param namespaces  The list of namespace identifiers passed in the constructor
     * @param propertyMap The map into which the property values are loaded if a single namespace is specified
     * @since 3.1
     */
    protected void initializeNamespaces(final List namespaces, final Map propertyMap) {
        if (propertyMap != null) {
            addNamespace(namespaces.get(0).toString(), propertyMap);
        } else if (namespaces != null) {
            addNamespaces(namespaces);
        }
    }
    
    /**
     * Returns whether the specified namespace has been registered.
     * @param namespace The namespace identifier
     * @return True if the namespace has been registered
     * @see #addNamespace(String)
     * @see #addNamespaces(List)
     * @since 3.1
     */
    public boolean isNamespaceRegistered(final String namespace) {
        return namespaces.contains(namespace);
    }
    
    /**
     * Removes the specified namespace from the list of registered namespaces.
     * @param namespace The namespace identifier
     * @return True if the namespace was successfully removed, false if it was not previously registered
     * @since 3.1
     */
    public boolean removeNamespace(final String namespace) {
        return namespaces.remove(namespace);
    }
}
