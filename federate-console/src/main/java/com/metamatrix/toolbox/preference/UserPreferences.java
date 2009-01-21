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

package com.metamatrix.toolbox.preference;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.DefaultPropertyAccessPolicy;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyAccessPolicy;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.transaction.UserTransactionFactory;
import com.metamatrix.common.transaction.manager.SimpleUserTransactionFactory;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.toolbox.ToolboxPlugin;
import com.metamatrix.toolbox.ui.widget.util.PropertyLoader;
import com.metamatrix.toolbox.ui.widget.util.StringFilter;

/**
 * @stereotype singleton
 */
public class UserPreferences implements PropertiedObject {

	public static final String LOG_CONTEXT = "USER_PREF"; //$NON-NLS-1$

	public static final String PROPERTY_PREFIX = PropertyLoader.PROPERTY_PREFIX;
	private static final String PROPERTY_DELIM = "."; //$NON-NLS-1$

	public static final String USER_PREFERENCES_DEFINITION_FILE_PROPERTY_NAME = "metamatrix.toolbox.userPrefs"; //$NON-NLS-1$

	/**
	 * @supplierCardinality 1
	 * @supplierRole instance
	 */
	private static UserPreferences INSTANCE = new UserPreferences();

	/**
	 * @supplierRole userPrefProperties
	 * @supplierCardinality 1
	 * @link aggregationByValue
	 */
	private String userPrefFileName;
	private Properties userPrefProperties;
	private Properties unmodifiableUserPrefProperties;

	/**
	 * @supplierCardinality 1
	 * @supplierRole configProperties
	 * @link aggregation
	 */
	private Properties configProperties;

	private boolean readOnly = false;
	private boolean changed = false;

	private UserPreferences() {
		readOnly = false; // this is assumed!
        
        //Load the properties and user preference values
        try {
            configProperties = CurrentConfiguration.getInstance().getProperties();
        } catch (ConfigurationException e) {
            configProperties = new Properties();
        }
        
		this.loadProperties();
	}

	public static UserPreferences getInstance() {
		return INSTANCE;
	}

    /**
     * Set the configuration properties used to initialize this. 
     * Useful for testing.
     * @param properties
     */
    public void setConfigProperties(Properties properties) {
        this.configProperties = properties;
        
        //re-init based on these properties
        this.loadProperties();
    }
    
    
	public boolean hasChanges() {
		return this.changed;
	}

	public void saveChanges() {
		if (this.changed) {
			// This should write out the properties file that CurrentConfig accesses
			this.saveChangedProperties();

			// and then force a re-read of the properties.
			this.loadProperties();
		}
	}

	public void clearChanges() {
		if (this.changed) {
			this.loadProperties();
		}
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public PropertiedObjectEditor getPropertiedObjectEditor() {
		return new UserPreferencesEditor();
	}

	public PropertiedObject getPropertiedObject() {
		return this;
	}

	public Object getValue(String propName) {
		Assertion.isNotNull(propName, ToolboxPlugin.Util.getString("UserPreferences.The_property_definition_reference_may_not_be_null_4")); //$NON-NLS-1$
		Assertion.isNotZeroLength(propName, ToolboxPlugin.Util.getString("UserPreferences.The_property_definition_reference_may_not_be_zero-length_5")); //$NON-NLS-1$

		Object result = userPrefProperties.getProperty(propName);
		if (result == null) {
			result = configProperties.getProperty(propName);
		}
		return result;
	}

	/**
	 * Convenience method for determining if a property value is "true" or "false"
	 */
	public boolean getBooleanValue(String propName) {
		Object value = getValue(propName);
		boolean result = false;
		if (value != null && value instanceof String) {
			String s = (String) value;
			result = Boolean.TRUE.toString().equalsIgnoreCase(s);
		}
		return result;
	}

	/**
	 * set this entity's value for the specified property name.
	 * @param propName the name of the property
	 * @param value the object to store for this property name.  If the value is null or a zero-length
	 * String, then the specified propName will be removed from this entity's list of properties.
	 */
	public void setValue(String propName, Object value) {
		Assertion.isNotNull(propName, ToolboxPlugin.Util.getString("UserPreferences.The_property_name_may_not_be_null_6")); //$NON-NLS-1$
		Assertion.isNotZeroLength(propName, ToolboxPlugin.Util.getString("UserPreferences.The_property_name_may_not_be_zero-length_7")); //$NON-NLS-1$

		Object previousValue = userPrefProperties.getProperty(propName);
		if (value == null
			|| ((value instanceof String) && ((String) value).length() == 0)) {
			userPrefProperties.remove(propName);
		} else {
			userPrefProperties.setProperty(propName, value.toString());
		}
		if (previousValue != null
			&& value != null
			&& !previousValue.equals(value)) {
			LogManager.logDetail(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.Marking_user_preferences_as_changed_(property___8") + propName + "\")"); //$NON-NLS-1$ //$NON-NLS-2$
			changed = true;
		} else if (previousValue != null && value == null) {
			LogManager.logDetail(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.Marking_user_preferences_as_changed_(property___10") + propName + "\")"); //$NON-NLS-1$ //$NON-NLS-2$
			changed = true;
		} else if (previousValue == null && value != null) {
			LogManager.logDetail(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.Marking_user_preferences_as_changed_(property___12") + propName + "\")"); //$NON-NLS-1$ //$NON-NLS-2$
			changed = true;
		}

	}

	/**
	 * Store an array of values as their toString returns, delimited by the specified delimiter
	 * @param propName the name of the property
	 * @param values a Collection of objects to store for this property name.  If the collection is either null
	 * or empty, then the specified propName will be removed from this entity's list of properties.
	 */
	public void setValues(String propName, Collection values, char delimiter) {
		Assertion.isNotNull(propName, ToolboxPlugin.Util.getString("UserPreferences.The_property_name_may_not_be_null_14")); //$NON-NLS-1$
		Assertion.isNotZeroLength(propName, ToolboxPlugin.Util.getString("UserPreferences.The_property_name_may_not_be_zero-length_15")); //$NON-NLS-1$

		userPrefProperties.getProperty(propName);
		if (values != null && !values.isEmpty()) {
			StringBuffer buf = new StringBuffer();
			Iterator iter = values.iterator();
			while (iter.hasNext()) {
				buf.append(iter.next().toString());
				if (iter.hasNext()) {
					buf.append(delimiter);
				}
			}
			setValue(propName, buf.toString());
		} else {
			//remove the propName from this entity's properties
			setValue(propName, null);
		}
	}

    public void removeValue(String propName) {
        userPrefProperties.remove(propName);
    }
    
	/**
	 * return an collection of String values tokenized buy the specified delimiter
	 */
	public Collection getValues(String propName, char delimiter) {
		Object value = getValue(propName);
		Collection result = Collections.EMPTY_LIST;
		if (value != null) {
			Assertion.assertTrue(value instanceof String, ToolboxPlugin.Util.getString("UserPreferences.The_value_of_a_property_definition_used_by_getValues_must_be_of_type_String_16")); //$NON-NLS-1$
			StringTokenizer tok =
				new StringTokenizer(
					(String) value,
					new String(new char[] { delimiter }));
			result = new ArrayList(tok.countTokens());
			while (tok.hasMoreTokens()) {
				result.add(tok.nextToken());
			}
		}
		return result;
	}

	/**
	 * Return an unmodifiable Properties object containing all user preference properties.
	 * @return all properties beneath the specified node, plus an additional property named PROPERTY_PREFIX
	 * with the value of the specified prefix.  Will not return null.
	 */
	public Properties getProperties() {
		return unmodifiableUserPrefProperties;
	}

	/**
	 * Return an unmodifiable Properties object containing all user preference properties that
	 * exist beneath the specified property name prefix.
	 * @param prefix the string filter that identifies the root of the property names to be returned;
	 * if null or zero-length, all properties are returned.
	 * @return all properties beneath the specified node, plus an additional property named PROPERTY_PREFIX
	 * with the value of the specified prefix.  Will not return null.
	 */
	public Properties getPropertiesBranch(String prefix) {
		if (prefix == null || prefix.length() == 0) {
			return this.getProperties();
		}

		// create the branch Properties object and load in the prefix property and value
		Properties branch = new Properties();
		if (prefix.endsWith(PROPERTY_DELIM)) {
			branch.put(
				PROPERTY_PREFIX,
				prefix.substring(0, prefix.length() - 1));
			// strip off the '.'
		} else {
			branch.put(PROPERTY_PREFIX, prefix);
		}

		// Loop through all of the config properties, loading any that begin with the prefix into a new Properties collection
		// This is required since 'keySet' doesn't return the names of default properties.
		Properties props = configProperties;
		Iterator iter = configProperties.keySet().iterator();
		while (iter.hasNext()) {
			String key = (String) iter.next();
			if (key.startsWith(prefix)) {
				Object val = props.get(key);
				if (val != null && !((String) val).equals("")) { //$NON-NLS-1$
					int indx = prefix.length();
					String newKey = key.substring(indx);
					if (newKey.startsWith(PROPERTY_DELIM)) {
						newKey = newKey.substring(1); // strip off the '.'
					}
					branch.put(newKey, val);
				}
			}
		}

		// Loop through all of the changed properties, loading any that begin with the prefix into a new Properties collection
		// This will overwrite some properties (that have changed).
		iter = unmodifiableUserPrefProperties.keySet().iterator();
		while (iter.hasNext()) {
			String key = (String) iter.next();
			if (key.startsWith(prefix)) {
				Object val = props.get(key);
				if (val != null && !((String) val).equals("")) { //$NON-NLS-1$
					int indx = prefix.length();
					String newKey = key.substring(indx);
					if (newKey.startsWith(PROPERTY_DELIM)) {
						newKey = newKey.substring(1); // strip off the '.'
					}
					branch.put(newKey, val);
				}
			}
		}
		return branch;
	}

	/**
	 * Return an unmodifiable Properties object that contains only those properties
	 * that match the specified filter.
	 * The filter is a string that may contain the '*' character as a wildcard one or more times
	 * in the string.  For example, the following filter:
	 * <p>
	 * <pre>       *metamatrix.*</pre>
	 * <p>
	 * finds all properties that contain somewhere in the property name the string "metamatrix.".
	 * <p>
	 * The filter is case sensitive.
	 * @param filter the string filter that will be used to narrow the set of properties returned;
	 * all properties are returned if the filter is null, zero-length or equal to "*".
	 * @return all properties specified with the filter pattern; never null
	 */
	public Properties getProperties(String filter) {
		return getProperties(filter, false);
	}

	/**
	 * Return an unmodifiable Properties object that contains only those properties
	 * that match the specified filter.
	 * The filter is a string that may contain the '*' character as a wildcard one or more times
	 * in the string.  For example, the following filter:
	 * <p>
	 * <pre>       *metamatrix.*</pre>
	 * <p>
	 * finds all properties that contain somewhere in the property name the string "metamatrix.".
	 * @param filter the string filter that will be used to narrow the set of properties returned;
	 * all properties are returned if the filter is null, zero-length or equal to "*".
	 * @param ignoreCase true if the case of the property names is to be ignored
	 * when using the filter.
	 * @return all properties specified with the filter pattern; never null
	 */
	public Properties getProperties(String filter, boolean ignoreCase) {
		if (filter == null || filter.length() == 0 || "*".equals(filter)) { //$NON-NLS-1$
			return this.getProperties();
		}

		StringFilter stringFilter = new StringFilter(filter, ignoreCase);
		Properties branch = new Properties();

		// Loop through all of the config properties, loading any that begin with the prefix into a new Properties collection
		// This is required since 'keySet' doesn't return the names of default properties.
		Properties props = configProperties;
		Iterator iter = configProperties.keySet().iterator();
		while (iter.hasNext()) {
			String key = (String) iter.next();
			if (stringFilter.includes(key)) {
				branch.put(key, props.get(key));
			}
		}

		// Loop through all of the changed properties, loading any that begin with the prefix into a new Properties collection
		// This will overwriter some properties (that have changed).
		iter = unmodifiableUserPrefProperties.keySet().iterator();
		while (iter.hasNext()) {
			String key = (String) iter.next();
			if (stringFilter.includes(key)) {
				branch.put(key, props.get(key));
			}
		}
		return branch;
	}

	private void loadProperties() {
		LogManager.logDetail(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.Loading_user_preferences_20")); //$NON-NLS-1$
		userPrefProperties = new Properties(configProperties);
		unmodifiableUserPrefProperties =
			new UnmodifiableProperties(userPrefProperties);

		// Try to load the user preferences from the specified file ...
		userPrefFileName =
			configProperties.getProperty(
				USER_PREFERENCES_DEFINITION_FILE_PROPERTY_NAME);
		LogManager.logDetail(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.User_preferences_file_set_to___21") + userPrefFileName + "\""); //$NON-NLS-1$ //$NON-NLS-2$
		if (userPrefFileName == null || userPrefFileName.length() == 0) {
			LogManager.logInfo(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.User_preferences_filename_is_invalid_or_not_specified_23")); //$NON-NLS-1$
		} else {
			File prefFile = new File(userPrefFileName);
			if (!prefFile.exists()) {
				LogManager.logInfo(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.User_preferences_file_not_found_24")); //$NON-NLS-1$
			} else if (prefFile.isDirectory()) {
				LogManager.logError(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.User_preferences_file___25") + userPrefFileName + ToolboxPlugin.Util.getString("UserPreferences.__is_not_valid_(may_be_a_directory)_26")); //$NON-NLS-1$ //$NON-NLS-2$
			} else {

				InputStream propStream = null;
				try {
					propStream = new FileInputStream(userPrefFileName);
				} catch (FileNotFoundException ex) {
					LogManager.logInfo(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.User_preferences_file_not_found_27")); //$NON-NLS-1$
				}
				if (propStream != null) {
					try {
						userPrefProperties.load(propStream);
						propStream.close();
					} catch (IOException e) {
						LogManager.logError(LOG_CONTEXT, e, ToolboxPlugin.Util.getString("UserPreferences.Unable_to_read_user_preferences_file___28") + userPrefFileName + "\""); //$NON-NLS-1$ //$NON-NLS-2$
					}
				}
			}
		}

		// Create the editable user preferences ...
		changed = false;
	}

	private void saveChangedProperties() {
		OutputStream propStream = null;
		try {
			propStream = new FileOutputStream(userPrefFileName);
		} catch (FileNotFoundException ex) {
			LogManager.logInfo(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.User_preferences_file_is_not_valid_(may_be_a_directory)_30")); //$NON-NLS-1$
			//throw new PropertyLoaderException("Property file \"" + userPrefFileName + "\" is not valid");
		}
		if (propStream != null) {
			try {
				LogManager.logInfo(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.Saving_user_preferences_to_file___31") + userPrefFileName + "\""); //$NON-NLS-1$ //$NON-NLS-2$
				//                newP.store(fos, ); //$NON-NLS-1$

				userPrefProperties.store(propStream, ToolboxPlugin.Util.getString("UserPreferences.MetaMatrix_preferences_file_-_all_rights_reserved_33")); //$NON-NLS-1$
				propStream.close();
			} catch (IOException e) {
				LogManager.logError(LOG_CONTEXT, e, ToolboxPlugin.Util.getString("UserPreferences.Unable_to_save_user_preferences_to_file___34") + userPrefFileName + "\""); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		changed = false;
		unmodifiableUserPrefProperties =
			new UnmodifiableProperties(userPrefProperties);

		LogManager.logDetail(LOG_CONTEXT, ToolboxPlugin.Util.getString("UserPreferences.Marking_user_preferences_as_saved_36")); //$NON-NLS-1$
	}

	private void print(PrintStream stream) {
		System.out.println(ToolboxPlugin.Util.getString("UserPreferences.User_Preferences__37")); //$NON-NLS-1$
		PropertiesUtils.print(stream, userPrefProperties);
		System.out.println(ToolboxPlugin.Util.getString("UserPreferences.Configuration_Properties__38")); //$NON-NLS-1$
		PropertiesUtils.print(stream, configProperties);
	}

	public static void main(String[] args) {
		System.out.println(ToolboxPlugin.Util.getString("UserPreferences.Getting_user_preferences..._n_39")); //$NON-NLS-1$
		UserPreferences prefs = UserPreferences.getInstance();
		System.out.println(ToolboxPlugin.Util.getString("UserPreferences._nPrinting_user_preferences..._n_40")); //$NON-NLS-1$
		prefs.print(System.out);

		prefs.setValue(ToolboxPlugin.Util.getString("UserPreferences.new.user.pref.prop1_41"), ToolboxPlugin.Util.getString("UserPreferences.value1_42")); //$NON-NLS-1$ //$NON-NLS-2$
		prefs.setValue(ToolboxPlugin.Util.getString("UserPreferences.new.user.pref.prop2_43"), ToolboxPlugin.Util.getString("UserPreferences.value1a_44")); //$NON-NLS-1$ //$NON-NLS-2$
		System.out.println(ToolboxPlugin.Util.getString("UserPreferences._nPrinting_user_preferences_after_adding_2_properties_..._n_45")); //$NON-NLS-1$
		prefs.print(System.out);

		prefs.clearChanges();
		System.out.println(ToolboxPlugin.Util.getString("UserPreferences._nPrinting_user_preferences_after_clearing_changes_..._n_46")); //$NON-NLS-1$
		prefs.print(System.out);

		prefs.setValue(ToolboxPlugin.Util.getString("UserPreferences.new.user.pref.prop3_47"), ToolboxPlugin.Util.getString("UserPreferences.value2_48")); //$NON-NLS-1$ //$NON-NLS-2$
		prefs.setValue(ToolboxPlugin.Util.getString("UserPreferences.new.user.pref.prop4_49"), ToolboxPlugin.Util.getString("UserPreferences.value3_50")); //$NON-NLS-1$ //$NON-NLS-2$
		prefs.setValue(ToolboxPlugin.Util.getString("UserPreferences.metamatrix.log_51"), ToolboxPlugin.Util.getString("UserPreferences.0_52")); //$NON-NLS-1$ //$NON-NLS-2$
		System.out.println(ToolboxPlugin.Util.getString("UserPreferences._nPrinting_user_preferences_after_after_adding_3_properites_..._n_53")); //$NON-NLS-1$
		prefs.print(System.out);
		prefs.saveChanges();
		System.out.println(ToolboxPlugin.Util.getString("UserPreferences._nPrinting_user_preferences_after_saving_changes_..._n_54")); //$NON-NLS-1$
		prefs.print(System.out);

		LogManager.stop();
		System.out.println(ToolboxPlugin.Util.getString("UserPreferences._nCompleted._55")); //$NON-NLS-1$
	}
}

class UserPreferencesEditor implements PropertiedObjectEditor {

	private PropertyAccessPolicy policy;
	private UserTransactionFactory txnFactory;
	private List propertyDefns;

	UserPreferencesEditor(
		PropertyAccessPolicy policy,
		UserTransactionFactory txnFactory) {
		Assertion.isNotNull(policy, ToolboxPlugin.Util.getString("UserPreferences.The_PropertyAccessPolicy_reference_may_not_be_null_56")); //$NON-NLS-1$
		Assertion.isNotNull(txnFactory, ToolboxPlugin.Util.getString("UserPreferences.The_UserTransactionFactory_reference_may_not_be_null_57")); //$NON-NLS-1$
		this.policy = policy;
		this.txnFactory = txnFactory;
		this.propertyDefns = new ArrayList();
	}
	UserPreferencesEditor() {
		this(
			new DefaultPropertyAccessPolicy(),
			new SimpleUserTransactionFactory());
	}

	protected UserPreferences assertUserPreferences(PropertiedObject obj) {
		Assertion.isNotNull(obj, ToolboxPlugin.Util.getString("UserPreferences.The_PropertiedObject_reference_may_not_be_null_58")); //$NON-NLS-1$
		Assertion.assertTrue(obj instanceof UserPreferences, ToolboxPlugin.Util.getString("UserPreferences.The_specified_PropertiedObject_is_not_am_instance_of_UserPreferences_59")); //$NON-NLS-1$
		return (UserPreferences) obj;
	}
	public void setPropertyDefinitions(List propDefns) {
		Assertion.isNotNull(propDefns, ToolboxPlugin.Util.getString("UserPreferences.The_list_of_PropertyDefinition_references_may_not_be_null_60")); //$NON-NLS-1$
		this.propertyDefns =
			Collections.unmodifiableList(new ArrayList(propDefns));
	}

	public List getPropertyDefinitions(PropertiedObject obj) {
		assertUserPreferences(obj);
		return this.propertyDefns;
	}

	public List getAllowedValues(
		PropertiedObject obj,
		PropertyDefinition def) {
		Assertion.isNotNull(def, ToolboxPlugin.Util.getString("UserPreferences.The_PropertyDefinition_reference_may_not_be_null_61")); //$NON-NLS-1$
		return def.getAllowedValues();
	}

	public Object getValue(PropertiedObject obj, PropertyDefinition def) {
		UserPreferences prefs = assertUserPreferences(obj);
		Assertion.isNotNull(def, ToolboxPlugin.Util.getString("UserPreferences.The_PropertyDefinition_reference_may_not_be_null_62")); //$NON-NLS-1$
		return prefs.getValue(def.getName());
	}
	public boolean isValidValue(
		PropertiedObject obj,
		PropertyDefinition def,
		Object value) {
		assertUserPreferences(obj);
		Assertion.isNotNull(def, ToolboxPlugin.Util.getString("UserPreferences.The_PropertyDefinition_reference_may_not_be_null_63")); //$NON-NLS-1$

		// Check for a null value ...
		if (value == null) {
			return (def.getMultiplicity().getMinimum() == 0);
			// only if minimum==0 is value allowed to be null
		}
		// From this point forward, the value is never null
		return def.getPropertyType().isValidValue(value);
	}
	public void setValue(
		PropertiedObject obj,
		PropertyDefinition def,
		Object value) {
		UserPreferences prefs = assertUserPreferences(obj);
		Assertion.isNotNull(def, ToolboxPlugin.Util.getString("UserPreferences.The_PropertyDefinition_reference_may_not_be_null_64")); //$NON-NLS-1$
		prefs.setValue(def.getName(), value);
	}
	public PropertyAccessPolicy getPolicy() {
		return this.policy;
	}
	public void setPolicy(PropertyAccessPolicy policy) {
		if (policy == null) {
			this.policy = new DefaultPropertyAccessPolicy();
		} else {
			this.policy = policy;
		}
	}

	// ########################## PropertyAccessPolicy Methods ###################################

	public boolean isReadOnly(PropertiedObject obj) {
		UserPreferences prefs = assertUserPreferences(obj);
		return prefs.isReadOnly();
	}

	public boolean isReadOnly(PropertiedObject obj, PropertyDefinition def) {
		Assertion.isNotNull(obj, ToolboxPlugin.Util.getString("UserPreferences.The_PropertiedObject_reference_may_not_be_null_65")); //$NON-NLS-1$
		Assertion.isNotNull(def, ToolboxPlugin.Util.getString("UserPreferences.The_PropertyDefinition_reference_may_not_be_null_66")); //$NON-NLS-1$
		return this.policy.isReadOnly(obj, def);
	}

	public void setReadOnly(
		PropertiedObject obj,
		PropertyDefinition def,
		boolean readOnly) {
		Assertion.isNotNull(obj, ToolboxPlugin.Util.getString("UserPreferences.The_PropertiedObject_reference_may_not_be_null_67")); //$NON-NLS-1$
		Assertion.isNotNull(def, ToolboxPlugin.Util.getString("UserPreferences.The_PropertyDefinition_reference_may_not_be_null_68")); //$NON-NLS-1$
		this.policy.setReadOnly(obj, def, readOnly);
	}

	public void setReadOnly(PropertiedObject obj, boolean readOnly) {
		Assertion.isNotNull(obj, ToolboxPlugin.Util.getString("UserPreferences.The_PropertiedObject_reference_may_not_be_null_69")); //$NON-NLS-1$
		this.policy.setReadOnly(obj, readOnly);
	}

	public void reset(PropertiedObject obj) {
		Assertion.isNotNull(obj, ToolboxPlugin.Util.getString("UserPreferences.The_PropertiedObject_reference_may_not_be_null_70")); //$NON-NLS-1$
		assertUserPreferences(obj);
		this.policy.reset(obj);
	}

	// ########################## UserTransactionFactory Methods ###################################

	public UserTransaction createReadTransaction() {
		return this.txnFactory.createReadTransaction();
	}
	public UserTransaction createWriteTransaction() {
		return this.txnFactory.createWriteTransaction();
	}
	public UserTransaction createWriteTransaction(Object source) {
		return this.txnFactory.createWriteTransaction(source);
	}

}
