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

//#############################################################################
package com.metamatrix.console.ui.util.property;

import java.beans.PropertyDescriptor;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JTextField;
import javax.swing.text.Document;

import com.metamatrix.core.util.StringUtil;
import com.metamatrix.toolbox.ui.Validator;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;

/**
 * The <code>GuiComponentFactory</code> creates {@link JComponent}s and sets
 * their properties. All JavaBean properties can be set using this class.
 * For example, to create a {@link JButton}, the properties file entries
 * could look like this:<br>
 * <pre>
 *     btn=javax.swing.JButton.class
 *     btn.background=Color.red
 *     btn.text="Red Button"
 *     btn.mnemonic='R'
 *     btn.toolTipText="This is a red button"
 * </pre>
 * After the component identifier, in the above example "btn", and after
 * the period separator, the property name is identified starting with
 * a lowercase letter. A {@link PropertyProvider} identifies which properties
 * files are searched.
 * @version 1.0
 * @author Dan Florian
 */
public class GuiComponentFactory {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * The separator used between component ID and property when composing
     * a properties file key. Currently only a period is used.
     */
    public static final String SEPARATOR = "."; //$NON-NLS-1$

    /**
     * The default properties file used in the {@link #createTextField(String)}
     * method.
     */
    public static final String TYPE_DEFS_PROP =
        "com/metamatrix/console/ui/data/type_defs"; //$NON-NLS-1$

    /** The prefix used to denote data types in property files. */
    public static final String TYPE_PREFIX = "type."; //$NON-NLS-1$

    /** Property suffix for collapsing consecutive spaces to a single space. */
    private static final String COLLAPSE = SEPARATOR + "collapsespaces"; //$NON-NLS-1$

    /** Property suffix for setting textfield columns. */
    private static final String COLUMNS = SEPARATOR + "cols"; //$NON-NLS-1$

    /** Property suffix for identifying the invalid characters. */
    private static final String INVALID_CHARS = SEPARATOR + "invalidchars"; //$NON-NLS-1$

    /** Property suffix for maximum number of characters allowed. */
    private static final String LENGTH_MAX = SEPARATOR + "length.max"; //$NON-NLS-1$

    /** Property suffix for minimum number of characters allowed. */
    private static final String LENGTH_MIN = SEPARATOR + "length.min"; //$NON-NLS-1$

    /** Property suffix for indicating if input is required. */
    private static final String REQUIRED = SEPARATOR + "required"; //$NON-NLS-1$

    /** Property suffix for indicating if leading and trailing spaces are deleted. */
    private static final String TRIM = SEPARATOR + "trim"; //$NON-NLS-1$

    /** Property suffix for identifying the valid characters. */
    private static final String VALID_CHARS = SEPARATOR + "validchars"; //$NON-NLS-1$

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * A map used to cache the {@link PropertyDescriptor}s of a
     * <code>Class</code>. The key is the class, and the value is an
     * array of descriptors.
     */
    private static Map descriptorMap =
        Collections.synchronizedMap(new HashMap());

    /** The provider used when {@link #createTextField(String)} is used. */
    private static PropertyProvider propProvider =
        new PropertyProvider(TYPE_DEFS_PROP);

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    // don't allow no arg construction
    private GuiComponentFactory() {}

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Creates a <code>TextFieldWidget</code> and initializes it by using the
     * given type. If the type is not found by the provider, a default textfield
     * is returned. The default provider is used.
     * @param theType the data type key
     * @return the newly created TextFieldWidget
     * @throws IllegalArgumentException if input parameter is <code>null</code>
     * @see #createTextField(String, PropertyProvider)
     */
    public static TextFieldWidget createTextField(String theType) {
        return createTextField(theType, propProvider);
    }

    /**
     * Creates a <code>TextFieldWidget</code> and initializes by using the given
     * type. If the type is not found by the provider, a default textfield
     * is returned. A typical type entry in a properties file would like like
     * this:<br>
     * <pre>
     *     type.heapsize.length.min="1"
     *     type.heapsize.length.max="4"
     *     type.heapsize.cols=get("type.heapsize.length.max")
     *     type.heapsize.required="true"
     *     type.heapsize.validchars="0-9"
     *     #type.heapsize.invalidchars=""
     * </pre>
     * @param theType the data type key
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     * @return the newly created TextFieldWidget
     * @throws IllegalArgumentException if either input parameter is
     * <code>null</code>
     */
    public static TextFieldWidget createTextField(
        String theType,
        PropertyProvider thePropProvider) {

        if ((theType == null) || (thePropProvider == null)) {
            throw new IllegalArgumentException(
                "Either the type <" + theType + //$NON-NLS-1$
                ">, or the property provider <" + thePropProvider + //$NON-NLS-1$
                "> is null."); //$NON-NLS-1$
        }

        TextFieldWidget txf = new TextFieldWidget();
        ValidationProps props =
            getValidationProperties(theType, thePropProvider);
        new TextFieldInitializer(props, txf);

        return txf;
    }

    /**
     * Indicates if consecutive spaces should be collapsed to one space.
     * @param theType the data type whose information is being requested
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     * @return <code>true</code> if consecutive spaces should be collapsed;
     * <code>false</code> otherwise.
     */
    private static boolean getCollapseSpaces(
        String theType,
        PropertyProvider thePropProvider) {

        // can't call getBoolean since that will return false if the
        // property is not found. and we want the default value to be true
        String temp =
            thePropProvider.getString(TYPE_PREFIX + theType + COLLAPSE, true);
        return (temp == null) ? true : Boolean.valueOf(temp).booleanValue();
    }

    /**
     * Gets the number of columns to size the text component.
     * @param theType the data type whose information is being requested
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     * @return the requested columns or the default value if none is found
     */
    private static int getColumns(
        String theType,
        PropertyProvider thePropProvider) {

        return thePropProvider.getInt(TYPE_PREFIX + theType + COLUMNS, 15);
    }

    /**
     * Gets the invalid characters for the given data type.
     * @param theType the data type whose information is being requested
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     * @return the requested invalid characters or <code>null</code> if all
     * characters are valid
     */
    private static String getInvalidCharacters(
        String theType,
        PropertyProvider thePropProvider) {

        return thePropProvider.getString(TYPE_PREFIX +
                                         theType +
                                         INVALID_CHARS,
                                         true);
    }

    /**
     * Indicates if a user input is required for the given data type.
     * @param theType the data type whose information is being requested
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     * @return <code>true</code> if input is required; <code>false</code>
     * otherwise.
     */
    private static boolean getIsRequired(
        String theType,
        PropertyProvider thePropProvider) {

        return thePropProvider.getBoolean(TYPE_PREFIX + theType + REQUIRED);
    }

    /**
     * Gets the maximum length for the given data type.
     * @param theType the data type whose information is being requested
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     * @return the requested maximum length or the default value if none is found
     */
    private static int getMaximumLength(
        String theType,
        PropertyProvider thePropProvider) {

        return thePropProvider.getInt(TYPE_PREFIX + theType + LENGTH_MAX, 15);
    }

    /**
     * Gets the minimum length for the given data type.
     * @param theType the data type whose information is being requested
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     * @return the requested minimum length or the default value if none is found
     */
    private static int getMinimumLength(
        String theType,
        PropertyProvider thePropProvider) {

        return thePropProvider.getInt(TYPE_PREFIX + theType + LENGTH_MIN, 1);
    }

    /**
     * Indicates if a user input is required for the given data type.
     * @param theType the data type whose information is being requested
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     * @return <code>true</code> if input is required; <code>false</code>
     * otherwise.
     */
    private static boolean getTrimSpaces(
        String theType,
        PropertyProvider thePropProvider) {

        // can't call getBoolean since that will return false if the
        // property is not found. and we want the default value to be true
        String temp =
            thePropProvider.getString(TYPE_PREFIX + theType + TRIM, true);
        return (temp == null) ? true : Boolean.valueOf(temp).booleanValue();
    }

    /**
     * Gets the initial properties of the given type.
     * @param theType the data type whose initial properties is being requested
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     */
    private static ValidationProps getValidationProperties(
        String theType,
        PropertyProvider thePropProvider) {

        ValidationProps props = new ValidationProps();
        props.required = getIsRequired(theType, thePropProvider);
        props.collapse = getCollapseSpaces(theType, thePropProvider);
        props.lengthMin = getMinimumLength(theType, thePropProvider);
        props.lengthMax = getMaximumLength(theType, thePropProvider);
        props.cols = getColumns(theType, thePropProvider);
        props.invalidChars = getInvalidCharacters(theType, thePropProvider);
        props.validChars = getValidCharacters(theType, thePropProvider);
        props.trim = getTrimSpaces(theType, thePropProvider);

        return props;
    }

    /**
     * Gets the valid characters for the given data type.
     * @param theType the data type whose information is being requested
     * @param thePropProvider the provider that determines the file(s) to look
     * at for properties
     * @return the requested valid characters or <code>null</code> if all
     * characters are valid
     */
    private static String getValidCharacters(
        String theType,
        PropertyProvider thePropProvider) {

        return thePropProvider.getString(TYPE_PREFIX +
                                         theType +
                                         VALID_CHARS,
                                         true);
    }

    private static class ValidationProps {
        public boolean collapse = true;
        public int cols = 15;
        public String invalidChars = null;
        public int lengthMax = 15;
        public int lengthMin = 0;
        public boolean required = false;
        public boolean trim = true;
        public String validChars = null;
    }

    // just being used to trim spaces
    // the text widget handles max length
    // required and min length not currently enforced
    private static class TextFieldInitializer implements Validator {
        private ValidationProps props;
        private JTextField txf;

        public TextFieldInitializer(
            ValidationProps theProps,
            JTextField theTextField) {
            props = theProps;
            txf = theTextField;

            // initialize
            txf.setColumns(theProps.cols);
            Document tempDoc = txf.getDocument();
            if (tempDoc instanceof DefaultTextFieldModel) {
                DefaultTextFieldModel doc = (DefaultTextFieldModel)tempDoc;
                doc.setMaximumLength(props.lengthMax);
                // valid/invalid chars
                if (props.invalidChars != null) {
                    try {
                        doc.setInvalidCharacters(props.invalidChars);
                    }
                    catch (ParseException theException) {}
                }
                else if (props.validChars != null) {
                    try {
                        doc.setValidCharacters(props.validChars);
                    }
                    catch (ParseException theException) {}
                }

                doc.addValidator(this);
            }
            txf.setMinimumSize(txf.getPreferredSize());
        }

        public Object validate(final Object theObject) {
            if (!(theObject instanceof String)) {
                throw new IllegalArgumentException(
                    "Object is not a String. Class=" + theObject); //$NON-NLS-1$
            }
            String original = (String)theObject;
            String text = (theObject == null) ? "" : original; //$NON-NLS-1$

            // trim leading/trailing spaces
            if (props.trim) {
                text = text.trim();
            }
            // collapse consecutive spaces
            if (props.collapse) {
                text = StringUtil.replaceAll(text, "  ", " "); //$NON-NLS-1$ //$NON-NLS-2$
            }

            // perform other validation
            boolean result = true;
/*
            int length = text.length();
            if (props.required && (length == 0)) {
                result = false;
            }
            else {
                if (!props.required && (length == 0)) {
                    result = true;
                }
                else if ((length < props.lengthMax) ||
                         (length > props.lengthMax)) {
                    result = false;
                }
            }
*/
            // if text is not the same as original, then spaces have been
            // trimmed, set the textfield
            if (!original.equals(text)) {
                txf.setText(text);
            }
            return new Boolean(result); // change to Boolean.valueOf(result) for 1.4
        }
    }

}
