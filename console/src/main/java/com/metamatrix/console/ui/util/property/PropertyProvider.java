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

//#############################################################################
package com.metamatrix.console.ui.util.property;

import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractButton;
import javax.swing.Icon;
import javax.swing.ImageIcon;

import com.metamatrix.common.properties.ObjectPropertyManager;

/**
 * The <code>PropertyProvider</code> class provides property values on request.
 * It can be configured to look at one or more property files in a predetermined
 * order looking for properties. The first occurrence of the property is returned.
 * This class uses the {@link UIStandards} class to read the property files and
 * to obtain the property values.
 * @version 1.0
 * @author Dan Florian
 */
public class PropertyProvider {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    /** The default error prefix is "**". */
    public static final String DEFAULT_ERROR_PREFIX = "**"; //$NON-NLS-1$

    /** The common properties file used by the Console. */
    public static final String COMMON_PROP =
        "com/metamatrix/console/ui/data/common_ui"; //$NON-NLS-1$

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    /** The error prefix to use when the property is not found. */
    private String errorPrefix = DEFAULT_ERROR_PREFIX;

    /** Declared just to get rid of javadoc warning. */
    protected AbstractButton DO_NOT_USE_1;

    /** Declared just to get rid of javadoc warning. */
    protected ObjectPropertyManager DO_NOT_USE_2;

    /** The icon used when the requested icon is not found. */
    private static Icon missingIcon;

    /**
     * The <code>List</code> of property file names to look for properties.
     */
    private List propFiles = new ArrayList();

    /** A provider configured to only use {@link #COMMON_PROP}. */
    private static PropertyProvider defaultProvider;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Constructs a <code>PropertyProvider</code> that has only one
     * properties file associated with it.
     * @param thePropertiesFile the name of the properties file
     * @throws IllegalArgumentException if argument is <code>null</code> or
     * empty
     */
    public PropertyProvider(String thePropertiesFile) {
        if ((thePropertiesFile == null) || (thePropertiesFile.length() == 0)) {
            throw new IllegalArgumentException(
                "Properties file name is null or empty."); //$NON-NLS-1$
        }
        propFiles.add(thePropertiesFile);
    }

    /**
     * Constructs a <code>PropertyProvider</code> that has one or more
     * property files associated with it.
     * @param thePropertiesCollection a collection of property files
     * @throws IllegalArgumentException if argument is <code>null</code> or
     * empty, or if any of the arguments elements are <code>null</code>, not
     * a {@link String}, or an empty <code>String</code>
     */
    public PropertyProvider(List thePropertiesCollection) {
        if ((thePropertiesCollection == null) ||
            thePropertiesCollection.isEmpty()) {
            throw new IllegalArgumentException(
                "Properties collection is null or empty."); //$NON-NLS-1$
        }
        for (int size=thePropertiesCollection.size(), i=0; i<size; i++) {
            Object obj = thePropertiesCollection.get(i);
            if ((obj == null) || !(obj instanceof String) ||
                (((String)obj).length() == 0)) {
                throw new IllegalArgumentException(
                    "Properties collection contains a null object, " + //$NON-NLS-1$
                    "an object that is not a string, or an empty string."); //$NON-NLS-1$
            }
            if (!propFiles.contains(obj)) {
                propFiles.add(obj);
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Gets the value of the given property as a <code>boolean</code>. Here is
     * an example properties file entry:<br>
     * <pre>filename.required="true"</pre>
     * @param theKey the property whose value is being requested
     * @return the property value as a <code>boolean</code> or
     * <code>false</code> if property not found
     */
    public boolean getBoolean(String theKey) {
        String value = getString(theKey);
        return value.equals("true"); //$NON-NLS-1$
    }

    /**
     * A provider configured only to use {@link #COMMON_PROP}.
     * @return the default property provider
     */
    public static PropertyProvider getDefault() {
        if (defaultProvider == null) {
            defaultProvider = new PropertyProvider(COMMON_PROP);
        }
        return defaultProvider;
    }

    /**
     * Gets the error prefix. The error prefix together with the property name
     * is returned as the property value when the property is not found.
     * @return the error prefix
     */
    public String getErrorPrefix() {
        return errorPrefix;
    }

    /**
     * Gets the icon associated with the given property. If the icon cannot
     * be found, a generic one representing a missing icon is returned.
     * Here is an example of a properties file entry:<br>
     * <pre>icon.back="com/metamatrix/console/images/back_medium.gif"</pre>
     * @param theKey the property whose value is being requested
     * @return the property value as an <code>Icon</code> or a generic
     * icon if not found
     * @see #getMissingIcon()
     */
    public Icon getIcon(String theKey) {
        Icon result = null;
        // first get the file of icon
//        String value = getString(theKey, true);
// this is a temporary fix until the toolbox fixes the problem
// of all keys being global (i.e., keys with the same name in different
// property files will overwrite each other-bad)
        String value = getString("temp." + theKey, true); //$NON-NLS-1$
        if (value == null) {
            result = getMissingIcon();
        }
        else {
            URL url = ClassLoader.getSystemResource(value);
            if (url == null) {
                result = getMissingIcon();
            }
            else {
                result = new ImageIcon(url);
            }
        }
        return result;
    }

    /**
     * Gets the value of the given property as an <code>int</code>. Here is
     * an example properties file entry:<br>
     * <pre>portnum.default="15001"</pre>
     * @param theKey the property whose value is being requested
     * @param theDefault the default value
     * @return the property value as an <code>int</code> or the default
     * value if the property is not found
     */
    public int getInt(
        String theKey,
        int theDefault) {

        int result = theDefault;
        String txt = getString(theKey, true);
        if (txt != null) {
            try {
                result = Integer.parseInt(txt);
            }
            catch (NumberFormatException theException) {/* use default */}
        }
        return result;
    }

    /**
     * Gets the <code>Icon</code> used when the requested one cannot be found.
     * @return the <code>Icon</code> used when the requested one cannot be found
     */
    public static Icon getMissingIcon() {
        if (missingIcon == null) {
            byte[] image = new byte[] {
                  71,   73,   70,   56,   57,   97,   16,    0,   16,    0, // 0
                  -9,    0,    0,    0,   32,    0,    0,   96,    0,   48, // 10
                  80,   48,   64,   64,   64,   80,   80,   80,   95,   95, // 20
                  95,   96,   96,   96,  127,  127,  127,   63,   63,  -33, // 30
                   0,  127,   -1,   64,  127,  -97,   64,  127,  -65,    0, // 40
                 -97,    0,   32, -128,   32,   64, -128,   64,   80, -112, // 50
                  80,  127,  -97,  127,    0,  -65,   -1,   64,  -65,  -65, // 60
                 127,  -65,  -65,  -91,  121,   17, -128,   32, -128,  -97, // 70
                  63,  -97,  -97,  127,  -97,  -65,  127,  -65,  -33,    0, // 80
                 -33,   -1,    0,   -1, -128, -128, -128, -112, -112, -112, // 90
                 -97,  -97,  -97, -113,  -81, -113,  -65,  -97,  -65,  -96, // 100
                 -96,  -96,  -81,  -81,  -81,  -80,  -80,  -80,  -65,  -65, // 110
                 -65,  -65,  -65,   -1,  -64,  -64,  -64,  -49,  -49,  -49, // 120
                 -33,  -33,  -33,  -17,  -17,  -17,   -1,   -1,   -1,    0, // 130
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 140
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 150
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 160
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 170
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 180
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 190
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 200
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 210
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 220
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 230
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 240
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 250
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 260
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 270
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 280
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 290
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 300
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 310
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 320
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 330
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 340
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 350
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 360
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 370
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 380
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 390
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 400
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 410
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 420
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 430
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 440
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 450
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 460
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 470
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 480
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 490
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 500
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 510
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 520
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 530
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 540
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 550
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 560
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 570
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 580
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 590
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 600
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 610
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 620
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 630
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 640
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 650
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 660
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 670
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 680
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 690
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 700
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 710
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 720
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 730
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 740
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 750
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 760
                   0,    0,    0,    0,    0,    0,    0,    0,    0,    0, // 770
                   0,   33,   -7,    4,    1,    0,    0,   20,    0,   44, // 780
                   0,    0,    0,    0,   16,    0,   16,    0,    0,    8, // 790
                 -67,    0,   15,    8,   28,   72,  112,   32, -123, -125, // 800
                   7,   80, -100,   88,  -56, -112,  -31, -127,   17,    7, // 810
                  16,  -98,   40,   81, -126, -125, -125,   13,   28,   40, // 820
                -106,   56, -112,    2,   34, -123,    3,   19,   65,   64, // 830
                 -16,  -48,   64, -128,   70, -114,   29,    5, -122,  124, // 840
                 -16, -128,    1, -128, -109,    3,   98,   14,    0,   89, // 850
                  66,    4, -128,    0,    0,    6, -128,  -48,   40,    2, // 860
                 -60, -120, -103,   19,  107,   14,   48,  -80,   65, -124, // 870
                  70, -118,   39, -128,   26,  -83,    9, -126,  -61,    4, // 880
                   9,   11,   20,   16,   56,  -79,  -31,    4,  -56,    3, // 890
                  25,   53,   74, -120, -112,    0,   65, -121,   14,   35, // 900
                  62,  -98,   24, -127,   33,  107,    9,    9,    9,   72, // 910
                -116,  -40,   16,   86,  -20,    8,   13,   23,   56, -104, // 920
                  96,  -72,  -95,  -61,    9,   10,   39,   58, -128,   28, // 930
                -111,   33, -125,    5,  -86,  117,  -17,  -30,  -19,    0, // 940
                  20,   67, -123,   15,   35,  -64,   30,   60,  120, -126, // 950
                  67,   10,  -96,    4,    6,   36,   22,  -52,   56,  -29, // 960
                 -29, -124,   99,  -85,   46,   30,  -68,  -16,  113,    1, // 970
                -127,   29,   54,   83,   -8,   41,   51,  102,   64,    0, // 980
                   0,   59                                                  // 990
            };
            missingIcon = new ImageIcon(image);
        }
        return missingIcon;
    }

    /**
     * Gets the value of the given property as an <code>Object</code>. Here is
     * an example properties file entry:<br>
     * <pre>importexport.extensions=new String[] {"xml"}</pre>
     * @param theKey the property whose value is being requested
     * @return the property value as an <code>Object</code> or <code>null</code>
     * if not found
     */
    public Object getObject(String theKey) {
        Object result = null;
        for (int size=propFiles.size(), i=0; i<size; i++) {
        	String fileName = (String)propFiles.get(i);
            final ObjectPropertyManager props = new ObjectPropertyManager(
            		fileName);
            //Code in ObjectPropertyManager changed, addNamespace() call no
            //longer needed.  BWP 04/03/03
            //props.addNamespace(fileName);
            result = props.get(theKey);
            if (result != null) {
                break;
            }
        }
        return result;
    }

    /**
     * Gets the first properties file name where the property was found.
     * @param theKey the property whose value is being requested
     * @return the properties file name where the property was found
     */
    public String getPropertiesFile(String theKey) {
        String result = null;
        for (int size=propFiles.size(), i=0; i<size; i++) {
        	String fileName = (String)propFiles.get(i);
            final ObjectPropertyManager props = new ObjectPropertyManager(
            		fileName);
            //Code in ObjectPropertyManager changed, addNamespace() call no 
            //longer needed.  BWP 04/03/03
            //props.addNamespace(fileName);
            result = props.getString(theKey);
            if (result != null) {
                result = (String)propFiles.get(i);
                break;
            }
        }
        return result;
    }

    /**
     * Gets the requested property value. This version of <code>getString</code>
     * never returns <code>null</code>. If the property cannot be found, the
     * property name is prepended with the error prefix and returned.
     * @param theKey the property whose value is being requested
     * @return the property value or the property value with the error prefix
     * prepended to it
     */
    public String getString(String theKey) {
        return getString(theKey, false);
    }

    /**
     * Gets the requested property value. If the <code>theReturnNullFlag</code>
     * is <code>true</code> a <code>null</code> value is returned if the
     * property cannot be found. If set to <code>false</code> and the
     * property cannot be found, the property name is prepended with the
     * error prefix and returned.
     * @param theKey the property whose value is being requested
     * @param theReturnNullFlag indicates if <code>null</code> should be
     * returned if the property is not found
     * @return the property value, the property value with the error prefix
     * prepended to it, or <code>null</code>
     */
    public String getString(
        String theKey,
        boolean theReturnNullFlag) {

        String result = null;
        for (int size=propFiles.size(), i=0; i<size; i++) {
        	String fileName = (String)propFiles.get(i);
            final ObjectPropertyManager props = new ObjectPropertyManager(
            		fileName);
            //Code in ObjectPropertyManager changed, addNamespace() call no
            //longer needed.  BWP 04/03/03
            //props.addNamespace(fileName);
            result = props.getString(theKey);
            if (result != null) {
                break;
            }
        }
        if (theReturnNullFlag) {
            return result;
        }
        return (result == null) ? "**" + theKey : result; //$NON-NLS-1$
    }

    /**
     * Gets the requested property value and inserts the argument element(s)
     * into the returned message. Here is an example properties file entry:<br>
     * <pre>confirmdelete.title="Delete Host {0}"</pre>
     * @param theKey the property whose value is being requested
     * @param theArgs the arguments being inserted into the property value
     * @return the property value with the arguments inserted in the
     * appropriate place or <code>null</code> if not found
     */
    public String getString(
        String theKey,
        Object[] theArgs) {

        String result = null;
        String fmt = getString(theKey, true);
        if (fmt != null) {
          MessageFormat formatter = new MessageFormat(fmt);
          result = formatter.format(theArgs);
        }
        return result;
    }

    /**
     * Sets the error prefix. When the property cannot be found, the
     * error prefix is prepended to the property name and returned as the
     * property value. If the parameter is <code>null</code> or empty,
     * the default prefix is used.
     * @param theErrorPrefix the new error prefix
     * @see #DEFAULT_ERROR_PREFIX
     */
    public void setErrorPrefix(String theErrorPrefix) {
        if ((theErrorPrefix == null) || (theErrorPrefix.length() == 0)) {
            errorPrefix = DEFAULT_ERROR_PREFIX;
        }
        else {
            errorPrefix = theErrorPrefix;
        }
    }

}

