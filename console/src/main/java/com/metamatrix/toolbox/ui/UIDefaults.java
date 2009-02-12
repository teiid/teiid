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

package com.metamatrix.toolbox.ui;

import java.awt.Color;
import java.awt.Font;
import java.awt.Insets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.swing.Icon;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.plaf.ComponentUI;

import bsh.Interpreter;

import com.metamatrix.common.properties.ObjectPropertyManager;

/**
 * @since 2.1
 */
public class UIDefaults extends ObjectPropertyManager {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    private static final String DEFAULT_PROPERTY_FILE_NAME = "com/metamatrix/toolbox/ui/uiDefaults"; //$NON-NLS-1$
    
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################
    
    private static UIDefaults instance;
    
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
    
    /**
    @since 2.1
    */
    public static UIDefaults getInstance() {
        if (instance == null) {
            instance = new UIDefaults();
        }
        return instance;
    }
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    /**
    @since 2.1
    */
    protected UIDefaults() {
        this((List)null);
    }
    
    /**
    @since 2.1
    */
    protected UIDefaults(final String namespace) {
        this(new String[] {namespace});
    }
    
    /**
    @since 2.1
    */
    protected UIDefaults(final String[] namespaces) {
        this(Arrays.asList(namespaces));
    }
    
    /**
    @since 2.1
    */
    protected UIDefaults(final List namespaces) {
        super(namespaces);
        initializeUIDefaults();
    }
    
    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
    @since 2.1
    */
    public boolean addNamespace(final String namespace) {
        return addNamespace(namespace, UIManager.getDefaults());
    }
    
    /**
    @since 2.1
    */
    public Border getBorder(final String key) {
        return (Border)getObject(key, Border.class);
    }
    
    /**
    @since 2.1
    */
    public Border getBorder(final String key, final Color defaultValue) {
        return (Border)getObject(key, Border.class, defaultValue);
    }
    
    /**
    @since 2.1
    */
    public Color getColor(final String key) {
        return (Color)getObject(key, Color.class);
    }
    
    /**
    @since 2.1
    */
    public Color getColor(final String key, final Color defaultValue) {
        return (Color)getObject(key, Color.class, defaultValue);
    }
    
    /**
    @since 2.1
    */
    public Font getFont(final String key) {
        return (Font)getObject(key, Font.class);
    }
    
    /**
    @since 2.1
    */
    public Font getFont(final String key, final Font defaultValue) {
        return (Font)getObject(key, Font.class, defaultValue);
    }
    
    /**
    @since 2.1
    */
    public Icon getIcon(final String key) {
        return (Icon)getObject(key, Icon.class);
    }
    
    /**
    @since 2.1
    */
    public Icon getIcon(final String key, final Icon defaultValue) {
        return (Icon)getObject(key, Icon.class, defaultValue);
    }
    
    /**
    @since 2.1
    */
    public Insets getInsets(final String key) {
        return (Insets)getObject(key, Insets.class);
    }
    
    /**
    @since 2.1
    */
    public Insets getInsets(final String key, final Insets defaultValue) {
        return (Insets)getObject(key, Insets.class, defaultValue);
    }
    
    /**
    @since 2.1
    */
    public ComponentUI getLookAndFeel(final String key) {
        return (ComponentUI)getObject(key, ComponentUI.class);
    }
    
    /**
    @since 2.1
    */
    public ComponentUI getLookAndFeel(final String key, final ComponentUI defaultValue) {
        return (ComponentUI)getObject(key, ComponentUI.class, defaultValue);
    }

    /**
     * @since 2.1
     */
    protected Object getObject(final String key, final Class valueClass) {
        final Object val = get(key);
        assertClass(key, val, valueClass);
        return val;
    }

    /**
     * @since 2.1
     */
    protected Object getObject(final String key, final Class valueClass, final Object defaultValue) {
        final Object val = get(key);
        if (val == null) {
            return defaultValue;
        }
        assertClass(key, val, valueClass);
        return val;
    }
    
    /**
    @since 2.1
    */
    protected void initializeNamespaces(final List namespaces, final Map propertyMap) {
        addNamespace(DEFAULT_PROPERTY_FILE_NAME);
        super.initializeNamespaces(namespaces, propertyMap);
    }
        
    /**
    @since 2.1
    */
    protected void initializeUIDefaults() {
        try {
            final Interpreter interpreter = getInterpreter();
            interpreter.eval("import javax.swing.border.*;"); //$NON-NLS-1$
            interpreter.eval("import java.net.URL;"); //$NON-NLS-1$
            interpreter.eval("import com.metamatrix.toolbox.ui.*;"); //$NON-NLS-1$
            interpreter.eval("import com.metamatrix.toolbox.ui.widget.*;"); //$NON-NLS-1$
            interpreter.eval("icon(String path) {" + //$NON-NLS-1$
                "final URL url = ClassLoader.getSystemResource(path);" + //$NON-NLS-1$
                "if (url == null) {" + //$NON-NLS-1$
                    "return null;" + //$NON-NLS-1$
                "}" + //$NON-NLS-1$
                "return new ImageIcon(url);" + //$NON-NLS-1$
            "}"); //$NON-NLS-1$
            // Must evaluate all values since JDK uses these via UIManager
            final Object[] keys = UIManager.getDefaults().keySet().toArray();
            Object key;
            for (int ndx = keys.length;  --ndx >= 0;) {
                key = keys[ndx];
                if (key instanceof String) {
                    get((String)key);
                }
            }
        } catch (final Exception err) {
            throwRuntimeException(err);
        }
    }
}
