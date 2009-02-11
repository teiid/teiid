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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget;

// System imports
import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.util.ArrayList;
import java.util.Iterator;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.ActionMap;
import javax.swing.Box;
import javax.swing.Icon;
import javax.swing.InputMap;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPasswordField;
import javax.swing.KeyStroke;
import javax.swing.border.Border;

import com.metamatrix.toolbox.ui.IconConstants;
import com.metamatrix.toolbox.ui.UIDefaults;

/**
This panel should be used by all MetaMatrix products as the means for a user to login to a system.
@since 2.0
@version 2.0
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class LoginPanel extends DialogPanel
implements IconConstants {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
     
    private static final UIDefaults DFLTS = UIDefaults.getInstance();

    public static final String PROPERTY_PREFIX = "LoginPanel.";
    public static final String FIELD_COLUMNS_PROPERTY   = PROPERTY_PREFIX + "fieldColumns";
    public static final String LOGO_BORDER_PROPERTY     = PROPERTY_PREFIX + "logoBorder";
    public static final String USER_NAME_LABEL_PROPERTY = PROPERTY_PREFIX + "userNameLabel";
    public static final String PASSWORD_LABEL_PROPERTY  = PROPERTY_PREFIX + "passwordLabel";
    public static final String SYSTEM_LABEL_PROPERTY    = PROPERTY_PREFIX + "systemLabel";
    
    private static final int FIELD_COLUMNS = DFLTS.getInt(FIELD_COLUMNS_PROPERTY);

    private static final Icon LOGO_AND_NAME_ICON    = DFLTS.getIcon(LOGO_AND_NAME_ICON_PROPERTY);
    private static final Border LOGO_BORDER         = DFLTS.getBorder(LOGO_BORDER_PROPERTY);

    private static final String NAME_LABEL      = DFLTS.getString(USER_NAME_LABEL_PROPERTY);
    private static final String PASSWORD_LABEL  = DFLTS.getString(PASSWORD_LABEL_PROPERTY);
    private static final String SYSTEM_LABEL    = DFLTS.getString(SYSTEM_LABEL_PROPERTY);

    private static final KeyStroke ENTER_RELEASED = KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0, true);

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private ArrayList labels;
    
    private TextFieldWidget nameFld;
    private JPasswordField pwdFld;
    private JComboBox sysFld;
    private Icon loginIcon;
    
    private Action enterKeyAction;
    
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a login panel containing a name, password, and system field
    @since 2.0
    */    
    public LoginPanel() {
        this(true);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a login panel containing a name and password field, and a system field if the specified
    @param addSystemField True if a system field should be added to the panel
    @since 2.0
    */
    public LoginPanel(final boolean addSystemField) {
    	loginIcon = LOGO_AND_NAME_ICON;
        initializeLoginPanel(addSystemField);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a login panel containing a name and password field, and a system field if the specified
    @param addSystemField True if a system field should be added to the panel
    @since 2.0
    */
    public LoginPanel(final boolean addSystemField, final Icon icon) {
    	loginIcon = icon;
        initializeLoginPanel(addSystemField);
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Adds the specified text (within a LabelWidget) and component to the panel in a horizontal layout, with the LabelWidget
    positioned to the left of the component.
    @param text         The text to be added to the container within a LabelWidget
    @param component    The component to be added to the container
    @since 2.0
    */
    public void addField(final String text, final JComponent component) {
        labels.add(addField((Container)getContent(), text, component));
        alignLabels();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Adds the specified text (within a LabelWidget) and component to the specified container in a horizontal layout, with the
    LabelWidget positioned to the left of the component.
    @param container    The container to which the text's LabelWidget and component will be added
    @param text         The text to be added to the container within a LabelWidget
    @param component    The component to be added to the container
    @return The LabelWidget containing the specified text.  The LoginWindow uses this reference to align the text for all fields.
    @since 2.0
    */
    protected LabelWidget addField(final Container container, final String text, JComponent component) {
        final Box hBox = Box.createHorizontalBox();
        final LabelWidget label = new LabelWidget(text + ' ');
        label.setHorizontalAlignment(LabelWidget.RIGHT);
        hBox.add(label);
        component.setMaximumSize(new Dimension(Short.MAX_VALUE, component.getPreferredSize().height));
        hBox.add(component);
        container.add(hBox);
        // Register enter key on component to click login button
        if (component instanceof JComboBox) {
            component = (JComponent)((JComboBox)component).getEditor().getEditorComponent();
            //component.registerKeyboardAction(enterKeyListener, ENTER_RELEASED, WHEN_FOCUSED);
            
            InputMap inputMap = component.getInputMap();
            inputMap.put(ENTER_RELEASED, "enter");
            
            ActionMap actionMap = component.getActionMap();
            actionMap.put("enter", enterKeyAction);
            
        }
        
        return label;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Makes all labels the same width, which effectively aligns them right-justified.
    @since 2.0
    */
    protected void alignLabels() {
        int maxWth = 0;
        Iterator iterator = labels.iterator();
        while (iterator.hasNext()) {
            maxWth = Math.max(maxWth, ((LabelWidget)iterator.next()).getPreferredSize().width);
        }
        final Dimension size = new Dimension(maxWth, ((LabelWidget)labels.get(0)).getPreferredSize().height);
        iterator = labels.iterator();
        LabelWidget label;
        while (iterator.hasNext()) {
            label = (LabelWidget)iterator.next();
            label.setMinimumSize(size);
            label.setPreferredSize(size);
            label.setMaximumSize(size);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a login button with a default label (as determined by the ToolboxStandards class).
    @return The login button
    @since 2.0
    */
    protected ButtonWidget createAcceptButton() {
        return WidgetFactory.createButton(LOGIN_BUTTON);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The login button
    @since 2.0
    */
    public ButtonWidget getLoginButton() {
        return getAcceptButton();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The password field
    @since 2.0
    */
    public JPasswordField getPasswordField() {
        return pwdFld;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The system field
    @since 2.0
    */
    public JComboBox getSystemField() {
        return sysFld;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The user name field
    @since 2.0
    */
    public TextFieldWidget getUserNameField() {
        return nameFld;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Builds the contents of the LoginPanel, including a company logo; three default fields for the user to enter a user name,
    password, and system; a button to login to the selected/entered system; and a button to cancel the login.
    @since 2.0
    */    
    protected void initializeLoginPanel(final boolean addSystemField) {
        // Add logo containing both icon and company name to panel
        final LabelWidget label = new LabelWidget(this.loginIcon);
        label.setBorder(LOGO_BORDER);
        label.setHorizontalAlignment(LabelWidget.CENTER);
        add(label, BorderLayout.NORTH);
        // Create ActionListener for [Enter] key that selects login button
        enterKeyAction = new AbstractAction() {
            public void actionPerformed(final ActionEvent event) {
                getLoginButton().requestFocus();
                getLoginButton().doClick();
            }
        };
        // Create and add the default fields to the content panel
        labels = new ArrayList();
        final Box vBox = Box.createVerticalBox();
        setContent(vBox);
        nameFld = new TextFieldWidget(FIELD_COLUMNS);
        labels.add(addField(vBox, NAME_LABEL, nameFld));
        pwdFld = new JPasswordField(FIELD_COLUMNS);
        labels.add(addField(vBox, PASSWORD_LABEL, pwdFld));
        if (addSystemField) {
            sysFld = new JComboBox();
            sysFld.setEditable(true);
            labels.add(addField(vBox, SYSTEM_LABEL, sysFld));
        }
        // Make all labels the same width
        alignLabels();
    }
}
