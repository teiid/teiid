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
package com.metamatrix.toolbox.ui.widget;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;

import com.metamatrix.common.util.crypto.Encryptor;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.event.WidgetActionEvent;

/**
 * @since 2.1
 * @version 2.1
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class PasswordButton extends ButtonWidget {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

	private static final String PREFIX = "PasswordButton.";

    public static final String DEFAULT_DIALOG_TITLE_PROPERTY	= PREFIX + "dialogTitle";
    public static final String DEFAULT_TEXT_PROPERTY			= PREFIX + "text";
    public static final String FIELD_COLUMNS_PROPERTY			= PREFIX + "fieldColumns";
    public static final String FIELD_NAME_PROPERTY				= PREFIX + "fieldName";
    
    public static final String	DEFAULT_DIALOG_TITLE	= UIDefaults.getInstance().getString(DEFAULT_DIALOG_TITLE_PROPERTY);
    public static final String	DEFAULT_TEXT			= UIDefaults.getInstance().getString(DEFAULT_TEXT_PROPERTY);
    public static final int 	FIELD_COLUMNS			= UIDefaults.getInstance().getInt(FIELD_COLUMNS_PROPERTY);
    public static final String	FIELD_NAME				= UIDefaults.getInstance().getString(FIELD_NAME_PROPERTY);

    private static final String LOG_CONTEXT = "PROPERTY";
    
	//############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

	private char[] pwd;    
    private String dlgTitle;
    private Encryptor encryptor; //not used
        
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
        
    
    
    /**
     * @since 2.1
     */
    public PasswordButton(final char[] password, Encryptor encryptor) {
        super(DEFAULT_TEXT, null);
        this.encryptor = encryptor;
        
        constructPasswordButton(password);
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    /**
     * Provided to be overridden by subclasses that need to add additional constraints to new passwords entered in the "Change
     * Password" dialog.  If a new password fails to meet any of these constraints, the specified event should be
     * {@link WidgetActionEvent#destroy() destroyed}.  Does nothing by default.
     * @param panel The dialog panel displayed containing the fields necessary to change the current password
     * @param event The WidgetActionEvent received when the user selects the dialog's accept button
     * @since 2.1
     */
    protected void accept(final DialogPanel panel, final WidgetActionEvent event) {
    }

    /**
     * @since 2.1
     */
    protected JPasswordField addField(final JPanel labelPanel, final JPanel fieldPanel, final String text) {
        labelPanel.add(new JLabel(text + ": ", JLabel.RIGHT));
        final JPasswordField fld = new JPasswordField(FIELD_COLUMNS);
        fieldPanel.add(fld);
        return fld;
    }

    /**
     * @since 2.1
     */    
    protected void constructPasswordButton(final char[] password) {
        Assertion.isNotNull(password, "Password");
        pwd = password;
        // Add listener to button to show dialog window containing change password panel
        addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                // Build dialog panel to change password
                final JPanel panel = new JPanel(new BorderLayout());
                final JPanel labelPanel = new JPanel(new GridLayout(0, 1));
                panel.add(labelPanel, BorderLayout.WEST);
                final JPanel fldPanel = new JPanel(new GridLayout(0, 1));
                panel.add(fldPanel, BorderLayout.CENTER);
                final JPasswordField fld = addField(labelPanel, fldPanel, FIELD_NAME);
                final DialogPanel dlgPanel = new DialogPanel() {
                    protected void accept(final WidgetActionEvent event) {
                        PasswordButton.this.accept(this, event);
                        if (!event.isDestroyed()) {
                            pwd = fld.getPassword();
                            PasswordButton.this.fireStateChanged();
                        }
                    }
                };
                final ActionListener listener = new ActionListener() {
                    public void actionPerformed(final ActionEvent event) {
                        dlgPanel.getAcceptButton().doClick();
                    }
                };
                fld.addActionListener(listener);
                dlgPanel.setContent(panel);
                DialogWindow.show(PasswordButton.this, dlgTitle, dlgPanel);
            }
        });
        // Set default dialog title
        setDialogTitle(DEFAULT_DIALOG_TITLE);
    }
    
    /**
     * @return
     * @since 2.1
     */
    public String getDialogTitle() {
        return dlgTitle;
    }

    /**
     * @since 2.1
     */
    public char[] getPassword() {
        final char[] pwd = new char[this.pwd.length];
        System.arraycopy(this.pwd, 0, pwd, 0, pwd.length);
        return pwd;
    }
    
    /**
     * @param title
     * @since 2.1
     */
    public void setDialogTitle(final String title) {
        dlgTitle = title;
    }    

}
