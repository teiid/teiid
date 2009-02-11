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
package com.metamatrix.console.ui.dialog;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.AbstractButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.toolbox.ui.IconConstants;
import com.metamatrix.toolbox.ui.widget.ButtonConstants;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.WidgetFactory;

public class ConsoleLoginDialog extends JDialog
implements ButtonConstants, IconConstants {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

//    private static final Icon LOGO_ICON = UIDefaults.getInstance().getIcon(LOGO_ICON_PROPERTY);
    
//    private static final long TIMEOUT = 5000;   // ms

	//############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private ConsoleLoginPanel panel;
	private AbstractButton displayedExitButton;
	private boolean exitingBlocked = false;
	private JCheckBox selectNewConnectionCB = new CheckBox(
			"Change display to select the new connection"); //$NON-NLS-1$
          
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a ConsoleLoginDialog containing the specified ConsoleLoginPanel, and with the specified title appended to the 
    specified prefix
    @param title The text to be appended to the window's title
    @param panel The LoginPanel that the window will contain
    @since 2.0
    */
    public ConsoleLoginDialog(final String title, final ConsoleLoginPanel panel,
    		boolean showSelectConnectionCheckBox,
    		boolean insertDefaultUserName) {
        super(ConsoleMainFrame.getInstance(), 
        		ConsolePlugin.Util.getString("ConsoleLoginDialog.titlePrefix", title), //$NON-NLS-1$
        		true);
        this.panel = panel;
        initializeLoginWindow(showSelectConnectionCheckBox,
        		insertDefaultUserName);
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The contained DialogPanel
    @since 2.0
    */
    public synchronized ConsoleLoginPanel getLoginPanel() {
        return panel;
    }

	public synchronized AbstractButton getDisplayedExitButton() {
		return displayedExitButton;
	}
	
	public void setExitingBlocked(boolean flag) {
		exitingBlocked = flag;
	}
	
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Called when the user selects the exit button.  Exits the applicatio by default.
    @param event The original event
    @since 2.0
    */
    public void exit() {
    	if (exitingBlocked) {
    		this.dispose();
    	} else {
    		System.exit(0);
    	}
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The exit button
    @since 2.0
    */
    public ButtonWidget getExitButton() {
        return panel.getCancelButton();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The login button
    @since 2.0
    */
    public ButtonWidget getLoginButton() {
        return panel.getLoginButton();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Builds the contents of the ConsoleLoginDialog, including a company logo; three default fields for the user to enter a user name,
    password, and system; and buttons to login or exit the selected/entered system.
    @since 2.0
    */
    protected void initializeLoginWindow(boolean showSelectConnectionCheckBox,
    		boolean insertDefaultUserName) {
		// Create a default ConsoleLoginPanel if one was not passed in constructor
        if (panel == null) {
            panel = new ConsoleLoginPanel(insertDefaultUserName);
        }
//        // Put the company logo icon in the window's title bar
//        setIconImage(((ImageIcon)LOGO_ICON).getImage());
        // Add ConsoleLoginPanel to window
        if (showSelectConnectionCheckBox) {
        	GridBagLayout layout = new GridBagLayout();
        	getContentPane().setLayout(layout);
        	getContentPane().add(panel);
        	getContentPane().add(selectNewConnectionCB);
        	selectNewConnectionCB.setSelected(true);
        	layout.setConstraints(panel, new GridBagConstraints(0, 0, 1, 1,
        			1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
        			new Insets(0, 0, 0, 0), 0, 0));
        	layout.setConstraints(selectNewConnectionCB, new GridBagConstraints(
        			0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
        			GridBagConstraints.NONE, new Insets(4, 4, 4, 4), 0, 0));
        } else {
        	getContentPane().add(panel);
        }
        final ButtonWidget loginButton = panel.getAcceptButton();
        displayedExitButton = panel.getCancelButton();
        // Change the cancel button to an exit button
        final ButtonWidget button = WidgetFactory.createButton(EXIT_BUTTON);
        displayedExitButton.setText(button.getText());
        displayedExitButton.setIcon(button.getIcon());
        displayedExitButton.setMnemonic(button.getMnemonic());
        // Set window non-resizable to prevent odd-looking login windows
        setResizable(false);
        // Set the login button as the window's default button
        getRootPane().setDefaultButton(loginButton);
        // Set the window's size to just accommodate its fields
        pack();
    }
    
    public boolean selectNewConnection() {
    	return (selectNewConnectionCB.isVisible() &&
    			selectNewConnectionCB.isSelected());
    }
}
