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
package com.metamatrix.console.ui.dialog;

import java.awt.AWTEvent;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;

import javax.swing.AbstractButton;
import javax.swing.ImageIcon;
import javax.swing.JCheckBox;
import javax.swing.JFrame;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.toolbox.ui.IconConstants;
import com.metamatrix.toolbox.ui.widget.ButtonConstants;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.WidgetFactory;
import com.metamatrix.toolbox.ui.widget.util.IconFactory;

/**
This window should be used by all MetaMatrix products as the means for a user to login to a system and begin using the product.
The calling thread will be blocked while the window is displayed.
@since 2.0
@version 2.0
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class ConsoleLoginWindow extends JFrame
implements ButtonConstants, IconConstants {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    // Icon for login dialog
    private static final ImageIcon LOGO_ICON =
        IconFactory.getIconForImageFile("logo.gif"); //$NON-NLS-1$
    
    private static final long TIMEOUT = 500;   // ms

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates and displays a ConsoleLoginWindow centered on the screen with the specified title, containing a default LoginPanel.
    @since 2.0
    */
    public static void show(final String title, boolean isFirstLogin) {
        show(title, null, isFirstLogin);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates and displays a ConsoleLoginWindow centered on the screen with the specified title, containing the specified LoginPanel.
    @since 2.0
    */
    public static void show(final String title, final ConsoleLoginPanel panel,
    		boolean isFirstLogin) {
        final ConsoleLoginWindow wdw = new ConsoleLoginWindow(title, panel,
        		(!isFirstLogin), isFirstLogin);
        final Dimension screen = Toolkit.getDefaultToolkit().getScreenSize();
        final Dimension wdwSize = wdw.getPreferredSize();
        wdw.setLocation((screen.width - wdwSize.width) / 2, (screen.height - wdwSize.height) / 2);
        wdw.setVisible(true);
    }
     
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
    Creates a ConsoleLoginWindow containing the specified ConsoleLoginPanel, and with the specified title appended to the 
    specified prefix.
    @param title The text to be appended to the window's title
    @param panel The LoginPanel that the window will contain
    @since 2.0
    */
    public ConsoleLoginWindow(final String title, final ConsoleLoginPanel panel,
    		boolean showSelectConnectionCheckBox,
    		boolean insertDefaultUserName) {
        super(ConsolePlugin.Util.getString("ConsoleLoginDialog.titlePrefix", title)); //$NON-NLS-1$
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
    protected void exit(final AWTEvent event) {
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
    Builds the contents of the ConsoleLoginWindow, including a company logo; three default fields for the user to enter a user name,
    password, and system; and buttons to login or exit the selected/entered system.
    @since 2.0
    */
    protected void initializeLoginWindow(boolean showSelectConnectionCheckBox,
    		boolean insertDefaultUserName) {
		// Create a default ConsoleLoginPanel if one was not passed in constructor
        if (panel == null) {
            panel = new ConsoleLoginPanel(insertDefaultUserName);
        }
        // Put the company logo icon in the window's title bar
        setIconImage((LOGO_ICON).getImage());
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
        // Add listeners to login and exit buttons
        final ButtonWidget loginButton = panel.getAcceptButton();
        if (loginButton != null) {
            loginButton.addActionListener(new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    login(event);
                }
            });
        }
        displayedExitButton = panel.getCancelButton();
        if (displayedExitButton != null) {
            displayedExitButton.addActionListener(new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    exit(event);
                }
            });
        }
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
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Called when the user selects the login button.  Simply disposes the window by default.
    @param event The original event
    @since 2.0
    */
    protected void login(final ActionEvent event) {
        DialogWindow.disposeConditionally(this, event);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to call the cancel() method in the event that the user cancels the window via its close button (the 'X' icon) or
    the 'Close' option in the system menu.
    @param event The window event to be processed
    @since 2.0
    */
    protected void processWindowEvent(final WindowEvent event) {
        super.processWindowEvent(event);
        if (event.getID() == WindowEvent.WINDOW_CLOSING) {
            exit(event);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to notify show method to stop blocking.
    @since 2.0
    */
    public synchronized void hide() {
        super.hide();
        notifyAll();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to block until the window is closed.
    @since 2.0
    */
    public synchronized void show() {
        super.show();
        final EventQueue q = Toolkit.getDefaultToolkit().getSystemEventQueue();
        q.push(new EventQueue());
        while (isVisible()) {
            try {
                wait(TIMEOUT);
            } catch (final InterruptedException ignored) {
            }
        }
    }
    
    public boolean selectNewConnection() {
    	return (selectNewConnectionCB.isVisible() &&
    			selectNewConnectionCB.isSelected());
    }
}
