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

package com.metamatrix.console.ui.views.authorization;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.Collections;
import java.util.Properties;

import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.admin.api.AuthorizationAdminAPI;
import com.metamatrix.platform.admin.api.MembershipAdminAPI;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

/**
 * This is the SummaryMain panel.  If the user has priveledges, they can reset the admin username and password
 * and also whether authorization and authentication are enabled.
 */
public class SummaryMain extends BasePanel implements WorkspacePanel, Refreshable {
	
	private static final String SPACE = " "; //$NON-NLS-1$
    private ConnectionInfo connection;
    private CheckBox chkbxEnableAuth;
    private ButtonWidget applyButton;
    private ButtonWidget resetButton;
    private TextFieldWidget usernameTextField;
    private ButtonWidget changePasswordButton;
    private String newPassword;
    
    /**
     * Constructor
     * @param conn the ConnectionInfo object
     */
    public SummaryMain(ConnectionInfo conn) {
        super();
        this.connection = conn;
        init();
    }

    /**
     * Layout the panel and init the values to startup configuration values.
     */
    private void init() {
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        
        // Create the panel and add it.
        JPanel summaryPanel = createSummaryPanel();
        add(summaryPanel);
        layout.setConstraints(summaryPanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.NORTHWEST, GridBagConstraints.HORIZONTAL,
                new Insets(2, 2, 2, 2), 0, 0));
        
        // Initialize the display to startup config values
        refresh(Configuration.STARTUP_ID);
    }


    /**
     * helper method to get the configurationManager for connection
     * @param configID the supplied ConfigurationID
     */
    private ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(this.connection);
    }
    
    /**
     * Creates a Panel containing the summary controls
     * @return the summary panel
     */
    private JPanel createSummaryPanel() {
        JPanel summaryPanel = new JPanel();
        
        GridBagLayout layout = new GridBagLayout();
        summaryPanel.setLayout(layout);
        
        //Check box to enable / disable security 
        this.chkbxEnableAuth = new CheckBox(SPACE+ConsolePlugin.Util.getString("SummaryMain.enableAuthCheckBox.label")); //$NON-NLS-1$
        this.chkbxEnableAuth.setToolTipText(ConsolePlugin.Util.getString("SummaryMain.enableAuthCheckBox.tooltip")); //$NON-NLS-1$
        this.chkbxEnableAuth.addItemListener(new ItemListener() {
        	public void itemStateChanged(ItemEvent ev) {
        		updateWidgetEnabledStates();
        	}
        });
        summaryPanel.add(chkbxEnableAuth);
        layout.setConstraints(chkbxEnableAuth, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        
        //Panel for resetting AdminUser name
        JPanel resetAdminUsernamePanel = createResetAdminUsernamePanel();
        summaryPanel.add(resetAdminUsernamePanel);
        layout.setConstraints(resetAdminUsernamePanel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        
        //Panel for changing Admin Password
        JPanel resetAdminPasswordPanel = createResetAdminPasswordPanel();
        summaryPanel.add(resetAdminPasswordPanel);
        layout.setConstraints(resetAdminPasswordPanel, new GridBagConstraints(0, 2, 1, 1,
                0.0, 0.0, GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        
        //Panel for update and reset buttons
        JPanel buttonPanel = createButtonPanel();
        summaryPanel.add(buttonPanel);
        layout.setConstraints(buttonPanel, new GridBagConstraints(0, 3, 1, 1,
                0.0, 0.0, GridBagConstraints.NORTHWEST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        
        return summaryPanel;
    }
    
    /**
     * Creates the button panel which contains 'Apply' and 'Reset' buttons.
     * @return the summary panel
     */
    private JPanel createButtonPanel() {
        JPanel buttonPanel = new JPanel();
        
        GridBagLayout layout = new GridBagLayout();
        buttonPanel.setLayout(layout);
        
        // ---------------------------------------------------------
        // Apply Button to set new values on the NextStartup Config
        // ---------------------------------------------------------
        applyButton = new ButtonWidget(ConsolePlugin.Util.getString("SummaryMain.applyButton.text")); //$NON-NLS-1$
        applyButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                applyButtonPressed();
            }
        });
        buttonPanel.add(applyButton);
        layout.setConstraints(applyButton, new GridBagConstraints(0, 0, 1, 1,
                0.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        
        // -----------------------------------------------------------
        // Reset Button to reset values back to Startup Config values
        // -----------------------------------------------------------
        resetButton = new ButtonWidget(ConsolePlugin.Util.getString("SummaryMain.resetButton.text")); //$NON-NLS-1$
        resetButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                refresh(Configuration.NEXT_STARTUP_ID);
            }
        });
        buttonPanel.add(resetButton);
        layout.setConstraints(resetButton, new GridBagConstraints(1, 0, 1, 1,
                0.8, 1.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        
        return buttonPanel;
    }

    /**
    Creates a Panel containing the reset admin username controls
    @return the reset admin username panel
    */
    private JPanel createResetAdminUsernamePanel() {
    	JPanel resetPanel = new JPanel();
    	
    	GridBagLayout layout = new GridBagLayout();
    	resetPanel.setLayout(layout);
    	
    	// ------------------------
    	// MMx Admin Account label
    	// ------------------------
        LabelWidget setUsernameLabel = new LabelWidget(ConsolePlugin.Util.getString("SummaryMain.username.label")); //$NON-NLS-1$
        resetPanel.add(setUsernameLabel);
        
        layout.setConstraints(setUsernameLabel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 1.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        
    	// -----------------------------
    	// MMx Admin Username textField
    	// -----------------------------
        this.usernameTextField = new TextFieldWidget(30);
        resetPanel.add(usernameTextField);
        layout.setConstraints(usernameTextField, new GridBagConstraints(1, 0, 1, 1,
                0.8, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        this.usernameTextField.getDocument().addDocumentListener(new DocumentListener() {
        	public void changedUpdate(DocumentEvent ev) {
        		updateWidgetEnabledStates();
        	}
        	public void insertUpdate(DocumentEvent ev) {
        		updateWidgetEnabledStates();
        	}
        	public void removeUpdate(DocumentEvent ev) {
        		updateWidgetEnabledStates();
        	}
        });
    	
        return resetPanel;
    }
    
    /**
    Creates a Panel containing the reset admin password controls
    @return the reset password panel
    */
    private JPanel createResetAdminPasswordPanel() {
    	JPanel resetPanel = new JPanel();
    	
    	GridBagLayout layout = new GridBagLayout();
    	resetPanel.setLayout(layout);
    	
    	// ------------------------
    	// MMx Admin Password label
    	// ------------------------
        LabelWidget setPasswordLabel = new LabelWidget(ConsolePlugin.Util.getString("SummaryMain.password.label")); //$NON-NLS-1$
        resetPanel.add(setPasswordLabel);
        
        layout.setConstraints(setPasswordLabel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 1.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        
    	// ---------------------------------
    	// MMx Admin Password change button
    	// ---------------------------------
        String text = ConsolePlugin.Util.getString("SummaryMain.changePassButton.text"); //$NON-NLS-1$
        this.changePasswordButton = new ButtonWidget(text); 
        this.changePasswordButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                setNewPassword();
                updateWidgetEnabledStates();
            }
        });
        resetPanel.add(this.changePasswordButton);
        layout.setConstraints(this.changePasswordButton, new GridBagConstraints(1, 0, 1, 1,
                0.8, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(2, 2, 2, 2), 0, 0));
        
        return resetPanel;
    }
    
    /**
     * Method to display a password confirmation panel and set this classes passwordfield.
     */
    private void setNewPassword() {
    	PasswordConfirmPanel pcp = new PasswordConfirmPanel();
    	DialogWindow.show(this, ConsolePlugin.Util.getString("SummaryMain.resetPasswordDialog.title"), pcp); //$NON-NLS-1$

    	// Accept Button Pressed
    	if(pcp.getSelectedButton() == pcp.getAcceptButton()) {
    		// Check that password entries are not empty
    		final char[] newPass = pcp.getNewPassword();
    		final String newPassStr = new String(newPass);
    		final char[] newPassConfirm = pcp.getNewPassword2();
    		final String newPassConfirmStr = new String(newPassConfirm);
    		this.newPassword = null;
    		
    		// Check Password Entries for empty entry
    		if(newPassStr==null || newPassStr.length()==0) {
    			String title = ConsolePlugin.Util.getString("SummaryMain.invalidPasswordDialog.title"); //$NON-NLS-1$
    			String msg = ConsolePlugin.Util.getString("SummaryMain.invalidPasswordDialog.invalidNewPass.msg"); //$NON-NLS-1$
                StaticUtilities.displayModalDialogWithOK(title, msg);
                return;
    		} else if(newPassConfirmStr==null || newPassConfirmStr.length()==0) {
    			String title = ConsolePlugin.Util.getString("SummaryMain.invalidPasswordDialog.title"); //$NON-NLS-1$
    			String msg = ConsolePlugin.Util.getString("SummaryMain.invalidPasswordDialog.invalidNewPassConf.msg"); //$NON-NLS-1$
                StaticUtilities.displayModalDialogWithOK(title, msg);
                return;
      		}
    		
    		
    		// Check newPass and confirmed newPass are the same
    		if(!newPassConfirmStr.equals(newPassStr)) {
    			String title = ConsolePlugin.Util.getString("SummaryMain.passNoMatchDialog.title"); //$NON-NLS-1$
    			String msg = ConsolePlugin.Util.getString("SummaryMain.passNoMatchDialog.msg"); //$NON-NLS-1$
                StaticUtilities.displayModalDialogWithOK(title, msg);
    			return;
    		}
    		
            // If we make it here, checks have passed, set the class field for new password.
    		this.newPassword = newPassConfirmStr;
    	
    	// Cancel Button Pressed
    	} else {
			String title = ConsolePlugin.Util.getString("SummaryMain.cancelResetDialog.title"); //$NON-NLS-1$
			String msg = ConsolePlugin.Util.getString("SummaryMain.cancelResetDialog.msg"); //$NON-NLS-1$
            StaticUtilities.displayModalDialogWithOK(title, msg);
            this.newPassword = null;
    	}
    	return;
    }
    
    /**
     * Determine if there is a pending change to the password.
     * @return 'true' if there is a pending password change, 'false' if not.
     */
    private boolean hasPasswordUpdate() {
    	return (this.newPassword!=null);
    }
    
    /**
     * Determine if there is a pending change to the username.
     * @return 'true' if there is a pending username change, 'false' if not.
     */
    private boolean hasUsernameUpdate() {
    	// Get username from nextStartup config
        final String nextStartUser = getUsername(Configuration.NEXT_STARTUP_ID);
        
        final String enteredUser = this.usernameTextField.getText();
        if(enteredUser!=null && enteredUser.trim().length()>0 && !enteredUser.equals(nextStartUser)) return true;
        return false;
    }
    
    /**
     * Determine if the entered username is empty
     * @return 'true' if the username is empty, 'false' if not.
     */
    private boolean enteredUserIsEmpty() {
    	final String enteredUser = this.usernameTextField.getText();
    	if(enteredUser==null || enteredUser.trim().length()==0) return true;
    	return false;
    }
    
    /**
     * Determine if the entered username is valid.  Valid means that the entered username is
     * not empty and consists only of alpha-numeric characters.
     * @return 'true' if the username is valid, 'false' if not.
     */
    private boolean enteredUserIsValid() {
    	final String enteredUser = this.usernameTextField.getText();
    	if(enteredUser==null || enteredUser.trim().length()==0) return false;
    	char[] userChars = enteredUser.trim().toCharArray();
    	for(int i=0; i<userChars.length; i++) {
    		if(!StringUtil.isLetterOrDigit(userChars[i])) return false;
    	}
    	return true;
    }
    
    /**
     * Determine if there is a pending change to the security enablement setting.
     * @return 'true' if there is a pending enablement change, 'false' if not.
     */
    private boolean hasEnabledUpdate() {
    	final boolean nextStartEnabledState = getEnabledState(Configuration.NEXT_STARTUP_ID);
    	
        if(nextStartEnabledState != this.chkbxEnableAuth.isSelected()) {
        	return true;
        }
        return false;
    }
    
    /**
     * Get the username value from the supplied configuration
     * @param configID the supplied ConfigurationID
     * @return the username value
     */
    private String getUsername(ConfigurationID configID) {
    	// Get username from config
        final Configuration config = getConfigurationManager().getConfig(configID);
        final ServiceComponentDefn serviceDefn = config.getServiceComponentDefn(ResourceNames.MEMBERSHIP_SERVICE);
        final String user = serviceDefn.getProperty(ConfigurationPropertyNames.MEMBERSHIP_ADMIN_USERNAME);
        
        return user;
    }
    
    /**
     * Get the enabled state value from the supplied configuration
     * @param configID the supplied ConfigurationID
     * @return the enabled state value
     */
    private boolean getEnabledState(ConfigurationID configID) {
    	final ConfigurationModelContainer configModel = getConfigurationManager().getConfigModel(configID);
        final Configuration config = getConfigurationManager().getConfig(configID);
        final ServiceComponentDefn serviceDefn = config.getServiceComponentDefn(ResourceNames.MEMBERSHIP_SERVICE);
        
        // Get default enabled state for MembershipService
        Properties props = configModel.getDefaultPropertyValues(serviceDefn.getComponentTypeID());
        final String enabledDefault = props.getProperty(ConfigurationPropertyNames.MEMBERSHIP_SECURITY_ENABLED);
        
        // Enabled state
        boolean enabledState = false;
        final String enabled = serviceDefn.getProperty(ConfigurationPropertyNames.MEMBERSHIP_SECURITY_ENABLED);
        if(enabled != null) {            
        	enabledState = new Boolean(enabled).booleanValue();
        } else if(enabledDefault != null) {
        	enabledState =  new Boolean(enabledDefault).booleanValue();
        }
        
        return enabledState;
    }
    
    /**
     * Save any pending changes to the NextStartup configuration and refresh the panel
     */
    private void applyButtonPressed() {
    	// If the entered username is not alpha-numeric, display error dialog and quit
    	if(!enteredUserIsValid()) {
			String title = ConsolePlugin.Util.getString("SummaryMain.invalidUserDialog.title"); //$NON-NLS-1$
			String msg = ConsolePlugin.Util.getString("SummaryMain.invalidUserDialog.msg"); //$NON-NLS-1$
            StaticUtilities.displayModalDialogWithOK(title,msg);
    		return;
    	}
		// Force User to re-enter the current users password.  Return on failure. 
    	// Ensures that they have priviledge to change security settings.
    	String dialogText = ConsolePlugin.Util.getString("SummaryMain.confirmPasswordDialog.msg"); //$NON-NLS-1$
    	CurrentPasswordConfirmPanel pcp = new CurrentPasswordConfirmPanel(dialogText);
    	DialogWindow.show(this, ConsolePlugin.Util.getString("SummaryMain.confirmPasswordDialog.title"), pcp); //$NON-NLS-1$

    	// Accept Button Pressed
    	if(pcp.getSelectedButton() == pcp.getAcceptButton()) {
    		// Check that password entries are not empty
    		final char[] currentPass = pcp.getCurrentPassword();
    		final String currentPassStr = new String(currentPass);
    		if(currentPassStr==null || currentPassStr.length()==0) {
    			String title = ConsolePlugin.Util.getString("SummaryMain.invalidPasswordDialog.title"); //$NON-NLS-1$
    			String msg = ConsolePlugin.Util.getString("SummaryMain.invalidPasswordDialog.invalidCurrentPass.msg"); //$NON-NLS-1$
                StaticUtilities.displayModalDialogWithOK(title, msg);
                return;
    		} 
    		
			final MembershipAdminAPI membershipAPI = ModelManager.getMembershipAPI(this.connection );
	        try {
	            if(! membershipAPI.authenticateUser(this.connection.getUser(), new Credentials(currentPass), null, null) ) {
	    			String title = ConsolePlugin.Util.getString("SummaryMain.authErrorDialog.title"); //$NON-NLS-1$
	    			String msg = ConsolePlugin.Util.getString("SummaryMain.authErrorDialog.msg"); //$NON-NLS-1$
	                StaticUtilities.displayModalDialogWithOK(title, msg);
	                return;
	            }
	        } catch (Exception err) {
				String title = ConsolePlugin.Util.getString("SummaryMain.authErrorDialog.title"); //$NON-NLS-1$
				String msg = ConsolePlugin.Util.getString("SummaryMain.authErrorMessage.text"); //$NON-NLS-1$
	            ExceptionUtility.showMessage(title, msg, err);
	            return;
	        }
	    // Cancel Button Pressed
    	} else {
			String title = ConsolePlugin.Util.getString("SummaryMain.cancelApplyDialog.title"); //$NON-NLS-1$
			String msg = ConsolePlugin.Util.getString("SummaryMain.cancelApplyDialog.msg"); //$NON-NLS-1$
            StaticUtilities.displayModalDialogWithOK(title, msg);
            return;
    	}
        
    	// Current User password was verified, make the changes
    	updateNextStartupConfigWithPendingChanges(); 
        refresh(Configuration.NEXT_STARTUP_ID);
    }
            
    /**
     * Update the NextStartup Configuration with pending widget changes
     */
    private void updateNextStartupConfigWithPendingChanges() {
        //Security updates are made to the next startup config
        Configuration config = getConfigurationManager().getConfig(Configuration.NEXT_STARTUP_ID);
        ServiceComponentDefn serviceDefn = config.getServiceComponentDefn(ResourceNames.MEMBERSHIP_SERVICE);
        Properties nextStartupProperties = serviceDefn.getProperties();
        
        // Clone properties object to hold changes
    	Properties props = (Properties)nextStartupProperties.clone();
    	
        // Update the username
        final String user = this.usernameTextField.getText();
        if(user != null && user.trim().length() > 0) {
            props.setProperty(ConfigurationPropertyNames.MEMBERSHIP_ADMIN_USERNAME, user); 
        }
        
        // Update the password
        if(this.newPassword != null && this.newPassword.trim().length() > 0){
			props.setProperty(ConfigurationPropertyNames.MEMBERSHIP_ADMIN_PASSWORD, this.newPassword); 
        }
        
        // Update the enabled flag
        props.setProperty(ConfigurationPropertyNames.MEMBERSHIP_SECURITY_ENABLED,new Boolean(this.chkbxEnableAuth.isSelected() ).toString() );
    	
        // Set properties on the NextStartup Configuration
		try {
            getConfigurationManager().modifyService(serviceDefn,props);
			String title = ConsolePlugin.Util.getString("SummaryMain.configModifiedDialog.title"); //$NON-NLS-1$
			String msg = ConsolePlugin.Util.getString("SummaryMain.configModifiedDialog.msg"); //$NON-NLS-1$
            StaticUtilities.displayModalDialogWithOK(title,msg);
        } catch (ExternalException err) {
            ExceptionUtility.showMessage(ConsolePlugin.Util.getString("SummaryMain.updateConfigError.text"), err); //$NON-NLS-1$
        }
    }
    
    /**
     * Refresh the display. Default is to refresh from NextStartup Configuration
     */
    public void refresh( ) {
    	refresh(Configuration.NEXT_STARTUP_ID);
    }

    /**
     * Refresh the display with values from the supplied configuration ID
     * @param configID the supplied ConfigurationID
     */
    public void refresh(ConfigurationID configID) {
    	// Update Username textfield
    	String nsUsername = getUsername(configID);
    	this.usernameTextField.setText(nsUsername);
    	
    	// Update Enabled checkbox
    	boolean nsEnabled = getEnabledState(configID);
    	this.chkbxEnableAuth.setSelected(new Boolean(nsEnabled).booleanValue());
    	
    	// Reset password modification value
    	this.newPassword = null;
    	   
    	// Update enabled states of the widgets
    	updateWidgetEnabledStates();
    }

    /**
     * Determine if current user is superUser.
     * @return 'true' if user is a superUser, 'false' if not.
     */
    private boolean isSuperUser() {
        boolean isSuperUser = false;
        //Update the enabled states based on user being the superUser
        final AuthorizationAdminAPI authAPI = ModelManager.getAuthorizationAPI(connection);
        try {
            isSuperUser = authAPI.isSuperUser(this.connection.getUser() );
        }catch(final Exception e) {
            ExceptionUtility.showMessage(ConsolePlugin.Util.getString("SummaryMain.getSettingsError.text"), e); //$NON-NLS-1$
        }
        return isSuperUser;
    }
    
    /**
     * Update the enable states of the panel widgets.
     */
    private void updateWidgetEnabledStates() {
        boolean isSuperUser = isSuperUser();
        
        // Username and password disabled for non-super user
        chkbxEnableAuth.setEnabled(isSuperUser);
        usernameTextField.setEnabled(isSuperUser);
        changePasswordButton.setEnabled(isSuperUser);
        
        // Apply and reset button
        if(!isSuperUser) {
        	applyButton.setEnabled(isSuperUser);
            resetButton.setEnabled(isSuperUser);
        } else {
        	// If entered username is empty, disable apply but enable reset
        	if(this.enteredUserIsEmpty()) {
        		this.applyButton.setEnabled(false);
                this.resetButton.setEnabled(true);
        	// Determine apply Button State - enable if any of the properties are different than the next startup
        	// Username check is first - also disables if empty username is entered.
        	} else if(hasUsernameUpdate() || hasPasswordUpdate() || hasEnabledUpdate()) {
        		this.applyButton.setEnabled(true);
                this.resetButton.setEnabled(true);
        	} else {
        		this.applyButton.setEnabled(false);
                this.resetButton.setEnabled(false);
        	}
        }
    }
    
    /**
     * Get SecuritySummary page title
     * @return the page title
     */
    public String getTitle() {
        return ConsolePlugin.Util.getString("SummaryMain.pageTitle.text"); //$NON-NLS-1$
    }

    /**
     * Get connection
     * @return the ConnectionInfo
     */
    public ConnectionInfo getConnection() {
        return connection;
    }
    
    public java.util.List /* <Action> */resume() {
        return Collections.EMPTY_LIST;
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    }
    
    /**
     * Dialog confirmation panel for re-entering the current user password.
     */
    private class CurrentPasswordConfirmPanel extends DialogPanel {
        private JPasswordField currentPasswordField;
        public CurrentPasswordConfirmPanel(String messageText) {
            super();
            init(messageText);
        }
        
        protected char[] getCurrentPassword() {
            if(currentPasswordField != null) {
                return currentPasswordField.getPassword();
            }
            
            return new char[] {};
        }
        
        private void init(String messageText) {
            JPanel confirmPasswordPanel = new JPanel();
            this.setContent(confirmPasswordPanel);
            
            GridBagLayout layout = new GridBagLayout();
            confirmPasswordPanel.setLayout(layout);
            
            //User must provide existing admin pw to authenticate that they have
            //privs to make security level changes.
            LabelWidget messageLabel = new LabelWidget(messageText);
            confirmPasswordPanel.add(messageLabel);
            
            layout.setConstraints(messageLabel, new GridBagConstraints(0, 0, 2, 1,
                    0.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                    new Insets(2, 2, 2, 2), 0, 0));
            
            // --------------------------------
            // Confirm existing password label
            // --------------------------------
            String text = ConsolePlugin.Util.getString("SummaryMain.currentPassLabel.text"); //$NON-NLS-1$
            LabelWidget currentAdminPasswordLabel = new LabelWidget(text); 
            confirmPasswordPanel.add(currentAdminPasswordLabel);
            
            layout.setConstraints(currentAdminPasswordLabel, new GridBagConstraints(0, 1, 1, 1,
                    0.0, 1.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                    new Insets(2, 2, 2, 2), 0, 0));
            
            // ----------------------------------
            // Existing Admin Password textField
            // ----------------------------------
            currentPasswordField = new JPasswordField(30);
            confirmPasswordPanel.add(currentPasswordField);
            layout.setConstraints(currentPasswordField, new GridBagConstraints(1, 1, 1, 1,
                    0.8, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                    new Insets(2, 2, 2, 2), 0, 0));

        }
    }

    /**
     * Dialog confirmation panel for entering new admin password
     */
    private class PasswordConfirmPanel extends DialogPanel {
        private JPasswordField newPasswordField;
        private JPasswordField newPasswordField2;
        public PasswordConfirmPanel() {
            super();
            init();
        }
        
        protected char[] getNewPassword() {
            if(newPasswordField != null) {
                return newPasswordField.getPassword();
            }
            
            return new char[] {};
        }
        
        protected char[] getNewPassword2() {
            if(newPasswordField2 != null) {
                return newPasswordField2.getPassword();
            }
            
            return new char[] {};
        }
        
        private void init() {
            JPanel confirmPasswordPanel = new JPanel();
            this.setContent(confirmPasswordPanel);
            
            GridBagLayout layout = new GridBagLayout();
            confirmPasswordPanel.setLayout(layout);
            
            // ------------------------
            // new password label
            // ------------------------
            String text = ConsolePlugin.Util.getString("SummaryMain.newPassLabel.text"); //$NON-NLS-1$
            LabelWidget newPasswordLabel = new LabelWidget(text); 
            confirmPasswordPanel.add(newPasswordLabel);
            
            layout.setConstraints(newPasswordLabel, new GridBagConstraints(0, 0, 1, 1,
                    0.0, 1.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                    new Insets(2, 2, 2, 2), 0, 0));
            
            // -----------------------------
            // MMx Admin Password textField
            // -----------------------------
            newPasswordField = new JPasswordField(30);
            confirmPasswordPanel.add(newPasswordField);
            layout.setConstraints(newPasswordField, new GridBagConstraints(1, 0, 1, 1,
                    0.8, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                    new Insets(2, 2, 2, 2), 0, 0));

            // ---------------------------
            // retype new password label
            // ---------------------------
            text = ConsolePlugin.Util.getString("SummaryMain.newPassConfLabel.text"); //$NON-NLS-1$
            LabelWidget newPasswordLabel2 = new LabelWidget(text); 
            confirmPasswordPanel.add(newPasswordLabel2);
            
            layout.setConstraints(newPasswordLabel2, new GridBagConstraints(0, 1, 1, 1,
                    0.0, 1.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                    new Insets(2, 2, 2, 2), 0, 0));
            
            // ---------------------------------
            // retype Admin Password textField
            // ---------------------------------
            newPasswordField2 = new JPasswordField(30);
            confirmPasswordPanel.add(newPasswordField2);
            layout.setConstraints(newPasswordField2, new GridBagConstraints(1, 1, 1, 1,
                    0.8, 1.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                    new Insets(2, 2, 2, 2), 0, 0));
        }
    }
            
}// end SummaryMain
