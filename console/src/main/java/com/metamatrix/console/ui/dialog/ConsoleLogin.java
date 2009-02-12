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

package com.metamatrix.console.ui.dialog;

// General
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.AbstractButton;

import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LoginPanel;


/**
*
*/
public class ConsoleLogin {
    public final static String APPLICATION_NAME = ConsolePlugin.Util.getString("ConsoleLogin.applicationName"); //$NON-NLS-1$
    
    public final static  int LOGON_SUCCEEDED = 1;
    public final static  int CANCELLED_LOGON = 2;

    /**
    * These are the two main inner components ConsoleLoginDialog and ConsoleLoginPanel
    */
    private ConsoleLoginDialog dlg;
    private ConsoleLoginWindow wdw;
    private ConsoleLoginPanel panel;

    private ConnectionInfo connectionInfo = null;
    private int result = -1;
    private boolean isFirstLogin;
    private ConnectionInfo[] existingConnections;
    private boolean existingConnectionURLAndUserEntered = false;
    private java.util.List urls;
    private String initialPassword;

    public ConsoleLogin(java.util.List urls, boolean isFirstLogin,
            ConnectionInfo[] existingConnections, String password) {
        super();
        this.urls = urls;
        this.isFirstLogin = isFirstLogin;
        this.existingConnections = existingConnections;
        this.initialPassword = password;
        if (this.existingConnections == null) {
            this.existingConnections = new ConnectionInfo[0];
        }
        //init(urls);
    }
    
    public ConsoleLogin(java.util.List urls, boolean isFirstLogin,
            ConnectionInfo[] existingConnections) {
        this(urls, isFirstLogin, existingConnections, null);
    }
    
    /** All initialization happens in this method
    * creates a new instance of the underlying LoginPanel and ConsoleLoginDialog or
    * ConsoleLoginWindow
    * 
    *  @return  one of LOGON_SUCCEEDED, etc., above 
    */
    public int showDialog() {
        panel = new ConsoleLoginPanel(urls, isFirstLogin);
        String appName;
        if (isFirstLogin) {
            appName = APPLICATION_NAME;
        } else {
            appName = APPLICATION_NAME + " Additional Login"; //$NON-NLS-1$
        }
        if (isFirstLogin) {
            wdw = new ConsoleLoginWindow(appName, panel, (!isFirstLogin), isFirstLogin);
            Dimension wdwSize = wdw.getPreferredSize();
            int newWidth = (int)(wdwSize.width * 1.3);
            wdwSize = new Dimension(newWidth, wdwSize.height);
            wdw.setSize(wdwSize);
            wdw.setLocation(StaticUtilities.centerFrame(wdwSize));
        } else {
            dlg = new ConsoleLoginDialog(appName, panel, (!isFirstLogin), isFirstLogin);
            AbstractButton exitButton = dlg.getDisplayedExitButton();
            exitButton.setText("Cancel"); //$NON-NLS-1$
            dlg.setExitingBlocked(true);
            dlg.addWindowListener(new WindowAdapter() {
                public void windowClosing(WindowEvent ev) {
                    exitButtonPressed();
                }
            });
            Dimension wdwSize = dlg.getPreferredSize();
            int newWidth = (int)(wdwSize.width * 1.3);
            wdwSize = new Dimension(newWidth, wdwSize.height);
            dlg.setSize(wdwSize);
            dlg.setLocation(StaticUtilities.centerFrame(wdwSize));
        }
        addListeners();
        if (initialPassword != null) {
            setPassword(initialPassword);
        }
        if (isFirstLogin) {
            wdw.setVisible(true);
        } else {
            dlg.show();
        }
        return getResult();
    }

    private void addListeners() {
        addLogonButtonListener();
        addExitButtonListener();
    }

    /**
    * This sets the logon button up
    */
    private void addLogonButtonListener() {
        ButtonWidget logonButton;
        if (isFirstLogin) {
            logonButton = wdw.getLoginButton();
        } else {
            logonButton = dlg.getLoginButton();
        }
        logonButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                logonButtonPressed();
            }

        });

    }

    /**
    * This sets the exit button up
    */
    private void addExitButtonListener() {
        ButtonWidget exitButton;
        if (isFirstLogin) {
            exitButton = wdw.getExitButton();
        } else {
            exitButton = dlg.getExitButton();
        }
        exitButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ae) {
                exitButtonPressed();
            }
        });
    }

    /**
    * This is the action that occurs when the login button is pressed
    */
    private void logonButtonPressed() {
        String url = getUrl();
        String user = getUser();
        boolean found = false;
        int i = 0;
        while ((i < existingConnections.length) && (!found)) {
            if (url.equalsIgnoreCase(existingConnections[i].getURL()) &&  //DefectID#10314
                    user.equalsIgnoreCase(existingConnections[i].getUser())) {
                found = true;
            } else {
                i++;
            }
        }
        if (found) {
            String msg = "Connection to " + url + " for user " + //$NON-NLS-1$ //$NON-NLS-2$
                    user + " already exists.  " +  //$NON-NLS-1$
                    "Must select another server or user.";  //$NON-NLS-1$
            StaticUtilities.displayModalDialogWithOK("Connection Already Exists", //$NON-NLS-1$
                    msg);
            existingConnectionURLAndUserEntered = true;
        } else {
            existingConnectionURLAndUserEntered = false;
            login();
            if (result == LOGON_SUCCEEDED) {
                // this was a succesful login so save the URLs
                panel.saveURLs(isFirstLogin);
                if (isFirstLogin) {
                    wdw.dispose();
                } else {
                    dlg.dispose();
                }
            } else {
                if (isFirstLogin) {
                    wdw.setVisible(true);
                } else {
                    //Do nothing
                }
            } 
        }
    }

    private String getUser() {
        return panel.getUserNameField().getText().trim();
    }

    private String getUrl() {
        return ((String)panel.getSystemField().getSelectedItem()).trim();
    }

    public boolean isExistingConnectionURLAndUserEntered() {
        return existingConnectionURLAndUserEntered;
    }
    
    /**
    * This is the action that occurs when the exit button is pressed
    */
    private void exitButtonPressed() {
        result = CANCELLED_LOGON;
        if (!isFirstLogin) {
            dlg.exit();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The LoginPanel used to display the login window
    @since 2.0
    */
    public LoginPanel getLoginPanel() {
        return panel;
    }

    public String getPassword() {
        return new String(getPasswordField());
    }
    
    private void setPassword(String password) {
        panel.getPasswordField().setText(password);
    }
    
    /**
    * Establish a connection to the server
    */
    private void login() {
        String userName = getUser();
        char[] password = getPasswordField();
        String url = getUrl();
        
        String failureHdrMsg = ConsolePlugin.Util.getString("ConsoleLogin.failureHdrMsg");  //$NON-NLS-1$
        String logonExceptionMsg = ConsolePlugin.Util.getString("ConsoleLogin.logonFailureMsg"); //$NON-NLS-1$
        String serverNotRunningMsg = ConsolePlugin.Util.getString("ConsoleLogin.serverNotRunningMsg");  //$NON-NLS-1$       
        String generalExceptionMsg = ConsolePlugin.Util.getString("ConsoleLogin.generalErrorMsg"); //$NON-NLS-1$

        try {
            connectionInfo = new ConnectionInfo(url, userName, password, APPLICATION_NAME);
            connectionInfo.login();
            result = LOGON_SUCCEEDED;
        } catch (ConnectionException ex) {
            ExceptionUtility.showMessage(failureHdrMsg, logonExceptionMsg, ex);
        } catch (CommunicationException ex) {
            if (ExceptionUtility.containsExceptionOfType(ex, LogonException.class)) {
                ExceptionUtility.showMessage(failureHdrMsg, logonExceptionMsg, ex);
            } else {
                ExceptionUtility.showMessage(failureHdrMsg, serverNotRunningMsg, ex);
            } 
        } catch (Throwable t) {
            String msg = null;
            if (t instanceof Exception) {
                msg = generalExceptionMsg;
            }
            ExceptionUtility.showMessage(failureHdrMsg, msg, t);
        }
    }

    private char[] getPasswordField() {
        return panel.getPasswordField().getPassword();
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }
    
    public boolean selectNewConnection() {
        boolean result;
        if (isFirstLogin) {
            result = wdw.selectNewConnection();
        } else {
            result = dlg.selectNewConnection();
        }
        return result;
    }
    
    /**
    *
    * @return int describing the result of this object
    */
    private int getResult() {
        return result;
    }

   
}