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

package com.metamatrix.console.main;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;

import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JWindow;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.log.config.BasicLogConfiguration;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.dialog.ConsoleLogin;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.log.FileLogWriter;
import com.metamatrix.internal.core.log.PlatformLog;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.SplashWindow;
import com.metamatrix.toolbox.ui.widget.util.IconFactory;

/**
 * Main class for the MetaMatrix Console application.
 */
public final class AdminConsoleMain {
    //############################################################################################################################
    //# Static variables
    //############################################################################################################################
    public final static String DEFAULT_LOG_FILE =
    		"..\\log\\console_%VM_NAME%.log"; //$NON-NLS-1$
    public final static String VM_STRING = "%VM_NAME%"; //$NON-NLS-1$
    public final static int VM_STRING_LEN = VM_STRING.length();
    public final static String DEFAULT_ICON = "console.ico"; //$NON-NLS-1$

    static {
        IconFactory.setDefaultJarPath("/com/metamatrix/console/images/"); //$NON-NLS-1$
        IconFactory.setDefaultRelativePath("../images/"); //$NON-NLS-1$
    }
    
    //############################################################################################################################
    //# Main                                                                                                                     #
    //############################################################################################################################

    /**
     * Main entry point into the application.
     */
    public static final void main(String[] args) {
        try {
            AdminConsoleMain a = new AdminConsoleMain();
            a.init();
        } catch (Throwable theThrowable) {
            final String MSG =
                "Exception running the Console. Please notify the help desk."; //$NON-NLS-1$
            if (theThrowable instanceof Exception) {
                ExceptionUtility.showMessage(MSG, theThrowable);
                LogManager.logCritical(LogContexts.GENERAL,
                        theThrowable, MSG);
            } else {
                LogManager.logCritical(LogContexts.GENERAL,
                        "Throwable exception in main:" + theThrowable); //$NON-NLS-1$
            }
            theThrowable.printStackTrace();
            System.exit(-1);
        }
    }

    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
     * Initialize the application.
     */
    private void init() throws ExternalException, Exception {
		setUpLogging();
    	//Note that this message will always be logged, even if the 
    	//LogContexts.GENERAL context is to be discarded.  Discarding
    	//does not take effect until the setLogLevelAndDiscardedLogContexts() 
    	//method is called, below.  BWP 02/17/04
        LogManager.logCritical(LogContexts.GENERAL,
                "Initializing the application"); //$NON-NLS-1$
        try {
            StaticProperties.loadBootStrap();
            
        } catch (Exception e) {
            LogManager.logError(LogContexts.GENERAL, e,
                    "Error loading bootstrap properties"); //$NON-NLS-1$
            ExceptionUtility.showCannotInitializeMessage(
                    "Error loading bootstrap.", e); //$NON-NLS-1$
            throw new RuntimeException(e.getMessage());
        }
		setLogLevelAndDiscardedLogContexts();

		java.util.List /*<String>*/ urls = getURLNames();
        ConsoleLogin logon = new ConsoleLogin(urls, true, null);
        int result = logon.showDialog();
        boolean continuing = true;
        JWindow splash = null;

        switch (result) {
            case ConsoleLogin.LOGON_SUCCEEDED:
            	splash = showSplashWindow(splash);
                break;
            case ConsoleLogin.CANCELLED_LOGON:
                continuing = false;
                break;
            default:
                LogManager.logCritical(LogContexts.GENERAL,
                        "ConsoleLogin returned unknown status.  Not continuing."); //$NON-NLS-1$
                continuing = false;
                break;
        }

        if (continuing) {
            ConnectionInfo connection = logon.getConnectionInfo();
            try {
                initModels(connection);
				UserCapabilities uc = UserCapabilities.createInstance();
				uc.init(connection);
			} catch (Exception ex) {
                throw ex;
            }
            StaticProperties.setUserName(
                    logon.getLoginPanel().getUserNameField().getText());
            StaticProperties.setCurrentURL(connection.getURL());

            try {
                StaticProperties.saveProperties();
            } catch (final ExternalException err) {
                ExceptionUtility.showMessage("Error saving properties", err); //$NON-NLS-1$
            }
			continuing = ModelManager.initViews(connection, true);
			splash.dispose();
        }
        if (!continuing) {
            System.exit(0);
        }
    }

	private void setLogLevelAndDiscardedLogContexts() {
		LogConfiguration logConfig = null;
 		try {
 			logConfig = CurrentConfiguration.getInstance().getConfiguration()
 					.getLogConfiguration();
 		} catch (Exception ex) {
 		}
 		if (logConfig != null) {
 			boolean modified = false;
 			String contextValues = (String)UserPreferences.getInstance(
 					).getProperties().get(
 					BasicLogConfiguration.LOG_CONTEXT_PROPERTY_NAME);
 			if (contextValues != null) {
 				StringTokenizer tokenizer = new StringTokenizer(contextValues, 
    					BasicLogConfiguration.CONTEXT_DELIMETER);
  				while (tokenizer.hasMoreElements()) {
  					String token = tokenizer.nextElement().toString();
  					logConfig.discardContext(token);
  					modified = true;
  				}
   			}
   			String logLevelStr = (String)UserPreferences.getInstance(
   					).getProperties().get(
   					BasicLogConfiguration.LOG_LEVEL_PROPERTY_NAME);
   			if (logLevelStr != null) {
   				Integer logLevelInt = null;
   				try {
   					logLevelInt = new Integer(logLevelStr);
   				} catch (Exception ex) {
   				}
   				if (logLevelInt != null) {
   					int logLevel = logLevelInt.intValue();
   					logConfig.setMessageLevel(logLevel);
   					modified = true;
   				}
   			}
   			if (modified) {
  				LogManager.setLogConfiguration(logConfig);
   			}
 		}
 	}   
     
	private JWindow showSplashWindow(JWindow splash) {
        final String alternateSplash = ConsolePlugin.Util.getString("Console.alternateSplash");  //$NON-NLS-1$
	    ImageIcon altSplashIcon = null;
	    if(alternateSplash!=null&&alternateSplash.trim().length()>0) {
	        altSplashIcon = IconFactory.getIconForImageFile(alternateSplash); 
	    }
	    JPanel panel = null;
	    if(altSplashIcon!=null) {
	    	panel = new JPanel();
	    	panel.add(new JLabel(altSplashIcon));
	    	splash = new JWindow();
	    	splash.getContentPane().add(panel);
	    	splash.pack();
	    } else {
	        splash = new SplashWindow();
	    }
	    
        splash.setLocation(StaticUtilities.centerFrame(splash.getSize()));
        splash.show();
        return splash;
	}

    /**
     * @returns Collection of strings with URL name
     */
    private java.util.List /*<String>*/ getURLNames() {
        java.util.List /*<string>*/ urlNames = StaticProperties.getURLsCopy();
        if (urlNames == null) {
            urlNames = new ArrayList(0);
        }
        return urlNames;
    }

    private void initModels(ConnectionInfo connection) throws Exception {
        ModelManager.init(connection);
    }
    
    private void setUpLogging() {
    	String captureSystemOutProp = 
    			"metamatrix.log.captureSystemOut"; //$NON-NLS-1$
    	String captureSystemErrProp =
    			"metamatrix.log.captureSystemErr"; //$NON-NLS-1$
    	String captureSystemOutVal = CurrentConfiguration.getInstance().getProperty(
    			captureSystemOutProp);
        if (captureSystemOutVal == null) {
            captureSystemOutVal = "false"; //$NON-NLS-1$
        } else {
            captureSystemOutVal = captureSystemOutVal.trim();
        }
    	boolean captureSystemOut = captureSystemOutVal.equalsIgnoreCase(
    			"true"); //$NON-NLS-1$
    	String captureSystemErrVal = CurrentConfiguration.getInstance().getProperty(
    			captureSystemErrProp);
        if (captureSystemErrVal == null) {
            captureSystemErrVal = "false"; //$NON-NLS-1$
        } else {
            captureSystemErrVal = captureSystemErrVal.trim();
        }
    	boolean captureSystemErr = captureSystemErrVal.equalsIgnoreCase(
    			"true"); //$NON-NLS-1$
    	String logFileProp = "metamatrix.log.file"; //$NON-NLS-1$
    	String logFile = CurrentConfiguration.getInstance().getProperty(logFileProp);
    	File tmpFile = null;
    	if (logFile == null) {
    		logFile = substituteVMName(DEFAULT_LOG_FILE);
    	} else {
    		try {
    			tmpFile = new File(substituteVMName(logFile));
    		} catch (Exception ex) {
    			logFile = DEFAULT_LOG_FILE;
    		}
    	}
		if (tmpFile == null) {
    		tmpFile = new File(logFile);
    	}
    	if (tmpFile.exists()) {
            
    		int index = logFile.lastIndexOf('.');
    		String logFileBegin = logFile.substring(0, index);
    		String logFileEnd = logFile.substring(index);
    		DateFormat formatter = new SimpleDateFormat(
    				"yyyy-MM-dd_HH-mm"); //$NON-NLS-1$
    		String logFileDate = formatter.format(new Date()).toString();
    		tmpFile.renameTo(new File(logFileBegin + logFileDate +
    				logFileEnd));
		}
    	try {
    		FileOutputStream fos = new FileOutputStream(tmpFile);
    		PrintStream log = new PrintStream(fos);
    		if (captureSystemOut) {
    			System.setOut(log);
    		}
    		if (captureSystemErr) {
    			System.setErr(log);
    		}
    		FileLogWriter flw = new FileLogWriter(tmpFile);
            PlatformLog.getInstance().addListener(flw);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
        
        StaticProperties.setLogDirectory(tmpFile.getParentFile());
    }
    
    private String substituteVMName(String str) {
		String outputStr;
    	int index = str.indexOf(VM_STRING);
    	if (index >= 0) {
    		String theVM = "MMProcess"; //$NON-NLS-1$
    		outputStr = str.substring(0, index) + theVM + str.substring(index + VM_STRING_LEN);
    	} else {
    		outputStr = str;
    	}
    	return outputStr;
    }
}
