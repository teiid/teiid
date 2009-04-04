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

package com.metamatrix.console.util;

import java.awt.Dimension;
import java.awt.Toolkit;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.Iterator;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.security.AuthorizationException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.dialog.ErrorDialog;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.toolbox.preference.UserPreferences;

public class ExceptionUtility {
	public final static String ERROR_DIALOG_DIR_KEY = 
			"metamatrix.console.errordialogdirectory"; //$NON-NLS-1$

//    private static final KeyStroke ENTER_RELEASED = 
//    		KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0, true);

    public static final String MSG_HOST_NOT_FOUND = ConsolePlugin.Util.getString("ExceptionUtility.hostNotFoundMsg"); //$NON-NLS-1$
    public static final String MSG_COMP_NOT_FOUND = ConsolePlugin.Util.getString("ExceptionUtility.compNotFoundMsg"); //$NON-NLS-1$
    public static final String MSG_QUERY_SERVICE_NOT_FOUND = ConsolePlugin.Util.getString("ExceptionUtility.queryServiceNotFoundMsg");  //$NON-NLS-1$
    public static final String MSG_AUTHORIZATION_FAILURE  = ConsolePlugin.Util.getString("ExceptionUtility.authFailureMsg"); //$NON-NLS-1$
    public static final String MSG_EXTERNAL_FAILURE = ConsolePlugin.Util.getString("ExceptionUtility.externalFailMsg"); //$NON-NLS-1$
    public static final String MSG_ILLEGAL_REQUEST  = ConsolePlugin.Util.getString("ExceptionUtility.illegalReqstMsg"); //$NON-NLS-1$
    public static final String MSG_CANNOT_INITIALIZE = ConsolePlugin.Util.getString("ExceptionUtility.cannotInitMsg"); //$NON-NLS-1$
    public static final String MSG_CALLBACK_PROBLEM  = ConsolePlugin.Util.getString("ExceptionUtility.callbackProbMsg"); //$NON-NLS-1$
    public static final String MSG_UNSPECIFIED_FAILURE = ConsolePlugin.Util.getString("ExceptionUtility.unspecFailureMsg"); //$NON-NLS-1$
    public static final String TITLE_DEFAULT = ConsolePlugin.Util.getString("ExceptionUtility.titleDefMsg"); //$NON-NLS-1$

    public static void showMessage(String text, String comment, Throwable thr) {
    	LogManager.logError(LogContexts.GENERAL, thr, text + " " + comment); //$NON-NLS-1$
    	showMessage(text, comment, thr, thr);
    }
    
    private static void showMessage(String text, String comment, Throwable thr, Throwable root) {

    	if (thr == null) {
    		showUnspecifiedFailureMessage(text, comment, root);
    		return;
    	}
    	
        if (thr instanceof NoClassDefFoundError) {
            comment = "Unable to locate class for: " + comment; //$NON-NLS-1$
            showUnspecifiedFailureMessage(text, comment, thr);
            return;
        }            
        
        if (thr instanceof ComponentNotFoundException) {
            showCompNotFoundFailureMessage(text, comment, root);
            return;
        }
            
        if ((ExceptionUtility.containsExceptionContainingString(thr,
  		      "class not compatible", true) != null) || //$NON-NLS-1$
  		      (ExceptionUtility.containsExceptionContainingString(thr,
  		      "unmarshal", true) != null)) { //$NON-NLS-1$
        	showUnavailableMessage(text + comment, root);
        	return;
        }
        
        if (thr instanceof ConnectionException) {
        	showExternalFailureMessage(text, ConsolePlugin.Util.getString("ConsoleLogin.logonFailureMsg"), thr); //$NON-NLS-1$
        	return;
        }
        
        if (thr instanceof CommunicationException) {
        	UnknownHostException ex = ExceptionUtil.getExceptionOfType(thr, UnknownHostException.class);
        	if (ex != null) {
        		showUnknownHostException(text + comment, MSG_HOST_NOT_FOUND + ":  Unknown host:  " + ex.getMessage(), ex ); //$NON-NLS-1$
                return;
        	}
            if (ExceptionUtility.containsExceptionOfType(thr, LogonException.class)) {
            	showExternalFailureMessage(text, ConsolePlugin.Util.getString("ConsoleLogin.logonFailureMsg"), thr); //$NON-NLS-1$
                return;
            }
            showExternalFailureMessage(text, ConsolePlugin.Util.getString("ConsoleLogin.serverNotRunningMsg"), thr);  //$NON-NLS-1$
            return;
        } 
        
        if( thr instanceof UnknownHostException) {
            showUnknownHostException(text, MSG_HOST_NOT_FOUND + ":  Unknown host:  " + thr.getMessage(), thr ); //$NON-NLS-1$
            return;
        } 
        
        if (thr instanceof AuthorizationException || thr instanceof LogonException) {
            showAuthorizationFailureMessage(text, comment, thr);
            return;
        }
        
        if (thr instanceof ApplicationInitializationException) { 
            Throwable encryptEx = ExceptionUtility.containsExceptionContainingString(thr, "decrypt", true); //$NON-NLS-1$
            if (encryptEx != null) {
                showExternalFailureMessage("Password Encryption Problem", //$NON-NLS-1$
                                           "Password Encryption Problem.  Please ensure that connector binding " + //$NON-NLS-1$
                                           "passwords were updated after the VDB was deployed from the repository.", //$NON-NLS-1$
                                           encryptEx);
                return;
            }
        }

        showMessage(text, comment, thr.getCause(), root);
    }

    public static void showMessage(String text, Throwable t) {
        showMessage(text, "", t); //$NON-NLS-1$
    }
    
    private static void showDialog( String text,
                                    String cmmnt,
                                    Throwable t,
                                    boolean increaseSize) {
        String comment = cmmnt;
        if (comment == null) {
            comment = ""; //$NON-NLS-1$
        }
        StaticUtilities.endWait(ViewManager.getMainFrame());
        int index = comment.toLowerCase().indexOf("start server side stack trace"); //$NON-NLS-1$
        String initialDirectory = getInitialErrorDialogSaveDirectory();
        ErrorDialog errDlg
            = new ErrorDialog( ViewManager.getMainFrame(),
                               TITLE_DEFAULT,
                               text,
                               (index == -1) ? comment : comment.substring(0, index),
                               t,
                               initialDirectory);
		if (increaseSize) {
			Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
			int width = (int)(screenSize.width * 0.5);
			int height = (int)(screenSize.height * 0.25);
			errDlg.setSize(width, height);
		}
        errDlg.show();
        if (errDlg.isSuccessfullySaved()) {
        	String fileName = errDlg.getSavedFileName().trim();
        	if ((fileName != null) && (fileName.length() > 0)) {
        		setInitialErrorDialogSaveDirectory(fileName);
        	}
        }
    }

	private static String getInitialErrorDialogSaveDirectory() {
		String dir = (String)UserPreferences.getInstance().getValue(
				ExceptionUtility.ERROR_DIALOG_DIR_KEY);
		return dir;
	}
		
	private static void setInitialErrorDialogSaveDirectory(String fileName) {
        String directoryName = null;
        int index = fileName.lastIndexOf(File.separatorChar);
        try {
            directoryName = fileName.substring(0, index);
        } catch (Exception ex) {
        	//Ignore
        }
        if (directoryName != null) {
        	UserPreferences.getInstance().setValue(
        			ExceptionUtility.ERROR_DIALOG_DIR_KEY, directoryName);
        	UserPreferences.getInstance().saveChanges();
        }
	}
	
	private static void showDialog(String text, String comment, Throwable t) {
		showDialog(text, comment, t, true);
	}
	
    public static void showExternalFailureMessage(String text, Exception e) {
        showExternalFailureMessage(text, "", e); //$NON-NLS-1$
    }

    public static void showCannotInitializeMessage(String text, Exception e) {
        showDialog( text, MSG_CANNOT_INITIALIZE, e );
    }

    // 3 args:
    private static void showCompNotFoundFailureMessage(String text,
            String cmmnt, Throwable e) {
        String comment = cmmnt;
        if (comment == null) {
            comment = ""; //$NON-NLS-1$
        }
        String sComment = ""; //$NON-NLS-1$
        if ( comment.trim().equals( "" ) ) { //$NON-NLS-1$
            sComment = MSG_COMP_NOT_FOUND;
        } else {
            sComment = comment;
        }
        showDialog( text, sComment, e );
    }
    
    private static void showAuthorizationFailureMessage(String text,
            String cmmnt, Throwable e) {
        String comment = cmmnt;
        if (comment == null) {
            comment = ""; //$NON-NLS-1$
        }
        String sComment = ""; //$NON-NLS-1$
        if ( comment.trim().equals( "" ) ) { //$NON-NLS-1$
            sComment = MSG_AUTHORIZATION_FAILURE;
        } else {
            sComment = comment;
        }
        showDialog( text, sComment, e );
    }

    private static void showExternalFailureMessage(String text,
            String cmmnt, Throwable e) {
        String comment = cmmnt;
        if (comment == null) {
            comment = ""; //$NON-NLS-1$
        }
        String sComment = ""; //$NON-NLS-1$
        if ( comment.trim().equals( "" ) ) { //$NON-NLS-1$
            sComment = MSG_EXTERNAL_FAILURE;
        } else {
            sComment = comment;
        }
        showDialog( text, sComment, e );
    }
    
	private static void showUnavailableMessage(String text, Throwable ex) {
		String comment = ConsolePlugin.Util.getString("ExceptionUtility.serverUnavailableMsg"); //$NON-NLS-1$
		
		showDialog(text, comment, ex, true);
	}
    
    private static void showUnknownHostException(String text,
                                                String cmmnt,
                                                Throwable e) {

        String comment = cmmnt;
        if (comment == null) {
            comment = ""; //$NON-NLS-1$
        }
        String sComment = ""; //$NON-NLS-1$
        if (comment.trim().equals("")) { //$NON-NLS-1$
            sComment = MSG_HOST_NOT_FOUND;
        } else {
            sComment = comment;
        }
        showDialog(text, sComment, e);

    }
   	
    private static void showUnspecifiedFailureMessage(String text,
            String cmmnt, Throwable t) {
        String comment = cmmnt;
        if (comment == null) {
            comment = ""; //$NON-NLS-1$
        }
        String sComment = ""; //$NON-NLS-1$
        if ( comment.trim().equals( "" ) ) { //$NON-NLS-1$
            sComment = MSG_UNSPECIFIED_FAILURE;
        } else {
            sComment = comment;
        }
        showDialog(text, sComment, t);
    }
    
    public static boolean containsExceptionOfType(Throwable ex, Class<? extends Throwable> cls) {
    	return ExceptionUtil.getExceptionOfType(ex, cls) != null;
    }

    private static Throwable containsExceptionContainingString(Throwable ex, String searchingFor, boolean ignoreCase) {
        int searchingForLen = searchingFor.length();
        Throwable result = null;
        Throwable currentThrowable = ex;
        boolean matchFound = false;
        boolean done = false;
        
        while ((!matchFound) && (!done) && currentThrowable != null) {
            String message = currentThrowable.getMessage();
            if (message != null) {
                int highestStartingIndex = message.length() - searchingForLen;
                int loc = 0;
                while ((!matchFound) && (loc <= highestStartingIndex)) {
                    matchFound = message.regionMatches(ignoreCase, loc, searchingFor, 0, searchingForLen);
                    if (matchFound) {
                        result = currentThrowable;
                    } else {
                        loc++;
                    }
                }
            }
            if (!matchFound) {
                if (currentThrowable instanceof MetaMatrixException) {
                    currentThrowable = ((MetaMatrixException)currentThrowable).getChild();
                } else if (currentThrowable instanceof javax.naming.NamingException) {
                	currentThrowable = ((javax.naming.NamingException)currentThrowable).getRootCause();
                } else {
                    done = true;
                }
            }
        }
        return result;
    }
    
    public static MetaMatrixException containsExceptionHavingCode(
    		Throwable ex, String code) {
    	MetaMatrixException result = null;
        Throwable currentThrowable = ex;
        boolean done = false;
        while (!done) {
        	if (currentThrowable instanceof MetaMatrixException) {
        		MetaMatrixException mex = (MetaMatrixException)currentThrowable;
        		String curCode = mex.getCode();
        		if (code.equalsIgnoreCase(curCode)) {
        			result = mex;
        			done = true;
        		} else {
        			currentThrowable = mex.getChild();
        		}
        	} else {
        		done = true;
        	}
        }
        return result;
    }    	
    
    /**
     * Unroll a chain of exceptions to return the cause.
     * @return A <code>Throwable</code> exception that represents the cause of the problem.
     */
    public static Throwable unRollException(final Throwable throwable) {
        
         Throwable result = throwable;

        if (throwable == null) {
            return result;
        }
//            throwables.add(throwable);
//            UniqueThrowableName name = new UniqueThrowableName(throwable.getClass().getName(), depth);
//            throwableNames.add(name);
        if (throwable instanceof MetaMatrixException) {
            result = unRollException(((MetaMatrixException)throwable).getChild());
        } else if (throwable instanceof MetaMatrixRuntimeException) {
            result = unRollException(((MetaMatrixRuntimeException)throwable).getChild());
        } else if (throwable instanceof MultipleException) {
            for (Iterator iter = ((MultipleException)throwable).getExceptions().iterator(); iter.hasNext();) {
                result = unRollException((Throwable)iter.next());
            }
        } else if (throwable instanceof InvocationTargetException) {
            result = unRollException(((InvocationTargetException)throwable).getTargetException());
        }
        
        // if one of the chained exceptions did not produce a child exception, then return the input
        if ( result == null ) {
            return result = throwable;
        }
        return result;
    }
    
        
}//end ExceptionUtility
