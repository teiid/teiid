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

package org.teiid.jdbc;

import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.sql.SQLException;

import org.teiid.client.ProcedureErrorInstructionException;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;


/**
 * Teiid specific SQLException
 */

public class TeiidSQLException extends SQLException {

	private static final long serialVersionUID = 3672305321346173922L;
	private String teiidCode;

	/**
     * No-arg constructor required by Externalizable semantics.
     */
    public TeiidSQLException() {
        super();
    }
    
    public TeiidSQLException(String reason) {
        super(reason, SQLStates.DEFAULT);
    }

    public TeiidSQLException(String reason, String state) {
        super(reason, state);
    }    
    
    public static TeiidSQLException create(Throwable exception) {
        if (exception instanceof TeiidSQLException) {
            return (TeiidSQLException)exception;
        }
        return create(exception, exception.getMessage());
    }
        
    public TeiidSQLException(Throwable ex, String reason, String sqlState, int errorCode) {
        super(reason, sqlState, errorCode); // passing the message to the super class constructor.
        initCause(ex);
    }
    
    private TeiidSQLException(SQLException ex, String message, boolean addChildren) {
        super(message, ex.getSQLState() == null ? SQLStates.DEFAULT : ex.getSQLState(), ex.getErrorCode(), ex);
        if (addChildren) {
        	SQLException childException = ex.getNextException(); // this a child to the SQLException constructed from reason

            while (childException != null) {
                if (childException instanceof TeiidSQLException) {
                    super.setNextException(ex);
                    break;
                } 
                super.setNextException(new TeiidSQLException(childException, getMessage(childException, null),false));
                childException = childException.getNextException();
            }
        }
    }    
    
    public static TeiidSQLException create(Throwable exception, String message) {
		message = getMessage(exception, message);
		Throwable origException = exception;
		if (exception instanceof TeiidSQLException 
				&& message.equals(exception.getMessage())) {
			return (TeiidSQLException) exception;
		}
		if (exception instanceof SQLException) {
			return new TeiidSQLException((SQLException) exception, message, true);
		}
		String sqlState = null;
		int errorCode = 0;
		SQLException se = ExceptionUtil.getExceptionOfType(exception, SQLException.class);
		if (se != null && se.getSQLState() != null) {
			sqlState = se.getSQLState();
			errorCode = se.getErrorCode();
		}
		TeiidException te = ExceptionUtil.getExceptionOfType(exception, TeiidException.class);
		String code = null;
		if (te != null && te.getCode() != null) {
			code = te.getCode();
			if (errorCode == 0) {
				String intPart = code;
				if (code.startsWith("TEIID")) { //$NON-NLS-1$
					intPart = code.substring(5);
				}
				try {
					errorCode = Integer.valueOf(intPart);
				} catch (NumberFormatException e) {
					
				}
			}
		}
		if (sqlState == null) {
			exception = findRootException(exception);
			sqlState = determineSQLState(exception, sqlState);
		}
		if (sqlState == null) {
			sqlState = SQLStates.DEFAULT;
		}
		TeiidSQLException tse = new TeiidSQLException(origException, message, sqlState, errorCode);
		tse.teiidCode = code;
		return tse;
	}

    /** 
     * @param exception
     * @param sqlState
     * @return
     */
    private static String determineSQLState(Throwable exception,
                                            String sqlState) {
        if (exception instanceof InvalidSessionException) { 
			sqlState = SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION;
		} else if (exception instanceof LogonException) { 
			sqlState = SQLStates.INVALID_AUTHORIZATION_SPECIFICATION_NO_SUBCLASS;
		} else if (exception instanceof ProcedureErrorInstructionException) {
			sqlState = SQLStates.VIRTUAL_PROCEDURE_ERROR;
		} else if (exception instanceof TeiidProcessingException) {
			sqlState = SQLStates.USAGE_ERROR;
		} else if (exception instanceof UnknownHostException
				|| exception instanceof ConnectException
				|| exception instanceof MalformedURLException
				|| exception instanceof NoRouteToHostException
				|| exception instanceof ConnectionException) {
			sqlState = SQLStates.CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION;
		} else if (exception instanceof IOException) {
			sqlState = SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION;
		} else if (exception instanceof TeiidException) {
            if (exception instanceof CommunicationException) {
                sqlState = SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION;
            }
            
            Throwable originalException = exception;
            exception = originalException.getCause();
            exception = findRootException(exception);
            
            if (exception instanceof CommunicationException) {
                sqlState = SQLStates.USAGE_ERROR;
                exception = exception.getCause();
            }
            
            if (exception != null && exception != originalException) {
                sqlState = determineSQLState(exception, sqlState);
            }
		}
        return sqlState;
    }

    /** 
     * @param exception
     * @return
     */
    private static Throwable findRootException(Throwable exception) {
        if (exception instanceof TeiidRuntimeException) {
        	while (exception.getCause() != exception
        			&& exception.getCause() != null) {
        		exception = exception.getCause();
        	}
        	if (exception instanceof TeiidRuntimeException) {
        		TeiidRuntimeException runtimeException = (TeiidRuntimeException) exception;
        		while (runtimeException.getCause() != exception
        				&& runtimeException.getCause() != null) {
        			if (runtimeException.getCause() instanceof TeiidRuntimeException) {
        				runtimeException = (TeiidRuntimeException) runtimeException
        						.getCause();
        			} else {
        				exception = runtimeException.getCause();
        				break;
        			}
        		}
        	}
        }
        return exception;
    }
    
    /** 
     * @param exception
     * @param message
     * @return
     * @since 4.1
     */
    private static String getMessage(Throwable exception,
                                     String message) {
        if (message == null) {
            message = exception.getMessage();
            if (message == null) {
                message = exception.getClass().getName();
            }
        }
        return message;
    }
    
    /** 
     * @see org.teiid.jdbc.api.SQLException#isSystemErrorState()
     * @since 4.3
     */
    public boolean isSystemErrorState() {
        return SQLStates.isSystemErrorState(getSQLState());
    }

    /** 
     * @see org.teiid.jdbc.api.SQLException#isUsageErrorState()
     * @since 4.3
     */
    public boolean isUsageErrorState() {
        return SQLStates.isUsageErrorState(getSQLState());
    }
    
    public String getTeiidCode() {
		return teiidCode;
	}
}