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

package com.metamatrix.jdbc;

import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.sql.SQLException;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ProcedureErrorInstructionException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.jdbc.api.SQLStates;

/**
 * All exceptions thrown by the MetaMatrix server are wrapped as MMSQLExceptions.
 * If there are multiple exceptions, they can be represented as a chain of
 * MMSQLExceptions. Thus a server side chain of exceptions map directly to a
 * corresponding chain of MMSQLExceptions. This class reads in MetaMatrix
 * exceptions, which are chained exceptions and constructs it's own chain of exceptions.
 */

public class MMSQLException extends SQLException implements com.metamatrix.jdbc.api.SQLException {

    /**
     * No-arg constructor required by Externalizable semantics.
     */
    public MMSQLException() {
        super();
    }
    
    public MMSQLException(String reason) {
        super(reason, SQLStates.DEFAULT);
    }

    public MMSQLException(String reason, String state) {
        super(reason, state);
    }    
    
    public static MMSQLException create(Throwable exception) {
        if (exception instanceof MMSQLException) {
            return (MMSQLException)exception;
        }
        return create(exception, exception.getMessage());
    }
        
    /**
     * Constructor used to wrap metamatrix exceptions into MMSQLExceptions.
     * @param reason String object which is the description of the exception.
     * @param ex Throwable object which needs to be wrapped.
     */
    public MMSQLException(Throwable ex, String reason, String sqlState) {
        super(reason, sqlState); // passing the message to the super class constructor.
        initCause(ex);
    }
    
    /**
     * Constructor used to wrap or copy a SQLException into a MMSQLException 
     * @see com.metamatrix.common.util.exception.SQLEXceptionUnroller
     * @param ex
     * @param message
     * @param addChildren
     */
    private MMSQLException(SQLException ex, String message, boolean addChildren) {
        super(message, ex.getSQLState() == null ? SQLStates.DEFAULT : ex.getSQLState(), ex.getErrorCode());
        
        if (addChildren) {
        	SQLException childException = ex; // this a child to the SQLException constructed from reason

            while (childException != null) {
                if (childException instanceof MMSQLException) {
                    super.setNextException(ex);
                    break;
                } 
                super.setNextException(new MMSQLException(childException, getMessage(childException, null),false));
                childException = childException.getNextException();
            }
        }
    }    
    public static MMSQLException create(Throwable exception, String message) {
		message = getMessage(exception, message);
		Throwable origException = exception;
		if (exception instanceof MMSQLException) {
			if (message.equals(exception.getMessage())) {
				return (MMSQLException) exception;
			}
		}
		if (exception instanceof SQLException) {
			return new MMSQLException((SQLException) exception, message, true);
		}
		String sqlState = SQLStates.DEFAULT;

		exception = findRootException(exception);

		sqlState = determineSQLState(exception, sqlState);
		return new MMSQLException(origException, message, sqlState);
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
		} else if (exception instanceof MetaMatrixProcessingException) {
			sqlState = SQLStates.USAGE_ERROR;
		} else if (exception instanceof UnknownHostException
				|| exception instanceof ConnectException
				|| exception instanceof MalformedURLException
				|| exception instanceof NoRouteToHostException
				|| exception instanceof ConnectionException) {
			sqlState = SQLStates.CONNECTION_EXCEPTION_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION;
		} else if (exception instanceof IOException) {
			sqlState = SQLStates.CONNECTION_EXCEPTION_STALE_CONNECTION;
		} else if (exception instanceof MetaMatrixCoreException) {
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
        if (exception instanceof MetaMatrixRuntimeException) {
        	while (exception.getCause() != exception
        			&& exception.getCause() != null) {
        		exception = exception.getCause();
        	}
        	if (exception instanceof MetaMatrixRuntimeException) {
        		MetaMatrixRuntimeException runtimeException = (MetaMatrixRuntimeException) exception;
        		while (runtimeException.getChild() != exception
        				&& runtimeException.getChild() != null) {
        			if (runtimeException.getChild() instanceof MetaMatrixRuntimeException) {
        				runtimeException = (MetaMatrixRuntimeException) runtimeException
        						.getChild();
        			} else {
        				exception = runtimeException.getChild();
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
     * @see java.lang.Throwable#getCause()
     * @since 4.3.2
     */
    public Throwable getCause() {
        if (this.getNextException() != null) {
        	return getNextException();
        }
        return super.getCause();
    }

    /** 
     * @see com.metamatrix.jdbc.api.SQLException#isSystemErrorState()
     * @since 4.3
     */
    public boolean isSystemErrorState() {
        return SQLStates.isSystemErrorState(getSQLState());
    }

    /** 
     * @see com.metamatrix.jdbc.api.SQLException#isUsageErrorState()
     * @since 4.3
     */
    public boolean isUsageErrorState() {
        return SQLStates.isUsageErrorState(getSQLState());
    }
}