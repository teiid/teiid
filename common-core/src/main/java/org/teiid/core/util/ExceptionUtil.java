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

package org.teiid.core.util;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.naming.ConfigurationException;


/**
 * Utility class that provides some useful things for users of the
 * com.metamatrix.api.exception package.  Also provides package-level
 * functionality that is shared among classes in this package.
 */
public class ExceptionUtil {

    /**
     * Prevent instantiation
     */
    private ExceptionUtil() {
        super();
    }

    static Iterator<Throwable> getChildrenIterator(Throwable e){
        return new NestedExceptionIterator(e);
    }

    public static String getLinkedMessagesVerbose(Throwable exception) {
        return getLinkedMessagesVerbose(exception, 0);
    }
    
    static String getLinkedMessagesVerbose( Throwable exception, int level ) {
        if (exception != null){
            StringBuffer buf = new StringBuffer();
            String lastMessage = appendMessage("", buf, null, exception); //$NON-NLS-1$
            Iterator<Throwable> children = getChildrenIterator(exception);
            while ( children.hasNext() ){
                level++;
                exception = children.next();
                lastMessage = appendMessage("->", buf, lastMessage, exception); //$NON-NLS-1$
            }
            return buf.toString();
        }
        return ""; //$NON-NLS-1$
    }
    
    private static final String appendMessage(String prefix, StringBuffer buffer, String lastMessage, Throwable exception) {
        String message = exception.getMessage();
        buffer.append(prefix);
        buffer.append(exception.getClass().getSimpleName());
        if (message != null && !message.equals(lastMessage)) {
            buffer.append('-'); 
            buffer.append(message);
        }
        return message;
    }
    
    /**
     * <p>An Iterator over any nested children <code>Throwable</code>s
     * of either a MetaMatrixException or a MetaMatrixRuntimeException.
     * The first Object returned (if any) by <code>next()</code> will be the same
     * <code>Throwable</code> as returned by the {@link #getChild} method of
     * the root</p>
     *
     * <p>In general, each Object A returned by the <code>next()</code>
     * method is guaranteed to be an instance of
     * <code>Throwable</code>; the <i>previous</i> Object B will have been a
     * <code>MetaMatrixException</code> or <code>MetaMatrixRuntimeException</code>
     * whose {@link #getChild} method will return the same Object A.</p>
     */
    public static class NestedExceptionIterator implements Iterator<Throwable>{

        Throwable exception;
        Throwable child;

        public NestedExceptionIterator(Throwable e){
            exception = e;
        }

        public boolean hasNext(){
            check();
            return (child != null);
        }

        public Throwable next(){
        	if (!hasNext()) {
                throw new NoSuchElementException();
            }
            exception = child;
            child = null;
            return exception;
        }

        private void check(){
            if (child == null){
                if (exception instanceof ConfigurationException) {
                    ConfigurationException e = (ConfigurationException) exception;
                    child = e.getRootCause();
                } else if (exception instanceof SQLException) {
                    SQLException e = (SQLException) exception;
                    child = e.getNextException();
                } 
                if (child == null) {
                	child = exception.getCause();
                }
                if (child == exception) {
                	child = null;
                }
            }
        }

        public void remove(){
            throw new UnsupportedOperationException();
        }
    }
}
