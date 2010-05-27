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

package org.teiid.translator;


/**
 * An execution represents the state and lifecycle for a particular 
 * command execution.  The methods provided on this interface define
 * standard lifecycle methods.  
 * When execution completes, the {@link #close()} will be called.  If 
 * execution must be aborted, due to user or administrator action, the 
 * {@link #cancel()} will be called.
 */
public interface Execution {

    /**
     * Terminates the execution normally.
     */
    void close();
    
    /**
     * Cancels the execution abnormally.  This will happen via
     * a different thread from the one performing the execution, so
     * should be expected to happen in a multi-threaded scenario.
     */
    void cancel() throws TranslatorException;
    
    /**
     * Execute the associated command.  Results will be retrieved through a specific sub-interface call.
     * @throws TranslatorException
     */
    void execute() throws TranslatorException;

}
