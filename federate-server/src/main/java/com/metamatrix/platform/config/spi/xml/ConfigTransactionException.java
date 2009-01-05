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

package com.metamatrix.platform.config.spi.xml;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.metamatrix.common.transaction.TransactionException;

/**
 * Date Oct 10, 2002
 *
 * 
 * TransactionException exception indicates that the request cannot be
 * executed because of an error with the transaction.
 */

public class ConfigTransactionException extends TransactionException {

    //the transState indicates the state of the transaction
    private String transState = ""; //$NON-NLS-1$

    public static final String TRANS_ALREADY_LOCKED = "TRANS_ALREADY_LOCKED"; //$NON-NLS-1$

    public static final String TRANS_NOT_LOCKED_BY_SAME_USER = "TRANS_NOT_LOCKED_BY_SAME_USER"; //$NON-NLS-1$

    public static final String TRANS_PROCESSING_ERROR = "TRANS_PROCESSING_ERROR"; //$NON-NLS-1$

    public void setTransactionState(String code) {
        this.transState = code;
    }
    
    public String getTransactionState() {
        return this.transState;
    }
    
    /**
     * No-arg CTOR
     */
    public ConfigTransactionException(  ) {
        super(  );
    }    

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public ConfigTransactionException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public ConfigTransactionException( String code, String message ) {
        super( code, message );
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param code A code denoting the exception
     * @param e An exception to nest within this one
     */
    public ConfigTransactionException( Exception e, String message ) {
        super( e, message );
    }

    /**
     * Construct an instance from a message and a code and an exception to
     * chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     * @param code A code denoting the exception
     */
    public ConfigTransactionException( Exception e, String code, String message ) {
        super( e, code, message );
    }

    /**
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        transState = (String)in.readObject();
    }

    /**
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(transState);
    }

} // END CLASS


