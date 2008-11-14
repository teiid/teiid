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

package com.metamatrix.platform.config.transaction;



/**
 * Date Oct 11, 2002
 *
 */
public class ConfigTransactionLockException extends ConfigTransactionException {
		
    /**
     * No-arg CTOR
     */
    public ConfigTransactionLockException(  ) {
        super(  );
    } 
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public ConfigTransactionLockException( String message ) {
        super(message );
        this.setTransactionState(ConfigTransactionException.TRANS_PROCESSING_ERROR);

    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public ConfigTransactionLockException( String code, String message ) {
        super( code, message );
        this.setTransactionState(ConfigTransactionException.TRANS_PROCESSING_ERROR);
        
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param code A code denoting the exception
     * @param e An exception to nest within this one
     */
    public ConfigTransactionLockException( Exception e, String message ) {
        super( e, message );
        
        this.setTransactionState(ConfigTransactionException.TRANS_PROCESSING_ERROR);
        
    }

    /**
     * Construct an instance from a message and a code and an exception to
     * chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     * @param code A code denoting the exception
     */
    public ConfigTransactionLockException( Exception e, String code, String message ) {
        super( e, code, message );
        this.setTransactionState(ConfigTransactionException.TRANS_PROCESSING_ERROR);
        
    }


}
