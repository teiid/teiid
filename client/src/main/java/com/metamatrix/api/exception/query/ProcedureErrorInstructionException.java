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

package com.metamatrix.api.exception.query;

import com.metamatrix.api.exception.MetaMatrixProcessingException;


/** 
 * Used to notify the user that the virtual procedure raised an error.
 * @since 4.3
 */
public class ProcedureErrorInstructionException extends MetaMatrixProcessingException {

    /** 
     * 
     * @since 4.3
     */
    public ProcedureErrorInstructionException() {
        super();
    }

    /** 
     * @param message
     * @since 4.3
     */
    public ProcedureErrorInstructionException(String message) {
        super(message);
    }

}
