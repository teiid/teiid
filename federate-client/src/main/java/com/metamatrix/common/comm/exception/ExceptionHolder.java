package com.metamatrix.common.comm.exception;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;

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

public class ExceptionHolder implements Externalizable {
	
	private String message;
	private String className;
	private boolean isProcessingException;
	private Throwable exception;
	
	public ExceptionHolder(Throwable exception) {
		this.exception = exception;
		this.isProcessingException = exception instanceof MetaMatrixProcessingException;
		this.className = exception.getClass().getName();
		this.message = exception.getMessage();
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.className = (String)in.readObject();
		this.isProcessingException = in.readBoolean();
		this.message = (String)in.readObject();
		this.exception = (Throwable)in.readObject();
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(this.className);
		out.writeBoolean(this.isProcessingException);
		out.writeObject(this.message);
		out.writeObject(this.exception);
	}
	
	public Throwable convertException() {
		if (exception != null) {
			return exception;
		}
		if (isProcessingException) {
			return new MetaMatrixProcessingException(CommPlatformPlugin.Util.getString("ExceptionHolder.converted_exception", this.className, this.message));
		}
		return new MetaMatrixComponentException(CommPlatformPlugin.Util.getString("ExceptionHolder.converted_exception", this.className, this.message));
	}
	
}
