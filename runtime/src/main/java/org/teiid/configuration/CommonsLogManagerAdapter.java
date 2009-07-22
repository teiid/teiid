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

package org.teiid.configuration;

import org.apache.commons.logging.Log;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;

/**
 * This class is bridge for hooking LogManager into systems that use apache commons logging. In the Teiid
 * JBoss cache/Jboss Transactions use commons logging. This class used in "commons-logging.properties" file.
 */
public class CommonsLogManagerAdapter implements Log {
	String context;
	
    public CommonsLogManagerAdapter(String context) {
        this.context = context;
    }
	
	@Override
	public void debug(Object arg0) {
		LogManager.log(MessageLevel.DETAIL, this.context, arg0);
	}

	@Override
	public void debug(Object arg0, Throwable arg1) {
		LogManager.log(MessageLevel.DETAIL, this.context, arg1, arg0);
	}

	@Override
	public void error(Object arg0) {
		LogManager.log(MessageLevel.ERROR, this.context, arg0);
	}

	@Override
	public void error(Object arg0, Throwable arg1) {
		LogManager.log(MessageLevel.ERROR, this.context, arg1, arg0);
	}

	@Override
	public void fatal(Object arg0) {
		LogManager.log(MessageLevel.CRITICAL, this.context, arg0);
	}

	@Override
	public void fatal(Object arg0, Throwable arg1) {
		LogManager.log(MessageLevel.CRITICAL, this.context, arg1, arg0);
	}

	@Override
	public void info(Object arg0) {
		LogManager.log(MessageLevel.INFO, this.context, arg0);
	}

	@Override
	public void info(Object arg0, Throwable arg1) {
		LogManager.log(MessageLevel.INFO, this.context, arg1, arg0);
	}

	@Override
	public void trace(Object arg0) {
		LogManager.log(MessageLevel.TRACE, this.context, arg0);
	}

	@Override
	public void trace(Object arg0, Throwable arg1) {
		LogManager.log(MessageLevel.TRACE, this.context, arg1, arg0);
	}

	@Override
	public void warn(Object arg0) {
		LogManager.log(MessageLevel.WARNING, this.context, arg0);
	}

	@Override
	public void warn(Object arg0, Throwable arg1) {
		LogManager.log(MessageLevel.WARNING, this.context, arg1, arg0);
	}
	
	@Override
	public boolean isDebugEnabled() {
		return LogManager.isMessageToBeRecorded(this.context, MessageLevel.DETAIL);
	}

	@Override
	public boolean isErrorEnabled() {
		return LogManager.isMessageToBeRecorded(this.context, MessageLevel.ERROR);
	}

	@Override
	public boolean isFatalEnabled() {
		return LogManager.isMessageToBeRecorded(this.context, MessageLevel.CRITICAL);
	}

	@Override
	public boolean isInfoEnabled() {
		return LogManager.isMessageToBeRecorded(this.context, MessageLevel.INFO);
	}

	@Override
	public boolean isTraceEnabled() {
		return LogManager.isMessageToBeRecorded(this.context, MessageLevel.TRACE);
	}

	@Override
	public boolean isWarnEnabled() {
		return LogManager.isMessageToBeRecorded(this.context, MessageLevel.WARNING);
	}
}
