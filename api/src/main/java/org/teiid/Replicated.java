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

package org.teiid;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to replicate Teiid components - this should be used in extension logic.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface Replicated {
	/**
	 * @return true if members should be called asynchronously.  asynch methods should be void. 
	 */
	boolean asynch() default true;
	/**
	 * @return the timeout in milliseconds, or 0 if no timeout.  affects only synch calls.
	 */
	long timeout() default 0;
	/**
	 * @return true if only remote members should be called.  should not be used with replicateState.  method should be void.
	 */
	boolean remoteOnly() default false;
	/**
	 * @return true if the remote members should have a partial state replication called using the first argument as the state after
	 *  the local method has been invoked. should not be used with remoteOnly.
	 */
	boolean replicateState() default false;

}