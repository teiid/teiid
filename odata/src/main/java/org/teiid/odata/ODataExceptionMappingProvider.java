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
package org.teiid.odata;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.odata4j.core.OError;
import org.odata4j.exceptions.ODataProducerException;
import org.odata4j.producer.resources.ExceptionMappingProvider;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;

@Provider
public class ODataExceptionMappingProvider extends ExceptionMappingProvider {

	public Response toResponse(RuntimeException e) {
		if (e instanceof ODataProducerException) {
			OError oError = ((ODataProducerException) e).getOError();
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_ODATA, MessageLevel.DETAIL)) {
				LogManager.logWarning(LogConstants.CTX_ODATA, e, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16012, oError.getCode(), oError.getMessage()));
			} else {
				LogManager.logWarning(LogConstants.CTX_ODATA, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16012, oError.getCode(), oError.getMessage()));
			}
		} else {
			LogManager.logError(LogConstants.CTX_ODATA, e, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16013));
		}
		return super.toResponse(e);
	}
}
