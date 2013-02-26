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

import java.io.StringWriter;
import java.util.ArrayList;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import org.odata4j.core.ODataConstants;
import org.odata4j.core.OError;
import org.odata4j.core.OErrors;
import org.odata4j.exceptions.ODataProducerException;
import org.odata4j.format.FormatWriter;
import org.odata4j.format.FormatWriterFactory;
import org.odata4j.producer.ErrorResponse;
import org.odata4j.producer.Responses;
import org.odata4j.producer.resources.ExceptionMappingProvider;

@Provider
public class ODataExceptionMappingProvider extends ExceptionMappingProvider {

	public Response toResponse(RuntimeException e) {
		if (e instanceof ODataProducerException) {
			return super.toResponse(e);
		}

		ArrayList<MediaType> headers = new ArrayList<MediaType>();
		headers.add(MediaType.APPLICATION_XML_TYPE);
		headers.add(MediaType.APPLICATION_JSON_TYPE);
		FormatWriter<ErrorResponse> fw = FormatWriterFactory.getFormatWriter(ErrorResponse.class, headers,"atom", null);
		StringWriter sw = new StringWriter();
		fw.write(null, sw, getErrorResponse(e));

		return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode())
				.type(fw.getContentType())
				.header(ODataConstants.Headers.DATA_SERVICE_VERSION,ODataConstants.DATA_SERVICE_VERSION_HEADER)
				.entity(sw.toString()).build();
	}

	public ErrorResponse getErrorResponse(RuntimeException exception) {
		OError error = OErrors.error("", exception.getMessage(), null);
		return Responses.error(error);
	}
}
