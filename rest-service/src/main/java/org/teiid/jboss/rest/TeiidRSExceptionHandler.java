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
package org.teiid.jboss.rest;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jboss.resteasy.spi.InternalServerErrorException;
import org.jboss.resteasy.spi.NotFoundException;
import org.jboss.resteasy.spi.UnauthorizedException;

@Provider
public class TeiidRSExceptionHandler implements ExceptionMapper<Exception> {
	
	@Context
	protected HttpHeaders httpHeaders;

	@Override
	public Response toResponse(Exception e) {
		
		ResponseError error = new ResponseError();
		
	    String code = "ERROR"; //$NON-NLS-1$ 
	    if(e instanceof UnauthorizedException){
			code = "401"; //$NON-NLS-1$ 
		} else if(e instanceof NotFoundException){
			code = "404"; //$NON-NLS-1$ 
		} else if(e instanceof InternalServerErrorException) {
			code = "500"; //$NON-NLS-1$ 
		}
		error.setCode(code);
		
		error.setMessage(e.getMessage());
		
	    StringWriter sw = new StringWriter();
	    PrintWriter pw = new PrintWriter(sw);
	    e.printStackTrace(pw);
	    error.setDetails(sw.toString());
        
	    String type = MediaType.APPLICATION_XML;
        List<MediaType> acceptTypes = httpHeaders.getAcceptableMediaTypes();
        if(acceptTypes != null){
        	for (MediaType acceptType : acceptTypes){
        		if (isApplicationJsonWithParametersIgnored(acceptType)) {
        			type = MediaType.APPLICATION_JSON;
        			break;
        		}
        	}
        }
        
        return Response.serverError().entity(error).type(type).build();
	}
	
	private boolean isApplicationJsonWithParametersIgnored(MediaType acceptType) {
	    return acceptType.getType().equals(MediaType.APPLICATION_JSON_TYPE.getType()) &&
	        acceptType.getSubtype().equals(MediaType.APPLICATION_JSON_TYPE.getSubtype());
	 }
	
	@XmlRootElement(name = "error") //$NON-NLS-1$ 
	@XmlType(propOrder = { "code", "message", "details"}) //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
	public static class ResponseError {

		private String code;
		
		private String message;
		
		private String details;

		@XmlElement(name = "code") //$NON-NLS-1$ 
		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}

		@XmlElement(name = "message") //$NON-NLS-1$ 
		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		@XmlElement(name = "details") //$NON-NLS-1$ 
		public String getDetails() {
			return details;
		}

		public void setDetails(String details) {
			this.details = details;
		}
		
	}

}
