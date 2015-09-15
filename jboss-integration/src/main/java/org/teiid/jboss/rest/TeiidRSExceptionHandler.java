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

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.jboss.resteasy.spi.InternalServerErrorException;
import org.jboss.resteasy.spi.NotFoundException;
import org.jboss.resteasy.spi.UnauthorizedException;

@Provider
public class TeiidRSExceptionHandler implements ExceptionMapper<Exception> {

	@Override
	public Response toResponse(Exception e) {
		
	    String code = "ERROR";
		if(e instanceof UnauthorizedException){
			code = "401";
		} else if(e instanceof NotFoundException){
			code = "404";
		} else if(e instanceof InternalServerErrorException) {
			code = "500";
		}
		
	    String message = e.getMessage();
		
	    StringWriter sw = new StringWriter();
	    PrintWriter pw = new PrintWriter(sw);
	    e.printStackTrace(pw);
	    String details = sw.toString();
		
	    StringBuilder response = new StringBuilder("<error>"); //$NON-NLS-1$ 
        response.append("<code>" + code + "</code>"); //$NON-NLS-1$ //$NON-NLS-2$
        response.append("<message>" + message + "</message>"); //$NON-NLS-1$ //$NON-NLS-2$
        response.append("<details>" + details + "</details>"); //$NON-NLS-1$ //$NON-NLS-2$
        response.append("</error>"); //$NON-NLS-1$ 
        return Response.serverError().entity(response.toString()).type(MediaType.APPLICATION_XML).build();
	}

}
