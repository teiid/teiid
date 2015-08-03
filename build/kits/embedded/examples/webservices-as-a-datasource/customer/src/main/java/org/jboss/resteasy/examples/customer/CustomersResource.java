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
package org.jboss.resteasy.examples.customer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import javax.ws.rs.core.MediaType;

@Path("/MyRESTApplication")
public class CustomersResource {
	
    private static final String XMLFILE = "customerList.xml";

    /**
     * 
     * @return
     */
    @GET
    @Path("customerList")
    @Produces({ MediaType.APPLICATION_XML })
	public String getCustomerList() {
    	
        System.out.println("*** Accessing /MyRESTApplication/customerList");
        
        StringBuffer fileContents = new StringBuffer();

		try {
			System.out.println("*** Read file: " + XMLFILE);
            
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(XMLFILE);
            
            BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
			try {
				String line = null;

				while ((line = input.readLine()) != null) {
					fileContents.append(line);
					fileContents.append(System.getProperty("line.separator"));
				}
			} finally {
                input.close();
                
                System.out.println("*** Completed reading file: " + XMLFILE);
            }
		} catch (IOException ex) {
			ex.printStackTrace();
		}
        
        System.out.println("*** Return file contents as application/xml");

        return fileContents.toString();
    }
}
