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

package com.metamatrix.test.servlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.util.ParameterParser;

import com.metamatrix.connector.xml.base.ProxyObjectFactory;

public class NameValueServlet  extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public NameValueServlet() {
		super();
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        System.out.println("NameValueServlet in doGet");
        if(null != req) {
			String queryString = req.getQueryString();
			System.out.println("NameValueServlet qstring = " + queryString);
			ParameterParser parser = new ParameterParser();
			List pairs = parser.parse(queryString, '?');
			for(Iterator iter = pairs.iterator(); iter.hasNext(); ){
				NameValuePair pair = (NameValuePair) iter.next();
				if(pair.getName().equals("id")) {
					File response = new File(ProxyObjectFactory.getDocumentsFolder() + "/purchaseOrdersShort.xml");
					FileInputStream stream = new FileInputStream(response);
					int data;
					while(true) {
						data = stream.read();
						if(-1 != data) {
							resp.getWriter().write(data);
						} else {
							resp.getWriter().flush();
							break;
						}
					}
					
							
				}
			}
		} else {
			System.out.println("NameValueServlet req is null");
		}
	}
}
