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

import java.io.IOException;
import java.util.BitSet;
import java.util.Enumeration;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LoggingServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public LoggingServlet() {
		super();
	}
	
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		if(null != req) {
			System.out.println("Start headers");
			Enumeration headers = req.getHeaderNames();
			while(headers.hasMoreElements()) {
				String name = (String) headers.nextElement();
	            String val = req.getParameter(name);
	            System.out.println(name + "-" + val);
			}
			System.out.println("End headers");
			System.out.println("Start parameters");
			Enumeration names = req.getParameterNames();
	        while(names.hasMoreElements()) {
	            String name = (String) names.nextElement();
	            String val = req.getParameter(name);
	            BitSet allowed = org.apache.commons.httpclient.URI.allowed_fragment;
	            val = org.apache.commons.httpclient.util.URIUtil.encode(val, allowed);
	            System.out.println(name + " - " + val);
	        }
	        System.out.println("End parameters");
		}
	}
}
