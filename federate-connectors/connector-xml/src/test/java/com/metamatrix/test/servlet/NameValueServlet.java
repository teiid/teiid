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

package com.metamatrix.test.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.util.ParameterParser;

public class NameValueServlet  extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public NameValueServlet() {
		super();
	}
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        System.out.println("NameValueServlet in doGet");
        if(null != req) {
			String queryString = req.getQueryString();
			ParameterParser parser = new ParameterParser();
			List pairs = parser.parse(queryString, '?');
			for(Iterator iter = pairs.iterator(); iter.hasNext(); ){
				NameValuePair pair = (NameValuePair) iter.next();
				if(pair.getName().equals("id")) {
					String id = pair.getName();
					StringBuffer buff = new StringBuffer();
					buff.append("<?xml version='1.0' ?>");
					//buff.append("<document>");
					buff.append("<Data>");
                    buff.append("<First>" + pair.getValue()+ "</First>");
                    buff.append("<Second>passed</Second>");
                    buff.append("</Data>");
                    //buff.append("</document>");
					writeResponse(resp.getWriter(), buff.toString());
				}
			}
		}
	}
	
	private void writeResponse(PrintWriter writer, String response) {
		writer.write(response);
		System.out.println(response);
		writer.flush();		
	}

	
	private StringBuffer setupReturn() {
		StringBuffer buff = new StringBuffer();
		buff.append("<?xml version='1.0' ?>");
		buff.append("<document xmlns:mmx='http://www.metamatrix.com>");
		buff.append("<mmx:return>");
		return buff;		
	}
}
