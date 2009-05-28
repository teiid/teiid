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
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DocPostBodyServlet  extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public DocPostBodyServlet() {
		super();
	}
	
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        System.out.println("DocNameValueServlet in doGet");
        if(null != req) {
			String queryString = req.getQueryString();
			if(null != queryString) {
                writeResponse(resp.getWriter(), "Query string should be null");
                return;
            } 
            String theDoc = req.getReader().readLine();
            if(!theDoc.startsWith("theParam")) {
                writeResponse(resp.getWriter(), "cannot find theParam");
                return;
            }
            StringBuffer buff = new StringBuffer();
            buff.append("<?xml version='1.0' ?>");
            buff.append("<Data>");
            buff.append("<First></First>");
            buff.append("<Second>passed</Second>");
            buff.append("</Data>");
            writeResponse(resp.getWriter(), buff.toString());
		} else {
            writeResponse(resp.getWriter(), "The request was null");
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
