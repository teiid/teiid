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
import java.util.BitSet;
import java.util.Enumeration;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class EchoServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public EchoServlet() {
		super();
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		if(null != req) {
			boolean isSpecial = checkRequestForSpecialConditions(req);
			if(!isSpecial) {
				StringBuffer buff = setupReturn();
				String qs = req.getQueryString();
				qs = qs.replaceAll("&", "&amp;");
				buff.append(qs);
				appendReturnClosure(buff);
				writeResponse(resp.getWriter(), buff.toString());
			} else {
				resp.flushBuffer();
			}
		}
	}
	
	private void writeResponse(PrintWriter writer, String response) {
		writer.write(response);
		System.out.println(response);
		writer.flush();		
	}

	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		if(null != req) {
			boolean isSpecial = checkRequestForSpecialConditions(req);
			if(!isSpecial) {
					
				StringBuffer buff = setupReturn();
				Enumeration names = req.getParameterNames();
		        while(names.hasMoreElements()) {
		            String name = (String) names.nextElement();
		            String val = req.getParameter(name);
		            BitSet allowed = org.apache.commons.httpclient.URI.allowed_fragment;
		            val = org.apache.commons.httpclient.util.URIUtil.encode(val, allowed);
		            buff.append(name + " - " + val);
		        }
		        appendReturnClosure(buff);
		        writeResponse(resp.getWriter(), buff.toString());
			} else {
				resp.flushBuffer();
			}
		}

	}
	
	private StringBuffer setupReturn() {
		StringBuffer buff = new StringBuffer();
		buff.append("<?xml version='1.0' ?>");
		buff.append("<document xmlns:mmx='http://www.metamatrix.com'>");
		buff.append("<mmx:return>");
		return buff;		
	}

	private void appendReturnClosure(StringBuffer buff) {
		buff.append("</mmx:return>");
		buff.append("</document>");		
	}
	
	private boolean checkRequestForSpecialConditions(HttpServletRequest req) throws IOException {
		String product = req.getParameter("Product");
		if(product != null && product.equals("EmptyDoc")) {
			return true;
		}
		
		if(product != null && product.equals("ThrowException")) {
			throw new IOException("requested Exception");
		}
		return false;
	}
}
