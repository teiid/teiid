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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Enumeration;
import java.util.ResourceBundle;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.util.URIUtil;

public class MockResponseServlet extends HttpServlet
{

    public MockResponseServlet()
    {
    }

    @Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws IOException
    {
        logRequest(req);
        handleRequestResponse(req, resp);
    }

    @Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws IOException
    {
        doGet(req, resp);
    }

    private void logRequest(HttpServletRequest req)
        throws IOException
    {
        System.out.println("query string: " + req.getQueryString());
        Enumeration paramEnum = req.getParameterNames();
        System.out.println("Start params");
        String name;
        for(; paramEnum.hasMoreElements(); System.out.println("name: " + name + " value: " + req.getParameter(name)))
            name = (String)paramEnum.nextElement();

        System.out.println("end params");
        InputStream stream = req.getInputStream();
        InputStreamReader reader = new InputStreamReader(stream);
        writeCharStreams(reader, new OutputStreamWriter(System.out));
    }

    private void handleRequestResponse(HttpServletRequest req, HttpServletResponse resp)
        throws IOException
    {
        if(req.getParameter("mm.echo") != null)
        {
            echoRequest(req, resp);
        } else
        {
            if(req.getParameter("mm.err") != null)
                throw new IOException("RequestedException");
            sendResponse(resp);
        }
    }

    private void sendResponse(HttpServletResponse resp)
        throws IOException
    {
        String file = ResourceBundle.getBundle("com.metamatrix.test.servlet.MockResponseServletProps").getString("mock.response.file");
        InputStream fis = getClass().getResourceAsStream(file);
        InputStreamReader reader = new InputStreamReader(fis);
        writeCharStreams(reader, resp.getWriter());
        resp.getWriter().flush();
        fis.close();
    }

    private void writeCharStreams(Reader in, Writer out)
        throws IOException
    {
        char buff[];
        int read;
        for(; in.ready(); out.write(buff, 0, read))
        {
            buff = new char[512];
            read = in.read(buff);
        }

    }

    private void echoRequest(HttpServletRequest req, HttpServletResponse resp)
        throws IOException
    {
        StringBuffer buff = setupReturn();
        encodeParams(req, buff);
        encodeBody(req, buff);
        appendReturnClosure(buff);
        writeResponse(resp.getWriter(), buff.toString());
    }

    private void encodeParams(HttpServletRequest req, StringBuffer buff)
        throws IOException
    {
        buff.append("<parameter-list>");
        String name;
        String val;
        for(Enumeration names = req.getParameterNames(); names.hasMoreElements(); buff.append("<parameter name='" + name + "' value='" + val + "' />"))
        {
            name = (String)names.nextElement();
            val = req.getParameter(name);
            java.util.BitSet allowed = URI.allowed_fragment;
            val = URIUtil.encode(val, allowed);
        }

        buff.append("</parameter-list>");
    }

    private void encodeBody(HttpServletRequest req, StringBuffer buff)
        throws IOException
    {
        InputStreamReader reader = new InputStreamReader(req.getInputStream());
        StringBuffer bodyBuffer = new StringBuffer();
        java.util.BitSet allowed = URI.allowed_fragment;
        char buffArr[];
        int read;
        for(; reader.ready(); bodyBuffer.append(buffArr, 0, read))
        {
            buffArr = new char[512];
            read = reader.read(buffArr);
        }

        String body = bodyBuffer.toString();
        body = URIUtil.encode(body, allowed);
        buff.append("<request-body>");
        buff.append(body);
        buff.append("</request-body>");
    }

    private StringBuffer setupReturn()
    {
        StringBuffer buff = new StringBuffer();
        buff.append("<?xml version='1.0' ?>");
        buff.append("<document xmlns='http://www.metamatrix.com/connector/xml/test'>");
        buff.append("<return>");
        return buff;
    }

    private void appendReturnClosure(StringBuffer buff)
    {
        buff.append("</return>");
        buff.append("</document>");
    }

    private void writeResponse(PrintWriter writer, String response)
        throws IOException
    {
        writer.write(response);
        System.out.println(response);
        writer.flush();
    }

    static final long serialVersionUID = 1L;
    private static final String MM_ECHO = "mm.echo";
    private static final String BUNDLE = "com.metamatrix.connector.xml.test.MockResponseServletProps";
    private static final String FILE_KEY = "mock.response.file";
    private static final String MM_ERR = "mm.err";
}
