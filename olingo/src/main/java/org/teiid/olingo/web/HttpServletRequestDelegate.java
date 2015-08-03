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
package org.teiid.olingo.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;

import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.Part;

public class HttpServletRequestDelegate implements HttpServletRequest {
    protected HttpServletRequest delegate;

    public boolean isUserInRole(String s) {
        return delegate.isUserInRole(s);
    }

    public Principal getUserPrincipal() {
        return delegate.getUserPrincipal();
    }

    public HttpServletRequestDelegate(HttpServletRequest delegate) {
        this.delegate = delegate;
    }

    public String getAuthType() {
        return delegate.getAuthType();
    }

    public int getLocalPort() {
        return delegate.getLocalPort();
    }

    public Cookie[] getCookies() {
        return delegate.getCookies();
    }

    public long getDateHeader(String s) {
        return delegate.getDateHeader(s);
    }

    public String getHeader(String s) {
        return delegate.getHeader(s);
    }

    public Enumeration getHeaders(String s) {
        return delegate.getHeaders(s);
    }

    public Enumeration getHeaderNames() {
        return delegate.getHeaderNames();
    }

    public int getIntHeader(String s) {
        return delegate.getIntHeader(s);
    }

    public String getMethod() {
        return delegate.getMethod();
    }

    public String getPathInfo() {
        return delegate.getPathInfo();
    }

    public String getPathTranslated() {
        return delegate.getPathTranslated();
    }

    public String getContextPath() {
        return delegate.getContextPath();
    }

    public String getQueryString() {
        return delegate.getQueryString();
    }

    public String getRemoteUser() {
        return delegate.getRemoteUser();
    }

    public String getRequestedSessionId() {
        return delegate.getRequestedSessionId();
    }

    public String getRequestURI() {
        return delegate.getRequestURI();
    }

    public StringBuffer getRequestURL() {
        return delegate.getRequestURL();
    }

    public String getServletPath() {
        return delegate.getServletPath();
    }

    public HttpSession getSession(boolean b) {
        return delegate.getSession(b);
    }

    public HttpSession getSession() {
        return delegate.getSession();
    }

    public boolean isRequestedSessionIdValid() {
        return delegate.isRequestedSessionIdValid();
    }

    public boolean isRequestedSessionIdFromCookie() {
        return delegate.isRequestedSessionIdFromCookie();
    }

    public boolean isRequestedSessionIdFromURL() {
        return delegate.isRequestedSessionIdFromURL();
    }

    /**
     * @deprecated
     */
    public boolean isRequestedSessionIdFromUrl() {
        return delegate.isRequestedSessionIdFromUrl();
    }

    public Object getAttribute(String s) {
        return delegate.getAttribute(s);
    }

    public Enumeration getAttributeNames() {
        return delegate.getAttributeNames();
    }

    public String getCharacterEncoding() {
        return delegate.getCharacterEncoding();
    }

    public void setCharacterEncoding(String s)
            throws UnsupportedEncodingException {
        delegate.setCharacterEncoding(s);
    }

    public int getContentLength() {
        return delegate.getContentLength();
    }

    public String getContentType() {
        return delegate.getContentType();
    }

    public ServletInputStream getInputStream() throws IOException {
        return delegate.getInputStream();
    }

    public String getParameter(String s) {
        return delegate.getParameter(s);
    }

    public Enumeration getParameterNames() {
        return delegate.getParameterNames();
    }

    public String[] getParameterValues(String s) {
        return delegate.getParameterValues(s);
    }

    public Map getParameterMap() {
        return delegate.getParameterMap();
    }

    public String getProtocol() {
        return delegate.getProtocol();
    }

    public String getScheme() {
        return delegate.getScheme();
    }

    public String getServerName() {
        return delegate.getServerName();
    }

    public int getServerPort() {
        return delegate.getServerPort();
    }

    public BufferedReader getReader() throws IOException {
        return delegate.getReader();
    }

    public String getRemoteAddr() {
        return delegate.getRemoteAddr();
    }

    public String getRemoteHost() {
        return delegate.getRemoteHost();
    }

    public void setAttribute(String s, Object o) {
        delegate.setAttribute(s, o);
    }

    public void removeAttribute(String s) {
        delegate.removeAttribute(s);
    }

    public Locale getLocale() {
        return delegate.getLocale();
    }

    public Enumeration getLocales() {
        return delegate.getLocales();
    }

    public boolean isSecure() {
        return delegate.isSecure();
    }

    public RequestDispatcher getRequestDispatcher(String s) {
        return delegate.getRequestDispatcher(s);
    }

    /**
     * @deprecated
     */
    public String getRealPath(String s) {
        return delegate.getRealPath(s);
    }

    public int getRemotePort() {
        return delegate.getRemotePort();
    }

    public String getLocalName() {
        return delegate.getLocalName();
    }

    public String getLocalAddr() {
        return delegate.getLocalAddr();
    }

    @Override
    public AsyncContext getAsyncContext() {
        return delegate.getAsyncContext();
    }

    @Override
    public DispatcherType getDispatcherType() {
        return delegate.getDispatcherType();
    }

    @Override
    public ServletContext getServletContext() {
        return delegate.getServletContext();
    }

    @Override
    public boolean isAsyncStarted() {
        return delegate.isAsyncStarted();
    }

    @Override
    public boolean isAsyncSupported() {
        return delegate.isAsyncSupported();
    }

    @Override
    public AsyncContext startAsync() throws IllegalStateException {
        return delegate.startAsync();
    }

    @Override
    public AsyncContext startAsync(ServletRequest arg0, ServletResponse arg1)
            throws IllegalStateException {
        return delegate.startAsync(arg0, arg1);
    }

    @Override
    public boolean authenticate(HttpServletResponse arg0) throws IOException,
            ServletException {
        return delegate.authenticate(arg0);
    }

    @Override
    public Part getPart(String arg0) throws IOException, ServletException {
        return delegate.getPart(arg0);
    }

    @Override
    public Collection<Part> getParts() throws IOException, ServletException {
        return delegate.getParts();
    }

    @Override
    public void login(String arg0, String arg1) throws ServletException {
        delegate.login(arg0, arg1);
    }

    @Override
    public void logout() throws ServletException {
        delegate.logout();
    }
}
