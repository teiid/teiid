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
package org.teiid.olingo.web.gzip;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

/**
 * Request wrapper which wraps input stream to {@link GZIPInputStream}
 */
public class GzipMessageRequest extends HttpServletRequestWrapper{

    private ServletInputStream returnedStream = null;
    private BufferedReader returnedReader = null;
    private final Charset charset;

    /**
     * Constructs a request object wrapping the given request.
     *
     * @param request request
     *
     * @throws IllegalArgumentException if the request is null
     */
    public GzipMessageRequest(HttpServletRequest request){
        super(request);
        String cs = request.getCharacterEncoding();
        charset = cs == null || cs.isEmpty() ? Charset.defaultCharset() : Charset.forName(cs);
    }

    @Override
    public ServletInputStream getInputStream() throws IOException{
        if(returnedReader != null){
            throw new IllegalStateException("Method getReader() has already been called.");
        }
        if(returnedStream == null){
            returnedStream = new GzipServletInputStream(super.getInputStream());
        }
        return returnedStream;
    }

    @Override
    public BufferedReader getReader() throws IOException{
        if(returnedStream != null){
            throw new IllegalStateException("Method getInputStream() has already been called.");
        }
        if(returnedReader == null){
            // It does not make sense to call getReader from underlying request.
            // There are binary data. Reader could not return reasonable strings.
            returnedReader = new BufferedReader(new InputStreamReader(new GzipServletInputStream(super.getInputStream()), charset));
        }
        return returnedReader;
    }

    /**
     * Wraps input stream/reader to {@link GZIPInputStream}
     */
    private class GzipServletInputStream extends ServletInputStream{
        private final InputStream src;
        private final ServletInputStream origin;

        private GzipServletInputStream(ServletInputStream origin) throws IOException{
            src = new GZIPInputStream(origin);
            this.origin = origin;
        }

        @Override
        public boolean isFinished(){
            try{
                return src.available() == 0;
            } catch (IOException e){
                return true;
            }
        }

        @Override
        public boolean isReady(){
            return !isFinished();
        }

        @Override
        public void setReadListener(ReadListener readListener){
            origin.setReadListener(readListener);
        }

        @Override
        public int read() throws IOException{
            return src.read();
        }

        @Override
        public int read(byte[] b) throws IOException{
            return src.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException{
            return src.read(b, off, len);
        }

        @Override
        public void close() throws IOException{
            src.close();
            super.close();
        }
    }
}
