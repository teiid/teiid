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

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.zip.GZIPOutputStream;

/**
 * Response wrapper which wraps output stream to {@link GZIPOutputStream}
 */
public class GzipMessageResponse extends HttpServletResponseWrapper{
    private ServletOutputStream returnedStream;
    private PrintWriter returnedWriter;
    private final Charset charset;


    /**
     * Constructs a response adaptor wrapping the given response.
     *
     * @param response response
     *
     * @throws IllegalArgumentException if the response is null
     */
    public GzipMessageResponse(HttpServletResponse response){
        super(response);
        String cs = response.getCharacterEncoding();
        charset = cs == null || cs.isEmpty() ? Charset.defaultCharset() : Charset.forName(cs);
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException{
        if(returnedWriter != null){
            throw new IllegalStateException("Method getWriter() has already been called.");
        }
        if(returnedStream == null){
            returnedStream = new GzipServletOutputStream(super.getOutputStream());
        }
        return returnedStream;
    }

    @Override
    public PrintWriter getWriter() throws IOException{
        if(returnedStream != null){
            throw new IllegalStateException("Method getOutputStream() has already been called.");
        }
        if(returnedWriter == null){
            // It does not make sense to write to Writer.
            // We will write binary data which
            returnedWriter = new PrintWriter(new OutputStreamWriter(new GzipServletOutputStream(super.getOutputStream()), charset));
        }
        return returnedWriter;
    }

    /**
     * Wraps output stream/writer to {@link GZIPOutputStream}
     */
    private class GzipServletOutputStream extends ServletOutputStream{
        private final OutputStream dest;
        private final ServletOutputStream origin;

        private GzipServletOutputStream(ServletOutputStream origin) throws IOException{
            dest = new GZIPOutputStream(origin);
            this.origin = origin;
        }

        @Override
        public void write(int b) throws IOException{
            dest.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException{
            dest.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException{
            dest.write(b, off, len);
        }

        @Override
        public void close() throws IOException{
            dest.close();
            super.close();
        }

        @Override
        public boolean isReady(){
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener){
            if(origin != null){
                origin.setWriteListener(writeListener);
            }
        }
    }
}
