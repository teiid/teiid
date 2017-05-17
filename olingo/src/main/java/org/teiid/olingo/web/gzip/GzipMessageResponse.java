/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
