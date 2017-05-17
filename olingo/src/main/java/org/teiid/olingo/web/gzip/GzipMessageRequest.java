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
