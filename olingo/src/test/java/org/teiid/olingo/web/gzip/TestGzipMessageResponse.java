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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class TestGzipMessageResponse{
    static final String TEST_STRING = "test string";
    static final byte[] TEST_STRING_IN_GZIP;
    static{
        try{
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gos = new GZIPOutputStream(bos);
            gos.write(TEST_STRING.getBytes());
            gos.finish();
            gos.flush();
            gos.close();
            TEST_STRING_IN_GZIP = bos.toByteArray();
        } catch (IOException ex){
            throw new InternalError("No exception expected: " + ex.toString());
        }
    }

    private GzipMessageResponse response;
    private ServletOutputStream stream;
    private List<Byte> streamBytes;

    @Before
    public void prepareResponse() throws IOException{
        streamBytes = new LinkedList<>();
        stream = new ServletOutputStream(){
            @Override public boolean isReady(){ return true; }
            @Override public void setWriteListener(WriteListener writeListener){}
            @Override public void write(int b) throws IOException{ streamBytes.add((byte)b); }
        };
        response = new GzipMessageResponse(mockResponse(stream));
    }

    @Test
    public void testIllegalGetOutputStreamInvocation() throws IOException{
        response.getWriter();
        try{
            response.getOutputStream();
            Assert.fail("Expected " + IllegalArgumentException.class);
        } catch (IllegalStateException ex){/* expected */}
    }

    @Test
    public void testIllegalGetWriterInvocation() throws IOException{
        response.getOutputStream();
        try{
            response.getWriter();
            Assert.fail("Expected " + IllegalArgumentException.class);
        } catch (IllegalStateException ex){/* expected */}
    }

    @Test
    public void testWriteToWriter() throws IOException{
        PrintWriter w = response.getWriter();
        w.write(TEST_STRING);
        w.close();
        Assert.assertArrayEquals("Expected output in GZIP.", TEST_STRING_IN_GZIP,
                ArrayUtils.toPrimitive(streamBytes.toArray(new Byte[streamBytes.size()])));
    }

    @Test
    public void testWriteToOutputStream() throws Exception{
        ServletOutputStream sos = response.getOutputStream();
        sos.write(TEST_STRING.getBytes());
        sos.close();
        Assert.assertArrayEquals("Expected output in GZIP.", TEST_STRING_IN_GZIP,
                ArrayUtils.toPrimitive(streamBytes.toArray(new Byte[streamBytes.size()])));
    }

    private static HttpServletResponse mockResponse(ServletOutputStream stream) throws IOException{
        HttpServletResponse out = Mockito.mock(HttpServletResponse.class);
        Mockito.doReturn(stream).when(out).getOutputStream();
        Mockito.doThrow(Error.class).when(out).getWriter();
        return out;
    }
}
