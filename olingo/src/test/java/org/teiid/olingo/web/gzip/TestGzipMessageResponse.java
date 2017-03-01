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
