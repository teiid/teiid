/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.util;

import java.io.*;
import java.util.*;

import com.metamatrix.common.CommonPlugin;

public class ByteArrayHelper {

    private static final String SEPARATOR = "-"; //$NON-NLS-1$
    private static final String ZERO_FILL = "0"; //$NON-NLS-1$

    /**
     * <p>The default size of each temporary byte array that is instantiated
     * to buffer data from an InputStream, in the
     * {@link #toByteArray(InputStream)}method.</p>
     *
     * <p>Ideally, this number
     * should be big enough that only one byte array is needed, but
     * small enough that wasted memory isn't allocated.  If the first
     * temp array is filled, then a second one of this size will be
     * created, and so on until all of the stream is read.</p>
     */
    public static final int CHUNK_SIZE = 32000;

    public static String toString(byte[] bytes) {
        if (bytes != null) {
            if (bytes.length == 0) {
                return ""; //$NON-NLS-1$
            }
            StringBuffer buf = new StringBuffer(bytes.length * 3 - 1);
            for (int i=0; i<bytes.length; i++) {
                // Insert a separator if specified
                if (i > 0) buf.append(SEPARATOR);
                // Get the hex value of the byte
                String hexValue = Integer.toHexString(bytes[i] & 0xFF);
                // Pad with a 0 if necessary
                if (hexValue.length() == 1) buf.append(ZERO_FILL);
                buf.append(hexValue.toUpperCase());
            }
            return buf.toString();
        }
        return null;
    }

    public static byte[] parse(String s) throws NumberFormatException {
        if (s != null) {
            // Tokenize the hex string into a sequence of byte values
            List byteArray = new ArrayList(s.length()/3 + 1);
            StringTokenizer tokenizer = new StringTokenizer(s, SEPARATOR);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (token.trim().length() != 2) {
                    throw new NumberFormatException(CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0019, token));
                }
                // Need to use Integer to parse the hex string as Byte.valueOf only parses 0 < hex < 128 due to sign
                byteArray.add(Integer.valueOf(token, 16));
            }
            // Convert the byte sequence to a byte array
            byte[] bytes = new byte[byteArray.size()];
            for (int i=0; i<byteArray.size(); i++) {
                bytes[i] = ((Integer) byteArray.get(i)).byteValue();
            }
            return bytes;
        }
        return null;
    }

    /**
     * <p>Reads data from the file and returns it as a
     * byte array.  The returned byte array is exactly filled with the data from the
     * InputStream, with no space left over.</p>
     *
     * @param file data to be converted to a byte array
     * @return byte array exactly filled with data; no leftover space
     * @throws IOException if there is an Exception reading from the InputStream
     */
    public static byte[] toByteArray(File file) throws IOException {
        
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            return ByteArrayHelper.toByteArray(fis);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
    }
    /**
     * <p>Reads binary data from the InputStream and returns it as a
     * byte array.  The InputStream is not closed in this method.
     * The returned byte array is exactly filled with the data from the
     * InputStream, with no space left over.</p>
     *
     * <p>If the amount of data in the input stream is known, use
     * {@link #toByteArray(InputStream, int) toByteArray(InputStream, int)}.
     * </p>
     * @param stream data to be converted to a byte array
     * @return byte array exactly filled with data; no leftover space
     * @throws IOException if there is an Exception reading from the InputStream
     */
    public static byte[] toByteArray(InputStream stream) throws IOException {
        return toByteArray(stream, CHUNK_SIZE);
    }

    /**
     * <p>Reads binary data from the InputStream and returns it as a
     * byte array.  The InputStream is not closed in this method.
     * The returned byte array is exactly filled with the data from the
     * InputStream, with no space left over.</p>
     *
     * <p>The chunkSize parameter controls the size of intermediate
     * byte array(s) that are used to buffer the stream data.
     * Ideally, this number
     * should be big enough that only one byte array is needed, but
     * small enough that wasted memory isn't allocated.  If the first
     * temp array is filled, then a second one of this size will be
     * created, and so on until all of the stream is read.  Then, data
     * will be copied into the final, correctly-size byte array which is
     * returned form this method.</p>
     *
     * <p>If the size of the input stream is known beforehand (for example,
     * if the size of a file represented by a FileInputStream is known), then
     * that size <i>plus one</i> should be passed in as the chunkSize.</p>
     *
     * <p>Implementation notes: If more than one intermediate byte array
     * is needed, an ArrayList is instantiated to hold the intermediate byte
     * arrays until all data is read from the stream.  Afterward, the
     * ArrayList is iterated through; the intermediate array(s) are
     * copied using System.arrayCopy into the final byte array.</p>
     *
     * @param stream data to be converted to a byte array
     * @param chunkSize size of intermediate byte array(s) to buffer data
     * @return byte array exactly filled with data; no leftover space
     * @throws IOException if there is an Exception reading from the InputStream
     */
    public static byte[] toByteArray(InputStream stream, int chunkSize) throws IOException {

        byte[] data = null;
        ArrayList dataArrays = null;

        //intermediate byte array(s) reference
        data= new byte[chunkSize];

        int pos= 0;
        int finalSize = 0;
        while (stream.available() > 0 ) {
            int n= stream.read(data, pos, data.length - pos);
            if (n>=0){ //n could equal -1 for some streams, indicating EOF
                pos += n;
            }

            if (data.length - pos == 0 ){
                if ( dataArrays == null){
                    dataArrays = new ArrayList();
                }
                dataArrays.add(data);
                data= new byte[chunkSize];
                pos = 0;
                finalSize = finalSize + chunkSize;
            }

        }

        finalSize = finalSize + pos;
        //final, correctly-sized byte array
        byte[] result = new byte[finalSize];
        int offSet = 0;

        if (dataArrays != null){
            Iterator i = dataArrays.iterator();
            byte[] tempArray = null;
            for (; i.hasNext(); offSet=offSet+chunkSize){
                tempArray = (byte[])i.next();
                System.arraycopy(tempArray,0,result,offSet,chunkSize);
            }
        }

        System.arraycopy(data,0,result,offSet,pos);

        return result;
    }


	/**
	 * Writes the byte array to the output stream.
	 */
    public static void toOutputStream(byte[] data, OutputStream outputStream) throws Exception {

        	ByteArrayInputStream bais = new ByteArrayInputStream(data);
        	InputStream isContent = new BufferedInputStream(bais);

            byte[] buff = new byte[2048];
            int bytesRead;

            // Simple read/write loop.
            while(-1 != (bytesRead = isContent.read(buff, 0, buff.length))) {
                outputStream.write(buff, 0, bytesRead);
            }

            outputStream.flush();
            outputStream.close();

    }
    
    /**
     * converts the byte array to an input stream 
     */
    public static InputStream toInputStream(byte[] data) throws Exception {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            InputStream isContent = new BufferedInputStream(bais);
            
            return isContent;



    }    

}
