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

package org.teiid.core.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidException;


public class ObjectConverterUtil {
    
    private static final int DEFAULT_READING_SIZE = 8192;

     protected static byte[] convertBlobToByteArray(final java.sql.Blob data) throws TeiidException {
          try {
              // Open a stream to read the BLOB data
              InputStream l_blobStream = data.getBinaryStream();
              return convertToByteArray(l_blobStream);
          } catch (IOException ioe) {
                final Object[] params = new Object[]{data.getClass().getName()};
                throw new TeiidException(ioe,CorePlugin.Util.getString("ObjectConverterUtil.Error_translating_results_from_data_type_to_a_byte[]._1",params)); //$NON-NLS-1$
          } catch (SQLException sqe) {
                final Object[] params = new Object[]{data.getClass().getName()};
                throw new TeiidException(sqe,CorePlugin.Util.getString("ObjectConverterUtil.Error_translating_results_from_data_type_to_a_byte[]._2",params)); //$NON-NLS-1$
          }
    }

    public static byte[] convertToByteArray(final Object data) throws TeiidException, IOException {
        if (data instanceof InputStream) {
            return convertToByteArray((InputStream) data);
        } else if (data instanceof byte[]) {
            return (byte[]) data;
        } else if (data instanceof java.sql.Blob)  {
            return convertBlobToByteArray((java.sql.Blob) data);
        } else if (data instanceof File) {
        	return convertFileToByteArray((File)data);
        }
        final Object[] params = new Object[]{data.getClass().getName()};
        throw new TeiidException(CorePlugin.Util.getString("ObjectConverterUtil.Object_type_not_supported_for_object_conversion._3",params)); //$NON-NLS-1$
    }

    public static byte[] convertToByteArray(final InputStream is) throws IOException {
    	return convertToByteArray(is, -1);
    }
    	
    /**
     * Returns the given input stream's contents as a byte array.
     * If a length is specified (ie. if length != -1), only length bytes
     * are returned. Otherwise all bytes in the stream are returned.
     * Note this does close the stream, even if not all bytes are written, 
     * because the buffering does not guarantee the end position.
     * @throws IOException if a problem occurred reading the stream.
     */
    public static byte[] convertToByteArray(final InputStream is, int length) throws IOException {
    	return convertToByteArray(is, length, true);
    }
    
    public static byte[] convertToByteArray(final InputStream is, int length, boolean close) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, is, length, close);
        out.close();
        return out.toByteArray();    	
    }

    public static int write(final OutputStream out, final InputStream is, byte[] l_buffer, int length) throws IOException {
        return write(out, is, l_buffer, length, true);
    }
    
    public static int write(final OutputStream out, final InputStream is, byte[] l_buffer, int length, boolean close) throws IOException {
    	int writen = 0;
        try {
	        int l_nbytes = 0;  // Number of bytes read
	        int readLength = length;
	        if (length == -1) {
	        	readLength = l_buffer.length;
	        }
	        else {
	        	readLength = Math.min(length, l_buffer.length);
	        }
	        while ((l_nbytes = is.read(l_buffer, 0, readLength)) != -1) {
	        	if (length != -1 && writen > length - l_nbytes) {
		        	out.write(l_buffer, 0, writen + l_nbytes - length); 
		        	break;
	        	}
	        	out.write(l_buffer,0,l_nbytes); 
	        	writen += l_nbytes;
	        }
	        return writen;
        } finally {
        	if (close) {
	        	try {
	       			is.close();
	        	} finally {
	        		out.close();
	        	}
        	}
        }
    }

    public static int write(final OutputStream out, final InputStream is, int length) throws IOException {
       return write(out, is, length, true);    	
    }    
    
    public static int write(final OutputStream out, final InputStream is, int length, boolean close) throws IOException {
    	return write(out, is, new byte[DEFAULT_READING_SIZE], length, close); // buffer holding bytes to be transferred
    }
    
    public static void write(final Writer out, final Reader is, int length) throws IOException {
    	int writen = 0;
        try {
	        char[] l_buffer = new char[DEFAULT_READING_SIZE]; // buffer holding bytes to be transferred
	        int l_nbytes = 0;  // Number of bytes read
	        while ((l_nbytes = is.read(l_buffer)) != -1) {
	        	if (length != -1 && writen > length - l_nbytes) {
		        	out.write(l_buffer, 0, writen + l_nbytes - length); 
		        	break;
	        	}
	        	out.write(l_buffer,0,l_nbytes); 
	        	writen += l_nbytes;
	        }
        } finally {
        	try {
        		is.close();
        	} finally {
        		out.close();
        	}
        }
    }

    public static InputStream convertToInputStream(byte[] data) {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        InputStream isContent = new BufferedInputStream(bais);
        return isContent;
    }

    public static InputStream convertToInputStream(final String data) {
        return convertToInputStream(data.getBytes());
    }
    
    public static InputStream convertToInputStream(final char[] data) {
        return convertToInputStream(new String(data));
    }

    public static void write(final InputStream is, final String fileName) throws IOException {
        File f = new File(fileName);
        write(is, f);
    }
    
    public static void write(final Reader reader, final String fileName) throws IOException {
        File f = new File(fileName);
        write(reader, f);
    }    
    
    public static void write(final Reader reader, final File f) throws IOException {
    	f.getParentFile().mkdirs();
    	FileWriter fw = new FileWriter(f);        
        write(fw, reader, -1);   
    }

    public static void write(final InputStream is, final File f) throws IOException {
    	f.getParentFile().mkdirs();
        FileOutputStream fio = new FileOutputStream(f);
        BufferedOutputStream bos = new BufferedOutputStream(fio);
    	write(bos, is, -1);
    }
    
    public static void write(byte[] data, final String fileName) throws Exception {
        InputStream is = ObjectConverterUtil.convertToInputStream(data);
        ObjectConverterUtil.write(is, fileName);
        is.close();
    }
    
    public static void write(char[] data, final String fileName) throws Exception {
        InputStream is = ObjectConverterUtil.convertToInputStream(data);
        ObjectConverterUtil.write(is, fileName);
        is.close();
    }

    /**
     * Returns the given bytes as a char array using a given encoding (null means platform default).
     */
    public static char[] bytesToChar(byte[] bytes, String encoding) throws IOException {

        return convertToCharArray(new ByteArrayInputStream(bytes), bytes.length, encoding);

    }
    /**
     * Returns the contents of the given file as a byte array.
     * @throws IOException if a problem occurred reading the file.
     */
    public static byte[] convertFileToByteArray(File file) throws IOException {
        return convertToByteArray(new FileInputStream(file), (int) file.length());
    }

    /**
     * Returns the contents of the given file as a char array.
     * When encoding is null, then the platform default one is used
     * @throws IOException if a problem occurred reading the file.
     */
    public static char[] convertFileToCharArray(File file, String encoding) throws IOException {
        InputStream stream = new FileInputStream(file);
        return convertToCharArray(stream, (int) file.length(), encoding);
    }
    
    /**
     * Returns the contents of the given file as a string.
     * @throws IOException if a problem occurred reading the file.
     */
    public static String convertFileToString(final File file) throws IOException {
        return new String(convertFileToCharArray(file,null));
    }

    
    /**
     * Returns the contents of the given InputStream as a string.
     * @throws IOException if a problem occurred reading the file.
     */
    public static String convertToString(final InputStream stream) throws IOException {
        return new String(convertToCharArray(stream, -1, null));
    }
    
    /**
     * Returns the given input stream's contents as a character array.
     * If a length is specified (ie. if length != -1), only length chars
     * are returned. Otherwise all chars in the stream are returned.
     * Note this doesn't close the stream.
     * @throws IOException if a problem occurred reading the stream.
     */
    public static char[] convertToCharArray(InputStream stream, int length, String encoding)
        throws IOException {
    	Reader r = null;
    	if (encoding == null) {
    		r = new InputStreamReader(stream);
    	} else {
    		r = new InputStreamReader(stream, encoding);
    	}
        return convertToCharArray(r, length);
    }
    
    /**
     * Returns the contents of the given zip entry as a byte array.
     * @throws IOException if a problem occurred reading the zip entry.
     */
    public static byte[] convertToByteArray(ZipEntry ze, ZipFile zip)
        throws IOException {
        return convertToByteArray(zip.getInputStream(ze), (int) ze.getSize());
    }
    
    public static String convertToString(Reader reader) throws IOException {
    	return new String(convertToCharArray(reader, Integer.MAX_VALUE));
    }

    public static char[] convertToCharArray(Reader reader, int length) throws IOException {
        StringWriter sb = new StringWriter();     
        write(sb, reader, length);
        return sb.toString().toCharArray();
    }

} 
