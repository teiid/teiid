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

package org.teiid.core.util;

import java.io.*;
import java.io.InputStreamReader;
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
                  throw new TeiidException(CorePlugin.Event.TEIID10030, ioe,CorePlugin.Util.gs(CorePlugin.Event.TEIID10030,params));
          } catch (SQLException sqe) {
                final Object[] params = new Object[]{data.getClass().getName()};
                  throw new TeiidException(CorePlugin.Event.TEIID10030, sqe,CorePlugin.Util.gs(CorePlugin.Event.TEIID10030,params));
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
          throw new TeiidException(CorePlugin.Event.TEIID10032, CorePlugin.Util.gs(CorePlugin.Event.TEIID10032,params));
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
	        int l_nbytes = 0;  
	        int count = 0;
	        int readLength = length;
	        if (length == -1) {
	        	readLength = l_buffer.length;
	        }
	        else {
	        	readLength = Math.min(length, l_buffer.length);
	        }
	        while (readLength > 0 && (l_nbytes = is.read(l_buffer, count, readLength)) != -1) {
	        	if (l_nbytes == 0) {
	        		continue;
	        	}
	        	count += l_nbytes;
	        	if (count >= l_buffer.length || (length > -1 && count + writen >= length)) {
	        		out.write(l_buffer, 0, count);
	        		writen += count;
	        		count = 0;
	        	}
	        	if (length != -1) {
	        		readLength = Math.min(length - writen, l_buffer.length - count);
	        	} else {
	        		readLength = l_buffer.length - count;
	        	}
	        }
	        if (count > 0) {
        		out.write(l_buffer, 0, count);
        		writen += count;
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
    
    public static int write(final Writer out, final Reader is, int length, boolean close) throws IOException {
    	int writen = 0;
        try {
	        char[] l_buffer = new char[DEFAULT_READING_SIZE]; // buffer holding bytes to be transferred
	        int l_nbytes = 0;  
	        int count = 0;
	        int readLength = length;
	        if (length == -1) {
	        	readLength = l_buffer.length;
	        }
	        else {
	        	readLength = Math.min(length, l_buffer.length);
	        }
	        while (readLength > 0 && (l_nbytes = is.read(l_buffer, count, readLength)) != -1) {
	        	if (l_nbytes == 0) {
	        		continue;
	        	}
	        	count += l_nbytes;
	        	if (count >= l_buffer.length || (length > -1 && count + writen >= length)) {
	        		out.write(l_buffer, 0, count);
	        		writen += count;
	        		count = 0;
	        	}
	        	if (length != -1) {
	        		readLength = Math.min(length - writen, l_buffer.length - count);
	        	} else {
	        		readLength = l_buffer.length - count;
	        	}
	        }
	        if (count > 0) {
        		out.write(l_buffer, 0, count);
        		writen += count;
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
    	OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(f), "UTF-8");        
        write(fw, reader, -1, true);   
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
        return new String(convertFileToCharArray(file,"UTF-8")); 
    }

    
    /**
     * Returns the contents of the given InputStream as a string.
     * @throws IOException if a problem occurred reading the file.
     */
    public static String convertToString(final InputStream stream) throws IOException {
        return new String(convertToCharArray(stream, -1, "UTF-8")); //$NON-NLS-1$
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
        write(sb, reader, length, true);
        return sb.toString().toCharArray();
    }

} 
