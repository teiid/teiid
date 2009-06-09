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

package com.metamatrix.core.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.MetaMatrixCoreException;

public class ObjectConverterUtil {
    
    private static final int DEFAULT_READING_SIZE = 8192;

     protected static byte[] convertToByteArray(final java.sql.Blob data) throws MetaMatrixCoreException {
          try {
              // Open a stream to read the BLOB data
              InputStream l_blobStream = data.getBinaryStream();

              // Open a file stream to save the BLOB data
              ByteArrayOutputStream out = new ByteArrayOutputStream();
              BufferedOutputStream bos  = new BufferedOutputStream(out);

              // Read from the BLOB data input stream, and write to the file output stream
              byte[] l_buffer = new byte[1024]; // buffer holding bytes to be transferred
              int l_nbytes = 0;  // Number of bytes read
              while ((l_nbytes = l_blobStream.read(l_buffer)) != -1) // Read from BLOB stream
                bos.write(l_buffer,0,l_nbytes); // Write to file stream

              // Flush and close the streams
              bos.flush();
              bos.close();
              l_blobStream.close();

              return out.toByteArray();

          } catch (IOException ioe) {
                final Object[] params = new Object[]{data.getClass().getName()};
                throw new MetaMatrixCoreException(ioe,CorePlugin.Util.getString("ObjectConverterUtil.Error_translating_results_from_data_type_to_a_byte[]._1",params)); //$NON-NLS-1$
          } catch (SQLException sqe) {
                final Object[] params = new Object[]{data.getClass().getName()};
                throw new MetaMatrixCoreException(sqe,CorePlugin.Util.getString("ObjectConverterUtil.Error_translating_results_from_data_type_to_a_byte[]._2",params)); //$NON-NLS-1$
          }
    }

    public static byte[] convertToByteArray(final Object data) throws MetaMatrixCoreException, IOException {
        if (data instanceof InputStream) {
            return convertToByteArray((InputStream) data);
        } else if (data instanceof byte[]) {
            return (byte[]) data;
        } else if (data instanceof java.sql.Blob)  {
            return convertToByteArray((java.sql.Blob) data);
        }
        final Object[] params = new Object[]{data.getClass().getName()};
        throw new MetaMatrixCoreException(CorePlugin.Util.getString("ObjectConverterUtil.Object_type_not_supported_for_object_conversion._3",params)); //$NON-NLS-1$
    }


    public static byte[] convertToByteArray(final InputStream is) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BufferedOutputStream bos  = new BufferedOutputStream(out);

        byte[] l_buffer = new byte[1024]; // buffer holding bytes to be transferred
        int l_nbytes = 0;  // Number of bytes read
        while ((l_nbytes = is.read(l_buffer)) != -1) // Read from BLOB stream
          bos.write(l_buffer,0,l_nbytes); // Write to file stream

        bos.flush();
        bos.close();

        byte[] data = out.toByteArray();
        return data;
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

    public static void write(final InputStream is, final String fileName) throws Exception {
        File f = new File(fileName);
        f.delete();

        FileOutputStream fio = new FileOutputStream(f);
        BufferedOutputStream bos = new BufferedOutputStream(fio);

        byte[] buff = new byte[2048];
        int bytesRead;

        // Simple read/write loop.
        while (-1 != (bytesRead = is.read(buff, 0, buff.length))) {
            bos.write(buff, 0, bytesRead);
        }

        bos.flush();
        bos.close(); 
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

    public static InputStream convertToInputStream(final File file) {
        try {
            FileInputStream fis = new FileInputStream(file);
            InputStream isContent = new BufferedInputStream(fis);
            return isContent;
        } catch (FileNotFoundException fie) {
            final Object[] params = new Object[]{file.getName()};
            throw new IllegalArgumentException(CorePlugin.Util.getString("ObjectConverterUtil.File_is_not_found_4",params)); //$NON-NLS-1$
        }
    }
    /**
     * Returns the given bytes as a char array using a given encoding (null means platform default).
     */
    public static char[] bytesToChar(byte[] bytes, String encoding) throws IOException {

        return convertToCharArray(new ByteArrayInputStream(bytes), bytes.length, encoding);

    }
    /**
     * Returns the contents of the given file as a byte array.
     * @throws IOException if a problem occured reading the file.
     */
    public static byte[] convertFileToByteArray(File file) throws IOException {
        InputStream stream = null;
        try {
            stream = new BufferedInputStream(new FileInputStream(file));
            return convertToByteArray(stream, (int) file.length());
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * Returns the contents of the given file as a char array.
     * When encoding is null, then the platform default one is used
     * @throws IOException if a problem occured reading the file.
     */
    public static char[] convertFileToCharArray(File file, String encoding) throws IOException {
        InputStream stream = null;
        try {
            stream = new BufferedInputStream(new FileInputStream(file));
            return convertToCharArray(stream, (int) file.length(), encoding);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                }
            }
        }
    }
    
    /**
     * Returns the contents of the given file as a string.
     * @throws IOException if a problem occured reading the file.
     */
    public static String convertFileToString(final File file) throws IOException {
        return new String(convertFileToCharArray(file,null));
    }

    
    /**
     * Returns the contents of the given InputStream as a string.
     * @throws IOException if a problem occured reading the file.
     */
    public static String convertToString(final InputStream stream) throws IOException {
        return new String(convertToCharArray(stream, -1, null));
    }
    
    
    /**
     * Returns the given input stream's contents as a byte array.
     * If a length is specified (ie. if length != -1), only length bytes
     * are returned. Otherwise all bytes in the stream are returned.
     * Note this doesn't close the stream.
     * @throws IOException if a problem occured reading the stream.
     */
    public static byte[] convertToByteArray(InputStream stream, int length)
        throws IOException {
        byte[] contents;
        if (length == -1) {
            contents = new byte[0];
            int contentsLength = 0;
            int amountRead = -1;
            do {
                int amountRequested = Math.max(stream.available(), DEFAULT_READING_SIZE);  // read at least 8K
                
                // resize contents if needed
                if (contentsLength + amountRequested > contents.length) {
                    System.arraycopy(
                        contents,
                        0,
                        contents = new byte[contentsLength + amountRequested],
                        0,
                        contentsLength);
                }

                // read as many bytes as possible
                amountRead = stream.read(contents, contentsLength, amountRequested);

                if (amountRead > 0) {
                    // remember length of contents
                    contentsLength += amountRead;
                }
            } while (amountRead != -1); 

            // resize contents if necessary
            if (contentsLength < contents.length) {
                System.arraycopy(
                    contents,
                    0,
                    contents = new byte[contentsLength],
                    0,
                    contentsLength);
            }
        } else {
            contents = new byte[length];
            int len = 0;
            int readSize = 0;
            while ((readSize != -1) && (len != length)) {
                // See PR 1FMS89U
                // We record first the read size. In this case len is the actual read size.
                len += readSize;
                readSize = stream.read(contents, len, length - len);
            }
        }

        return contents;
    }

    /**
     * Returns the given input stream's contents as a character array.
     * If a length is specified (ie. if length != -1), only length chars
     * are returned. Otherwise all chars in the stream are returned.
     * Note this doesn't close the stream.
     * @throws IOException if a problem occured reading the stream.
     */
    public static char[] convertToCharArray(InputStream stream, int length, String encoding)
        throws IOException {
        InputStreamReader reader = null;
        reader = encoding == null
                    ? new InputStreamReader(stream)
                    : new InputStreamReader(stream, encoding);
        char[] contents;
        if (length == -1) {
            contents = new char[0];
            int contentsLength = 0;
            int amountRead = -1;
            do {
                int amountRequested = Math.max(stream.available(), DEFAULT_READING_SIZE);  // read at least 8K

                // resize contents if needed
                if (contentsLength + amountRequested > contents.length) {
                    System.arraycopy(
                        contents,
                        0,
                        contents = new char[contentsLength + amountRequested],
                        0,
                        contentsLength);
                }

                // read as many chars as possible
                amountRead = reader.read(contents, contentsLength, amountRequested);

                if (amountRead > 0) {
                    // remember length of contents
                    contentsLength += amountRead;
                }
            } while (amountRead != -1);

            // resize contents if necessary
            if (contentsLength < contents.length) {
                System.arraycopy(
                    contents,
                    0,
                    contents = new char[contentsLength],
                    0,
                    contentsLength);
            }
        } else {
            contents = new char[length];
            int len = 0;
            int readSize = 0;
            while ((readSize != -1) && (len != length)) {
                // See PR 1FMS89U
                // We record first the read size. In this case len is the actual read size.
                len += readSize;
                readSize = reader.read(contents, len, length - len);
            }
            // See PR 1FMS89U
            // Now we need to resize in case the default encoding used more than one byte for each
            // character
            if (len != length)
                System.arraycopy(contents, 0, (contents = new char[len]), 0, len);
        }

        return contents;
    }
    
    /**
     * Returns the contents of the given zip entry as a byte array.
     * @throws IOException if a problem occured reading the zip entry.
     */
    public static byte[] convertToByteArray(ZipEntry ze, ZipFile zip)
        throws IOException {

        InputStream stream = null;
        try {
            stream = new BufferedInputStream(zip.getInputStream(ze));
            return convertToByteArray(stream, (int) ze.getSize());
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                }
            }
        }
    }
    
    public static String convertToString(Reader reader) throws IOException {
    	return new String(convertToCharArray(reader, Integer.MAX_VALUE));
    }

    public static char[] convertToCharArray(Reader reader, int length) throws IOException {
        StringBuilder sb = new StringBuilder();        
        int chr = -1;
        for (int i = 0; i < length && (chr = reader.read()) != -1; i ++) {
            sb.append((char)chr);
        }
        return sb.toString().toCharArray();
    }

} 
