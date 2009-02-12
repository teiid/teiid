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

package com.metamatrix.internal.core.xml.xsd;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.xml.CoreXmlPlugin;

public class XsdHeaderReader {
    
    private static final String XML_DECLARATION_PREFIX_STRING = "<?xml version="; //$NON-NLS-1$

    // ==================================================================================
    //                        C O N S T R U C T O R S
    // ==================================================================================

    public XsdHeaderReader() {
    }
    
    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    public static XsdHeader readHeader(InputStream istream) throws MetaMatrixCoreException {
        XsdHeaderReader reader = new XsdHeaderReader();
        return reader.read(istream); 
    }

    public static XsdHeader readHeader(File file) throws MetaMatrixCoreException {
        XsdHeaderReader reader = new XsdHeaderReader();
        return reader.read(file); 
    }

    /**
     * Read only the <XMI.header> section of the file and return the
     * <code>XMIHeader</code> object representing its contents
     * @param istream the InputStream from which we read the header
     * @return the XMIHeader object representing the contents of this section
     * @throws MetaMatrixException if there is an error reading from the stream
     */
    public XsdHeader read(InputStream istream) throws MetaMatrixCoreException {
        if (istream == null) {
            final String msg = CoreXmlPlugin.Util.getString("XsdHeaderReader.The_InputStream_reference_may_not_be_null._1"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }
        
        DefaultHandler handler = new TerminatingXsdHeaderContentHandler();
        try {
            Thread.currentThread().setContextClassLoader(XsdHeaderReader.class.getClassLoader());
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            SAXParser parser = spf.newSAXParser();
            parser.parse(new InputSource(istream), handler);
        } catch (SAXException e) {
            if (TerminatingXsdHeaderContentHandler.SCHEMA_FOUND_EXCEPTION_MESSAGE.equals(e.getMessage())) {
                // The schema information was successfully read
            } else if (TerminatingXsdHeaderContentHandler.SCHEMA_NOT_FOUND_EXCEPTION_MESSAGE.equals(e.getMessage())) {
                // The file is probably an XML file but not an XSD file
                return null;
            } else if (e instanceof SAXParseException) {
                // The file is probably a text file but not an XML file
                return null;
            }
        } catch (IOException e) {
            // The file is not a file that can be interpretted by the SAX parser
        } catch (Throwable e) {
            final String msg = CoreXmlPlugin.Util.getString("XsdHeaderReader.Error_in_parsing_file_1"); //$NON-NLS-1$
            throw new MetaMatrixCoreException(e, msg);
        }
        return ((TerminatingXsdHeaderContentHandler)handler).getXsdHeader();
    }

    /**
     * Read only the <XMI.header> section of the file and return the
     * <code>XMIHeader</code> object representing its contents
     * @param file the File from which we read the header
     * @return the XMIHeader object representing the contents of this section
     * @throws MetaMatrixException if there is an error reading the file
     */
    public XsdHeader read(File file) throws MetaMatrixCoreException {
        if (file == null) {
            final String msg = CoreXmlPlugin.Util.getString("XsdHeaderReader.The_file_reference_may_not_be_null_2"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }
        if (!file.exists()) {
            final Object[] params = new Object[]{file};
            final String msg = CoreXmlPlugin.Util.getString("XsdHeaderReader.The_file_0_does_not_exist_and_therefore_cannot_be_read._3",params); //$NON-NLS-1$
            throw new IllegalArgumentException(msg); 
        }
        if (!file.canRead()) {
            final Object[] params = new Object[]{file};
            final String msg = CoreXmlPlugin.Util.getString("XsdHeaderReader.The_file_0_does_not_have_read_privileges._4",params); //$NON-NLS-1$
            throw new IllegalArgumentException(msg); 
        }

        // If the file does not start with an XML declaration tag ...
        if (!isXmlFile(file)) {
            return null;
        }

        // Attempt to read the XML file and interpret it as an XMI file
        FileInputStream fis     = null;
		BufferedInputStream bis = null;
		try {
            fis = new FileInputStream(file);
		    bis = new BufferedInputStream(fis);
            return read(bis);
        } catch (FileNotFoundException e) {
            final String msg = CoreXmlPlugin.Util.getString("XsdHeaderReader.Error_in_parsing_file_1"); //$NON-NLS-1$
            throw new MetaMatrixCoreException(e, msg);
		} finally {
            if (bis != null) {
                try {
				    bis.close();
                } catch (IOException e) {
                }
            }
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                }
            }
		}
    }
    
    // ==================================================================================
    //                         P R I V A T E   M E T H O D S
    // ==================================================================================
    
    private static boolean isXmlFile(File file) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            byte[] buf = new byte[32];
            fis.read(buf);
            if (new String(buf).startsWith(XML_DECLARATION_PREFIX_STRING)) {
                return true;
            }
        } catch (IOException e) {
            // do nothing
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                }
            }
        }
        return false;
    }

}

