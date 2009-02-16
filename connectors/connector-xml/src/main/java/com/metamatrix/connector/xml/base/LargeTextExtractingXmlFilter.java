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

package com.metamatrix.connector.xml.base;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;

class LargeTextExtractingXmlFilter extends XMLFilterImpl {
    LargeTextExtractingXmlFilter(int maxInMemoryStringSize,
                                 File cacheFolder,
                                 ConnectorLogger logger) {
        this.maxInMemoryStringSize = maxInMemoryStringSize;
        this.logger = logger;
        this.m_cacheFolder = cacheFolder;
    }

    // All the files created so far
    private Collection files = new ArrayList();
    
    int maxInMemoryStringSize;
    private File m_cacheFolder;

    ConnectorLogger logger;
    
    int totalChars = 0;

    List bufferedChars = null;

    Writer writer = null;

    private static class Characters {
        Characters(char[] ch, int start, int length, boolean ignorableWhitespace) {
            this.ch = new char[length];
            System.arraycopy(ch, start, this.ch, 0, length);
            this.ignorableWhitespace = ignorableWhitespace;
        }

        char[] ch;

        boolean ignorableWhitespace;
    }

    public void characters(char[] ch, int start, int length)
            throws SAXException {
        string(ch, start, length, false);
    }

    public void ignorableWhitespace(char[] ch, int start, int length)
            throws SAXException {
        string(ch, start, length, true);
    }

    public void startElement(String namespaceURI, String localName,
            String qName, Attributes atts) throws SAXException {
        element();
        super.startElement(namespaceURI, localName, qName, atts);
    }

    public void endElement(String namespaceURI, String localName, String qName)
            throws SAXException {
        element();
        super.endElement(namespaceURI, localName, qName);
    }

    protected void string(char[] ch, int start, int length,
            boolean ignorableWhitespace) throws SAXException {
        try {
            // Check to see if we pass the threshold that causes us to write
            // text out to a file
            if (writer == null) {
                if (totalChars + length > maxInMemoryStringSize) {
                    File file = File.createTempFile("xmlclob", ".txt", m_cacheFolder);
                    file.deleteOnExit();
                    FileLifeManager fileLifeManager = new FileLifeManager(file, logger);
                    files.add(fileLifeManager);

                    String fileReference = getXmlStringForFile(file);
                    super.characters(fileReference.toCharArray(), 0, fileReference.length());

                    FileOutputStream stream = new FileOutputStream(file);
                    writer = new OutputStreamWriter(stream,
                            FileBackedValueReference.getEncoding());

                    if(bufferedChars != null) {
                    	for (Iterator iter = bufferedChars.iterator(); iter
                    		.hasNext();) {
                    		Characters characters = (Characters) iter.next();
                    		writer.write(characters.ch);
                    	}

                    	// Do we care that we lose the ignorable whitespace info
                    	// here?
                    	bufferedChars = null;
                    }
                }
            }

            // Write the new text to a file, or add it to the stored up
            if (writer == null) {
                if (bufferedChars == null) {
                    bufferedChars = new ArrayList();
                }
                bufferedChars.add(new Characters(ch, start, length, false));
            } else {
                writer.write(ch, start, length);
            }
            totalChars += length;
        } catch (IOException e) {
            throw new SAXException(e);
        }
    }

    protected void element() throws SAXException {
        try {
            if (bufferedChars != null) {
                // We may have character data that we have buffered up; we
                // decided not to write it
                // out to a file, so we should pass it on to the content handler
                // now.
                for (Iterator iter = bufferedChars.iterator(); iter.hasNext();) {
                    Characters characters = (Characters) iter.next();
                    if (characters.ignorableWhitespace) {
                        super.ignorableWhitespace(characters.ch, 0,
                                characters.ch.length);
                    } else {
                        super.characters(characters.ch, 0,
                                        characters.ch.length);
                    }
                }
                bufferedChars = null;
            } else if (writer != null) {
                writer.flush();
                writer.close();
                writer = null;
            }
            totalChars = 0;
        } catch (IOException e) {
            throw new SAXException(e);
        }
    }

    public static String getXmlStringForFile(File file) {
        String filename = file.getAbsolutePath();
        String retval = magicPrefix + filename + "." + file.length();
        return retval;
    }

    public static LargeOrSmallString stringOrValueReference(String str, XMLDocument doc)
            throws ConnectorException {
        if (str == null) {
            return null;
        }
        
        if (!str.startsWith(magicPrefix)) {
            return LargeOrSmallString.createSmallString(str);
        }

        String filenamePlusLength = str.substring(magicPrefix.length());
        int lastDot = str.lastIndexOf('.');

        if (lastDot == -1) {
            return LargeOrSmallString.createSmallString(str);
        }

        long size = Long.valueOf(str.substring(lastDot + 1)).longValue();
        String filename = str.substring(magicPrefix.length(), lastDot);
        FileLifeManager[] files = doc.getExternalFiles();
        FileLifeManager fileLifeManager = null;
        for (int iFile = 0; iFile < files.length; iFile++) {
            try {
            	if (files[iFile].doesMatch(filename)) {
            		fileLifeManager = files[iFile];
            		break;
            	}
            }
            catch (IOException e) {
            	// ignore; just treat it as not found
            }
        }
        
        if (fileLifeManager == null) {
            return LargeOrSmallString.createSmallString(str);
        }

        try {
            LargeTextValueReference valueReference = new FileBackedValueReference(fileLifeManager);
            LargeOrSmallString retval = LargeOrSmallString.createLargeString(valueReference);
            return retval;
        } catch (IOException e) {
            throw new ConnectorException(e);
        }
    }

	FileLifeManager[] getFiles() {
        FileLifeManager[] retval = new FileLifeManager[files.size()];
        files.toArray(retval);
		return retval;
	}

	// A magic number that will tell the connector that the text is stored in a
    // file, instead
    // of directly in the XML
    private static final String magicPrefix = "{c0b03165-e8a8-11d9-9485-00e08161165f}+{F930BF8D-407F-4D37-B661-AEEFE717B01A}+";
}