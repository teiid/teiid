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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.api.ConnectorLogger;


// Deletes a file when garbage collected. This allows a physical file
// to be removed during the garbage collection process. As long as everyone
// that references a file does so through its file deleter object
// it will get delewted as soon as it is no longer referenced.
public class FileLifeManager {
	private File file;
    private ConnectorLogger logger;
    private List openFiles;
	protected FileLifeManager(File file, ConnectorLogger logger) throws IOException
	{
		this.file = file.getCanonicalFile();
        this.logger = logger;
        if (logger != null) {
        	logger.logInfo("XML Connector Framework: Creating FileLifeManager to manage lifetime of file " + file.toString()); //$NON-NLS-1$
        }
        openFiles = new ArrayList();
	}

    public RandomAccessFile createRandomAccessFile() throws IOException
    {
    	RandomAccessFile openFile = new RandomAccessFile(file, "r"); //$NON-NLS-1$
        openFiles.add(openFile);
        return openFile;
    }
    
	private File getFile()
	{
		return file;
	}
    
    public boolean doesMatch(String filename) throws IOException
    {
        File otherFile = new File(filename).getCanonicalFile();

        // the next two lines are for debugging
        String thisFilename = file.toString();
        String otherFilename = otherFile.toString();

        return file.equals(otherFile);
    }

    public long getLength() throws IOException
    {
    	return file.length();
    }
    
    public void finalize()
    {
        if (logger != null) {
            logger.logInfo("XML Connector Framework: FileLifeManager is deleting file " + file.toString()); //$NON-NLS-1$
        }
        for (Iterator iter = openFiles.iterator(); iter.hasNext();) {
        	Object o = iter.next();
            RandomAccessFile openFile = (RandomAccessFile)o;
            try {
            	openFile.close();
            }
            catch (IOException e) {
                if (logger != null) {
                    logger.logInfo("XML Connector Framework: Unable to close file " + file.toString() + "."); //$NON-NLS-1$ //$NON-NLS-2$
                }
            }
        }
        boolean success = file.delete();
        if (logger != null) {
            logger.logInfo("XML Connector Framework: Delete of file " + (success ? "succeeded" : "failed")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }
}
