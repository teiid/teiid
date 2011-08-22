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
package org.teiid.jboss;

import java.io.*;

import org.jboss.logging.Logger;
import org.teiid.core.util.FileUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;


public class ObjectSerializer {
	
	private static final Logger log = Logger.getLogger(ObjectSerializer.class);

	private static final String ATTACHMENT_SUFFIX = ".ser"; //$NON-NLS-1$

	private String storagePath;
	
	public ObjectSerializer(String path) {
		this.storagePath = path;
	}
	
	public <T> T loadAttachment(File attachmentsStore, Class<T> expected) throws IOException, ClassNotFoundException {
		if (log.isTraceEnabled()) {
			log.trace("loadAttachment, attachmentsStore=" + attachmentsStore); //$NON-NLS-1$
		}

		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(attachmentsStore));
			return expected.cast(ois.readObject());
		} finally {
			if (ois != null) {
				ois.close();
			}
		}
	}

	public void saveAttachment(File attachmentsStore, Object attachment) throws IOException {
		if (log.isTraceEnabled()) {
			log.trace("saveAttachment, attachmentsStore=" + attachmentsStore + ", attachment=" + attachment); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(attachmentsStore));
			oos.writeObject(attachment);
		} finally {
			if (oos != null) {
				oos.close();
			}
		}
	}
	
	public boolean isStale(File cacheFile, long timeAfter) {
		return (cacheFile.exists() && timeAfter > cacheFile.lastModified());
	}
	
	public void removeAttachments(String fileName) {
		String dirName = baseDirectory(fileName);
		FileUtils.removeDirectoryAndChildren(new File(dirName));
	}

	public File getAttachmentPath(String fileName, String baseName) {
		
		String dirName = baseDirectory(fileName);

		final String vfsPath = baseName + ATTACHMENT_SUFFIX;
		File f = new File(dirName, vfsPath);
		if (!f.getParentFile().exists()) {
			f.getParentFile().mkdirs();
		}
		return f;
	}

	private String baseDirectory(String fileName) {
		String dirName = this.storagePath + File.separator + fileName + File.separator;
		return dirName;
	}
	
	public <T> T loadSafe(File cacheFile, Class<T> clazz) {
		try {
			if (cacheFile.exists()) {
				return clazz.cast(loadAttachment(cacheFile, clazz));
			}
			return null;
		} catch (Exception e) {
			LogManager.logWarning(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.getString("invalid_metadata_file", cacheFile.getAbsolutePath())); //$NON-NLS-1$
		}
		cacheFile.delete();
		return null;
	}	
}
