/*******************************************************************************
 * Copyright (c) 2000, 2003 IBM Corporation and others.
 * All rights reserved. This program and the accompanying materials 
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     IBM Corporation - initial API and implementation
 *     MetaMatrix, Inc - repackaging and updates for use as a metadata store
 *******************************************************************************/
package org.teiid.internal.core.index;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * A safe subclass of RandomAccessFile, which ensure that it's closed
 * on finalize.
 */
public class SafeRandomAccessFile extends RandomAccessFile {
	public SafeRandomAccessFile(java.io.File file, String mode) throws java.io.IOException {
		super(file, mode);
	}
	public SafeRandomAccessFile(String name, String mode) throws java.io.IOException {
		super(name, mode);
	}
	protected void finalize() throws IOException {
		close();
	}
}
