/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */
package org.teiid.internal.core.index;

import java.io.File;
import java.io.IOException;

import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.metadata.VDBResource;


public class VirtualRandomAccessFile {
    File indexFile;
    String mode;

    public VirtualRandomAccessFile(VDBResource file, String mode) throws IOException{
        this.indexFile = File.createTempFile(file.getName(), null);
        ObjectConverterUtil.write(file.openStream(), indexFile);
        this.mode = mode;
    }

    public SafeRandomAccessFile getSafeRandomAccessFile() throws IOException {
        return new SafeRandomAccessFile(indexFile, mode);
    }

    public void close() {
        indexFile.delete();
    }
}
