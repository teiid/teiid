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
