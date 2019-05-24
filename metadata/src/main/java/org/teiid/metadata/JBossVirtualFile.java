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

package org.teiid.metadata;

import java.io.IOException;
import java.io.InputStream;
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
import java.util.List;
import java.util.stream.Collectors;

import org.teiid.query.metadata.VirtualFile;

public class JBossVirtualFile implements VirtualFile {

    private org.jboss.vfs.VirtualFile file;

    public JBossVirtualFile(org.jboss.vfs.VirtualFile file) {
        this.file = file;
    }

    @Override
    public InputStream openStream() throws IOException {
        return file.openStream();
    }

    @Override
    public long getSize() {
        return file.getSize();
    }

    @Override
    public String getName() {
        return file.getName();
    }

    @Override
    public List<VirtualFile> getFileChildrenRecursively() throws IOException {
        return file.getChildrenRecursively().stream().filter(f -> f.isFile())
                .map(f -> new JBossVirtualFile(f)).collect(Collectors.toList());
    }

    @Override
    public boolean isFile() {
        return file.isFile();
    }

    @Override
    public String getPathName() {
        return file.getPathName();
    }

    @Override
    public VirtualFile getChild(String string) {
        return new JBossVirtualFile(this.file.getChild(string));
    }

    @Override
    public boolean exists() {
        return file.exists();
    }

}
