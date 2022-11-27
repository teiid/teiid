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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.CodeSigner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.jboss.vfs.VFS;
import org.jboss.vfs.VFSUtils;
import org.jboss.vfs.VirtualFile;
import org.jboss.vfs.spi.FileSystem;
import org.jboss.vfs.util.PathTokenizer;
import org.teiid.common.buffer.AutoCleanupUtil;
import org.teiid.common.buffer.AutoCleanupUtil.Removable;

/**
 * <p>
 * This implementation is backed by a zip file.  The provided file must be owned by this instance; otherwise, if the
 * file disappears unexpectedly, the filesystem will malfunction.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public final class PureZipFileSystem implements FileSystem {

    private static AtomicInteger counter = new AtomicInteger();

    public static VirtualFile mount(URL url) throws IOException, URISyntaxException {
        //we treat each zip as unique if it's possible that it
        String fileName = "teiid-vdb-" + url.toExternalForm(); //$NON-NLS-1$
        VirtualFile root = VFS.getChild(fileName);
        File f = new File(url.toURI());
        if (root.exists()) {
            long lastModified = f.lastModified();
            if (root.getLastModified() != lastModified) {
                fileName += counter.get();
                //TODO: should check existence
                root = VFS.getChild(fileName);
            }
        }
        synchronized (PureZipFileSystem.class) {
            if (!root.exists()) {
                final Closeable c = VFS.mount(root, new PureZipFileSystem(f));
                //in theory we don't need to track the closable as we're not using any resources
                AutoCleanupUtil.setCleanupReference(root, new Removable() {

                    @Override
                    public void remove() {
                        try {
                            c.close();
                        } catch (IOException e) {
                        }
                    }
                });
            }
        }
        return root;
    }

    private final JarFile zipFile;
    private final File archiveFile;
    private final long zipTime;
    private final ZipNode rootNode;

    /**
     * Create a new instance.
     *
     * @param archiveFile the original archive file
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    public PureZipFileSystem(File archiveFile) throws IOException {
        zipTime = archiveFile.lastModified();
        final JarFile zipFile;
        this.zipFile = zipFile = new JarFile(archiveFile);
        this.archiveFile = archiveFile;
        final Enumeration<? extends JarEntry> entries = zipFile.entries();
        final ZipNode rootNode = new ZipNode(new HashMap<String, ZipNode>(), "", null);
        FILES:
        for (JarEntry entry : iter(entries)) {
            final String name = entry.getName();
            final boolean isDirectory = entry.isDirectory();
            final List<String> tokens = PathTokenizer.getTokens(name);
            ZipNode node = rootNode;
            final Iterator<String> it = tokens.iterator();
            while (it.hasNext()) {
                String token = it.next();
                if (PathTokenizer.isCurrentToken(token) || PathTokenizer.isReverseToken(token)) {
                    // invalid file name
                    continue FILES;
                }
                final Map<String, ZipNode> children = node.children;
                if (children == null) {
                    // todo - log bad zip entry
                    continue FILES;
                }
                ZipNode child = children.get(token);
                if (child == null) {
                    child = it.hasNext() || isDirectory ? new ZipNode(new HashMap<String, ZipNode>(), token, null) : new ZipNode(null, token, entry);
                    children.put(token, child);
                }
                node = child;
            }
        }
        this.rootNode = rootNode;
    }

    private static <T> Iterable<T> iter(final Enumeration<T> entries) {
        return Collections.list(entries);
    }

    /** {@inheritDoc} */
    public File getFile(VirtualFile mountPoint, VirtualFile target) throws IOException {
        final ZipNode zipNode = getExistingZipNode(mountPoint, target);
        final JarEntry zipEntry = zipNode.entry;
        try {
            return new File(new URI("jar", archiveFile.toURI().toString() + "!/", zipEntry.getName()));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
   public InputStream openInputStream(VirtualFile mountPoint, VirtualFile target) throws IOException {
        final ZipNode zipNode = getExistingZipNode(mountPoint, target);
        if (rootNode == zipNode) {
            return new FileInputStream(archiveFile);
        }
        final JarEntry entry = zipNode.entry;
        if (entry == null) {
            throw new IOException("Not a file: \"" + target.getPathName() + "\"");
        }
        return zipFile.getInputStream(entry);
    }

    /** {@inheritDoc} */
    public boolean delete(VirtualFile mountPoint, VirtualFile target) {
        return false;
    }

    /** {@inheritDoc} */
    public long getSize(VirtualFile mountPoint, VirtualFile target) {
        final ZipNode zipNode = getZipNode(mountPoint, target);
        if (zipNode == null) {
            return 0L;
        }
        final JarEntry entry = zipNode.entry;
        if (zipNode == rootNode) {
            return archiveFile.length();
        }
        return entry == null ? 0L : entry.getSize();
    }

    /** {@inheritDoc} */
    public long getLastModified(VirtualFile mountPoint, VirtualFile target) {
        final ZipNode zipNode = getZipNode(mountPoint, target);
        if (zipNode == null) {
            return 0L;
        }
        final JarEntry entry = zipNode.entry;
        return entry == null ? zipTime : entry.getTime();
    }

    /** {@inheritDoc} */
    public boolean exists(VirtualFile mountPoint, VirtualFile target) {
        final ZipNode zipNode = rootNode.find(mountPoint, target);
        return zipNode != null;
    }

    /** {@inheritDoc} */
    public boolean isFile(final VirtualFile mountPoint, final VirtualFile target) {
        final ZipNode zipNode = rootNode.find(mountPoint, target);
        return zipNode != null && zipNode.entry != null;
    }

    /** {@inheritDoc} */
    public boolean isDirectory(VirtualFile mountPoint, VirtualFile target) {
        final ZipNode zipNode = rootNode.find(mountPoint, target);
        return zipNode != null && zipNode.entry == null;
    }

    /** {@inheritDoc} */
    public List<String> getDirectoryEntries(VirtualFile mountPoint, VirtualFile target) {
        final ZipNode zipNode = getZipNode(mountPoint, target);
        if (zipNode == null) {
            return Collections.emptyList();
        }
        final Map<String, ZipNode> children = zipNode.children;
        if (children == null) {
            return Collections.emptyList();
        }
        final Collection<ZipNode> values = children.values();
        final List<String> names = new ArrayList<String>(values.size());
        for (ZipNode node : values) {
            names.add(node.name);
        }
        return names;
    }

    /**
     * {@inheritDoc}
     */
    public CodeSigner[] getCodeSigners(VirtualFile mountPoint, VirtualFile target) {
       final ZipNode zipNode = getZipNode(mountPoint, target);
       if (zipNode == null) {
           return null;
       }
       JarEntry jarEntry = zipNode.entry;
       return jarEntry.getCodeSigners();
    }

    private ZipNode getZipNode(VirtualFile mountPoint, VirtualFile target) {
        return rootNode.find(mountPoint, target);
    }

    private ZipNode getExistingZipNode(VirtualFile mountPoint, VirtualFile target)
            throws FileNotFoundException {
        final ZipNode zipNode = rootNode.find(mountPoint, target);
        if (zipNode == null) {
            throw new FileNotFoundException(target.getPathName());
        }
        return zipNode;
    }

    /** {@inheritDoc} */
    public boolean isReadOnly() {
        return true;
    }

    /** {@inheritDoc} */
    public File getMountSource() {
        return archiveFile;
    }

    public URI getRootURI() throws URISyntaxException {
        return new URI("jar", archiveFile.toURI().toString() + "!/", null);
    }

    /** {@inheritDoc} */
    public void close() throws IOException {
        VFSUtils.safeClose(new Closeable() {
            public void close() throws IOException {
                zipFile.close();
            }
        });
    }

    private File buildFile(File contentsDir, String name) {
       List<String> tokens = PathTokenizer.getTokens(name);
       File currentFile = contentsDir;
       for(String token : tokens) {
          currentFile = new File(currentFile, token);
       }
       currentFile.getParentFile().mkdirs();
       return currentFile;
    }

    private static final class ZipNode {

        // immutable child map
        private final Map<String, ZipNode> children;
        private final String name;
        private final JarEntry entry;

        private ZipNode(Map<String, ZipNode> children, String name, JarEntry entry) {
            this.children = children;
            this.name = name;
            this.entry = entry;
        }

        private ZipNode find(VirtualFile mountPoint, VirtualFile target) {
            if (mountPoint.equals(target)) {
                return this;
            } else {
                final ZipNode parent = find(mountPoint, target.getParent());
                if (parent == null) {
                    return null;
                }
                final Map<String, ZipNode> children = parent.children;
                if (children == null) {
                    return null;
                }
                return children.get(target.getName());
            }
        }
    }
}