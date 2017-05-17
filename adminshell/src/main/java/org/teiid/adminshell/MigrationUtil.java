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

package org.teiid.adminshell;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.ObjectConverterUtil;


@SuppressWarnings("nls")
public class MigrationUtil {

	public static void main(String[] args) throws IOException, TransformerConfigurationException, TransformerFactoryConfigurationError, TransformerException {
		if (args.length != 1) {
			System.err.println(
					"Teiid 7.0 VDB Migration Utility" +
					"\n\nUsage:" +
					"\n  A vdb or .def file must be specified as the only argument." +
					"\n\nResult:"+
					"\n  7.0 compatible replacement files will be created in the same directory " +
					"\n  as your file." +
					"\n  If you supply a vdb, the new vdb file will have a _70.vdb suffix." +
					"\n  If you supply a dynamic vdb file, <file name>-vdb.xml is created " +					
					"\n\nNote: This program will create translator names by Connector's Component Type name" +
					"\n  As they are not gureented to match; recheck their for their validity" +
					"\n\nNote: this program will NOT create the -ds.xml files needed by JBoss to " +
					"\n      create underlying DataSource connection pools." +
					"\n      You will need to manually create one -ds.xml for each JDBC DataSource " +
					"\n      with a JNDI name of <connector binding name>DS, " +
					"\n      where any spaces in the name are replace by _"); 
			System.exit(-1);
		}
		File file = new File(args[0]);
		if (!file.exists()) {
			System.err.println(args[0] + " does not exist."); //$NON-NLS-1$
			System.exit(-1);
		}
		String fullName = file.getName();
		String fileName = fullName.substring(0, fullName.length() - 4);
		String ext = FileUtils.getExtension(file);
		if (ext == null) {
			System.err.println(fullName + " is not a vdb or xml file."); //$NON-NLS-1$
			System.exit(-1);
		}
		ext = ext.toLowerCase();
		if (ext.endsWith("vdb")) {
			File dir = createTempDirectory();
			try {
				extract(file, dir);
				File metainf = new File(dir, "META-INF");
				File config = new File(dir, "ConfigurationInfo.def"); 
				File manifest = new File(dir, "MetaMatrix-VdbManifestModel.xmi");
				if (manifest.exists()) {
					String configStr = ObjectConverterUtil.convertFileToString(config);
					String manifestStr = ObjectConverterUtil.convertFileToString(manifest);
					int index = configStr.lastIndexOf("</VDB>");
					int manifestBegin = manifestStr.indexOf("<xmi");
					configStr = configStr.substring(0, index) + manifestStr.substring(manifestBegin) + "</VDB>";
					FileUtils.write(configStr.getBytes(), config);
					manifest.delete();
				}
				transformConfig(config, "/vdb.xsl", new StreamResult(new File(metainf, "vdb.xml")));
				config.delete();
				FileOutputStream out = new FileOutputStream(new File(file.getParent(), fileName + "_70.vdb"));
				ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(out));
				int parentLength = dir.getPath().length();
				addDirectory(dir, zos, parentLength);
				zos.close();
			} finally {
				FileUtils.removeDirectoryAndChildren(dir);
			}
		} else if (ext.endsWith("xml") || ext.endsWith("def")){
			File parent = file.getParentFile();
			transformConfig(file, "/vdb.xsl", new StreamResult(new File(parent, fileName + "-vdb.xml")));
		} else {
			System.err.println(fullName + " is not a vdb or xml file.  Run with no arguments for help."); //$NON-NLS-1$
			System.exit(-1);
		}
	}

	private static void addDirectory(File dir, ZipOutputStream zos,
			int parentLength) throws IOException {
		String[] files = dir.list();
		for (String entry : files) {
			File f = new File(dir, entry);
			if (f.isDirectory()) {
				addDirectory(f, zos, parentLength);
			} else {
				ZipEntry e = new ZipEntry(f.getPath().substring(parentLength));
				zos.putNextEntry(e);
				FileUtils.write(f, zos);
				zos.closeEntry();
			}
		}
	}

	private static void transformConfig(File config, String styleSheet, Result target)
			throws TransformerFactoryConfigurationError,
			TransformerConfigurationException, TransformerException {
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer t = tf.newTransformer(new StreamSource(MigrationUtil.class.getResourceAsStream(styleSheet)));
		t.setParameter("version", ApplicationInfo.getInstance().getReleaseNumber()); //$NON-NLS-1$
		t.transform(new StreamSource(config), target);
	}

	/**
	 * Extract the given zip file to the given destination directory base.
	 * 
	 * @param zipFileName
	 *            The full path and file name of the Zip file to extract.
	 * @param destinationDirectory
	 *            The root directory to extract to.
	 * @throws IOException
	 */
	static void extract(final File sourceZipFile, File unzipDestinationDirectory) throws IOException {
		// Open Zip file for reading
		ZipFile zipFile = new ZipFile(sourceZipFile, ZipFile.OPEN_READ);

		// Create an enumeration of the entries in the zip file
		Enumeration zipFileEntries = zipFile.entries();

		// Process each entry
		while (zipFileEntries.hasMoreElements()) {
			// grab a zip file entry
			ZipEntry entry = (ZipEntry) zipFileEntries.nextElement();

			String currentEntry = entry.getName();

			File destFile = new File(unzipDestinationDirectory, currentEntry);

			// grab file's parent directory structure
			File destinationParent = destFile.getParentFile();

			// create the parent directory structure if needed
			destinationParent.mkdirs();

			// extract file if not a directory
			if (!entry.isDirectory()) {
				ObjectConverterUtil.write(zipFile.getInputStream(entry),
						destFile);
			}
		}
		zipFile.close();
	}

	static File createTempDirectory() throws IOException {
		File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

		temp.delete();

		if (!(temp.mkdir())) {
			throw new IOException("Could not create temp directory: "
					+ temp.getAbsolutePath());
		}

		return temp;
	}

}
