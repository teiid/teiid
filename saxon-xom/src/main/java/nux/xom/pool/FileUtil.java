/*
 * Copyright (c) 2005, The Regents of the University of California, through
 * Lawrence Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy). All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * (2) Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * (3) Neither the name of the University of California, Lawrence Berkeley
 * National Laboratory, U.S. Dept. of Energy nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * You are under no obligation whatsoever to provide any bug fixes, patches, or
 * upgrades to the features, functionality or performance of the source code
 * ("Enhancements") to anyone; however, if you choose to make your Enhancements
 * available either publicly, or directly to Lawrence Berkeley National
 * Laboratory, without imposing a separate written license agreement for such
 * Enhancements, then you hereby grant the following license: a non-exclusive,
 * royalty-free perpetual license to install, use, modify, prepare derivative
 * works, incorporate into other computer software, distribute, and sublicense
 * such enhancements or derivative works thereof, in binary and source code
 * form.
 */
package nux.xom.pool;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Various file related utilities.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.32 $, $Date: 2006/02/18 04:26:29 $
 */
public class FileUtil {

	private FileUtil() {} // not instantiable
	
	private static final Pattern PATH_SEPARATOR = Pattern.compile("[\\s:;,]+");
	private static final Pattern MATCH_ALL      = Pattern.compile(".*");
	
	// trick to detect default platform charset
	private static final Charset DEFAULT_PLATFORM_CHARSET = 
		Charset.forName(new InputStreamReader(new ByteArrayInputStream(new byte[0])).getEncoding());	

	/**
	 * Returns the URIs of all files who's path matches at least one of the
	 * given <i>inclusion </i> wildcard or regular expressions but none of the
	 * given <i>exclusion </i> wildcard or regular expressions; starting from
	 * the given directory, optionally with recursive directory traversal,
	 * insensitive to underlying operating system conventions.
	 * <p>
	 * An inclusion and exclusion pattern can contain zero or more expressions,
	 * each separated by one or more <code>':'</code>,<code>';'</code>,
	 * <code>','</code> or whitespace characters, for example
	 * <code>"*.xml *.xsl"</code> or <code>"*.xml, *.xsl"</code>.
	 * <p>
	 * A wildcard expression can contain zero or more "match any char sequence"
	 * wildcards, denoted by the <code>'*'</code> character. Wildcard
	 * expressions (and the directory parameter) have identical behaviour no
	 * matter whether they contains Unix <code>'/'</code> or Windows
	 * <code>'\'</code> file separator characters, on any host operating
	 * system. In other words, <code>path/to/file</code> and
	 * <code>path\to\file</code> and <code>path/to\file</code> will always
	 * work fine, irrespective of the underlying OS. This is not the case for
	 * regular expressions (because those characters can have multiple meanings,
	 * and thus cannot be safely substituted).
	 * <p>
	 * Wildcard expressions are simple and intuitive, whereas regular
	 * expressions are more complex and powerful. A wildcard expression is
	 * indicated by the absence of a leading <code>'#'</code> character.
	 * Otherwise the expression is treated as a normal Java regular expression,
	 * with the leading <code>'#'</code> character stripped off.
	 * <p>
	 * Direct or indirect infinite cycles in recursive directory traversal
	 * (cyclic symbolic links etc.) are detected and avoided via
	 * <code>File.getCanonicalPath()</code> duplicate checks.
	 * <p>
	 * Example usage:
	 * 
	 * <pre>
	 * // Simple wildcard expressions
	 * // all files ending with ".xml" or ".xsl" or ".svg" in the "/tmp" dir: 
	 * listFiles("/tmp", false, "*.xml *.xsl *.svg", null); 
	 * 
	 * // Simple wildcard expressions
	 * // all files in the current working dir and descendants,  
	 * // excluding hidden dot files and files ending with "~" or ".bak", 
	 * // and excluding files in CVS directories,
	 * // and excluding files starting with "error" or of the form "bugXYZreport-XYZ.xml" 
	 * // where XYZ can be any character sequence (including zero chars)
	 * listFiles(".", true, null, ".*, *~, *.bak, CVS/*, error*, bug*report-*.xml"); 
	 * 
	 * 
	 * 
	 * // Advanced regular expressions
	 * // Note that javadoc renders regexes in buggy ways: for the correct regexes 
	 * // check the source code rather than the javadoc HTML output. 
	 * 
	 * // all files ending with ".xml" or ".xsl" or ".svg" in the "/tmp" dir: 
	 * listFiles("/tmp", false, "#.*\\.xml, #.*\\.xsl, #.*\\.svg", null); 
	 * 
	 * // Advanced regular expressions
	 * // all files in the current working dir and descendants,  
	 * // excluding files in CVS directories, hidden dot files and files ending with "~" or ".bak":
	 * String dotFiles = "#.*"  +  "/\\.[^/]*";
	 * listFiles(".", true, null, "#.*CVS/.*, " + dotFiles + ", #.*~, #.*\\.bak"); 
	 * </pre>
	 * <p>
	 * Note: The returned URIs can be converted to valid files via
	 * <code>new File(uri)</code> or to strings via
	 * <code>uri.toString()</code>. These can be passed to, say,
	 * <code>new Builder().build(...)</code> or be used to load documents in
	 * XPath/XQuery via
	 * 
	 * <pre>
	 *     declare namespace util = "java:nux.xom.pool.FileUtil"; 
	 *     for $uri in util:listFiles(".", false(), "*.xml", "") 
	 *     return count(doc(string($uri))//*);
	 * </pre>
	 * 
	 * This method is thread-safe.
	 * 
	 * @param directory
	 *            the path or URI of the directory to start at. Leading
	 *            <code>"file://"</code> and <code>"file:"</code> URI
	 *            prefixes are stripped off if present. If <code>null</code>
	 *            or <code>""</code> or <code>"."</code> defaults to the
	 *            current working directory. If the directory does not exist an
	 *            empty result set is returned.
	 *            <p>
	 *            Absolute examples: <code>"/tmp/lib"</code>,
	 *            <code>"file:/tmp/lib"</code>,
	 *            <code>"file:///tmp\lib"</code>,<code>"C:\tmp\lib"</code>,
	 *            <code>"file://C:\tmp\lib"</code>, Windows UNC
	 *            <code>"\\server\share\tmp\lib"</code>,
	 *            <code>"file:\\server\share\tmp\lib"</code> 
	 *            <code>"file://\\server\share\tmp\lib"</code>,
	 *            etc.
	 *            <p>
	 *            Relative examples: <code>"."</code>,
	 *            <code>"nux/lib/CVS"</code>,<code>"nux/lib\CVS"</code>
	 * 
	 * @param recurse
	 *            whether or not to traverse the file system tree.
	 * @param includes
	 *            zero or more wildcard or regular expressions to match for
	 *            result set inclusion; as in
	 *            <code>File.getPath().matches(regex)</code>. If
	 *            <code>null</code> or an empty string defaults to matching
	 *            all files. Example: <code>"*.xml *.xsl"</code>. Example:
	 *            <code>"*.xml, *.xsl"</code>.
	 * @param excludes
	 *            zero or more wildcard or regular expressions to match for
	 *            result set exclusion; as in
	 *            <code>File.getPath().matches(regex)</code>. If
	 *            <code>null</code> or an empty string defaults to matching
	 *            (i.e. excluding) no files. Example: <code>"*.xml *.xsl"</code>.
	 *            Example: <code>"*.xml, *.xsl"</code>.
	 * 
	 * @return the URIs of all matching files (omitting directories)
	 * @see Pattern
	 * @see File
	 * @see String#matches(java.lang.String)
	 */
	public static URI[] listFiles(
			String directory, boolean recurse, String includes, String excludes) {
		
		// prepare dir
		File dir = parsePath(directory);
		
		// prepare expressions
		Matcher[] includes2 = parseExpressions(includes);
		if (includes2.length == 0) includes2 = new Matcher[] {MATCH_ALL.matcher("")};
//		if (includes2.length == 0) includes2 = parseExpressions("#.*"); // match all
		Matcher[] excludes2 = parseExpressions(excludes);
		
		// do the real work
		List uris = new ArrayList();
		Set history = recurse ? new HashSet() : null;
		listFiles2(dir, includes2, excludes2, uris, history);
		
		// output results
		URI[] results = new URI[uris.size()];
		uris.toArray(results);
		return results;
	}

	/** Parses OS insensitive file path, stripping off leading URI scheme, if any */
	private static File parsePath(String path) {
		path = (path == null ? "" : path.trim());
		if (path.startsWith("file://"))  {
			path = path.substring("file://".length());
		} else if (path.startsWith("file:")) { 
			path = path.substring("file:".length());	
		}
		
		if (path.length() == 0 || path.equals(".")) {
			path = XOMUtil.getSystemProperty("user.dir", "."); // CWD
		} else {	
			// convert separators to native format
			path = path.replace('\\', File.separatorChar);
			path = path.replace('/',  File.separatorChar);
			
			if (path.startsWith("~")) {
				// substitute Unix style home dir: ~ --> user.home
				String home = XOMUtil.getSystemProperty("user.home", "~");
				path = home + path.substring(1);
			}
		}
		
		return new File(path);
	}
	
	/**
	 * Parses wildcard expressions or regexes into regex matchers, splitting on
	 * one or more whitespace or ':' or ';' or ',' path separators.
	 */
	private static Matcher[] parseExpressions(String expressions) {
		expressions = (expressions == null ? "" : expressions.trim());
		if (expressions.length() == 0) return new Matcher[0]; // optimization
		
		String[] exprs = PATH_SEPARATOR.split(expressions);
//		String[] exprs = expressions.split("[\\s:;,]+");
		Matcher[] matchers = new Matcher[exprs.length];
		int size = 0;
		for (int i=0; i < exprs.length; i++) {
			if (exprs[i].length() > 0) {
				String regex = expression2Regex(exprs[i]);
				matchers[size++] = Pattern.compile(regex).matcher("");
			}
		}
		
		if (size == matchers.length) return matchers;
		Matcher[] results = new Matcher[size];
		System.arraycopy(matchers, 0, results, 0, size);
		return results;
	}

	/** translates a wildcard expression or regex to a regex */
	private static String expression2Regex(String expr) {
		if (expr.startsWith("#")) return expr.substring(1); // it's a regex
		
		expr = "*" + File.separatorChar + expr;             // anypath/expr
		
		// convert separators to native format:
		expr = expr.replace('\\', File.separatorChar);
		expr = expr.replace('/',  File.separatorChar);
		
		// escape all chars except wildcards, substitute wildcards with ".*" regex:
		StringBuffer buf = new StringBuffer(3 * expr.length());
		for (int i = 0; i < expr.length(); i++) {
			char c = expr.charAt(i);
			if (c == '*') {
				buf.append(".*"); // wildcard --> regex
			} else if (c == '\\') {
				buf.append(c);
				buf.append(c); // escape backslash
			} else {
				buf.append("\\Q"); // quote begin
				buf.append(c);
				buf.append("\\E"); // quote end
			}
		}
		return buf.toString();
	}
	
	/** the work horse: recursive file system tree walker */
	private static void listFiles2(
			File dir, Matcher[] includes, Matcher[] excludes, List uris, Set history) {
		
		boolean recurse = (history != null);
		try { // avoid infinite cycles in directory traversal (cyclic symlinks etc.)
//			if (DEBUG) System.err.println(dir.getCanonicalPath());
			if (recurse && !history.add(dir.getCanonicalPath())) return;
		} catch (IOException e) {
			return; // harmless
		}
		
		File[] files = dir.listFiles();
		if (files == null) return; // dir does not exist
		
		// breadth-first search
		for (int i=0; i < files.length; i++) {
			File file = files[i];
			if (!file.isDirectory()) {
				for (int j=0; j < includes.length; j++) {
					if (includes[j].reset(file.getPath()).matches()) {
						boolean exclude = false; 
						for (int k=0; !exclude && k < excludes.length; k++) {
							exclude = excludes[k].reset(file.getPath()).matches();
						}
						if (!exclude) uris.add(file.toURI());
						break; // move to next file (mark as non-dir)
					}
				}
				
				// mark as non-directory, avoiding expensive isDirectory() calls below
				files[i] = null;
			}
		}
		
		// recurse into directories
		for (int i=0; recurse && i < files.length; i++) {
			if (files[i] != null) {
				listFiles2(files[i], includes, excludes, uris, history);
			}
		}
	}

	/**
	 * Reads until end-of-stream and returns all read bytes, finally closes the stream.
	 * 
	 * @param input the input stream
	 * @throws IOException if an I/O error occurs while reading the stream
	 * @return the bytes read from the input stream
	 */
	public static byte[] toByteArray(InputStream input) throws IOException {
		try {
			if (input.getClass() == ByteArrayInputStream.class) { // fast path
				synchronized (input) { // better safe than sorry
					int avail = input.available();
					if (avail >= 0) { // better safe than sorry
						byte[] buffer = new byte[avail];
						input.read(buffer);
						return buffer;
					}
				}
			}
			
			// safe and fast even if input.available() behaves weird or buggy
			int size = Math.max(256, input.available());
			byte[] buffer = new byte[size];
			byte[] output = new byte[size];
			
			size = 0;
			int n;
			while ((n = input.read(buffer)) >= 0) {
				if (size + n > output.length) { // grow capacity
					byte tmp[] = new byte[Math.max(2 * output.length, size + n)];
					System.arraycopy(output, 0, tmp, 0, size);
					System.arraycopy(buffer, 0, tmp, size, n);
					buffer = output; // use larger buffer for future larger bulk reads
					output = tmp;
				} else {
					System.arraycopy(buffer, 0, output, size, n);
				}
				size += n;
			}
	
			if (size == output.length) return output;
			buffer = null; // help gc
			buffer = new byte[size];
			System.arraycopy(output, 0, buffer, 0, size);
			return buffer;
		} finally {
			if (input != null) input.close();
		}
	}

	/**
	 * Reads until end-of-stream and returns all read bytes as a string, finally
	 * closes the stream, converting the data with the given charset encoding, or
	 * the system's default platform encoding if <code>charset == null</code>.
	 * 
	 * @param input the input stream
	 * @param charset the charset to convert with, e.g. <code>Charset.forName("UTF-8")</code>
	 * @throws IOException if an I/O error occurs while reading the stream
	 * @return the bytes read from the input stream, as a string
	 */
	public static String toString(InputStream input, Charset charset) throws IOException {
		if (charset == null) charset = DEFAULT_PLATFORM_CHARSET;			
		byte[] data = toByteArray(input);
		return charset.decode(ByteBuffer.wrap(data)).toString();
	}
			
}