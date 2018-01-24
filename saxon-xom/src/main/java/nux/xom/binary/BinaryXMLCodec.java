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
package nux.xom.binary;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import nu.xom.Attribute;
import nu.xom.Comment;
import nu.xom.DocType;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.IllegalAddException;
import nu.xom.Node;
import nu.xom.NodeFactory;
import nu.xom.Nodes;
import nu.xom.ParentNode;
import nu.xom.ProcessingInstruction;
import nu.xom.Text;
import nu.xom.WellformednessException;
import nu.xom.XMLException;
import nux.xom.io.StreamingSerializer;

/**
 * Serializes (encodes) and deserializes (decodes) XOM XML documents to and from
 * an efficient and compact custom binary XML data format (termed <i>bnux </i>
 * format), without loss or change of any information. Serialization and
 * deserialization is much faster than with the standard textual XML format, and
 * the resulting binary data is more compressed than textual XML.
 * 
 * <h4>Applicability</h4>
 * 
 * The overall goal of the <i>bnux algorithm</i> is to maximize serialization
 * and deserialization (parsing) performance without requiring any schema
 * description. Serialization and deserialization speed are roughly balanced
 * against each other; neither side is particularly favoured over the other.
 * Another benefitial effect of the algorithm is that a considerable degree of
 * XML data redundancy is eliminated, but compression is more a welcome
 * side-effect than a primary goal in itself. The algorithm is primarily
 * intended for tightly coupled high-performance systems exchanging large
 * volumes of XML data over networks, as well as for compact main memory caches
 * and for <i>short-term </i> storage as BLOBs in backend databases or files
 * (e.g. "session" data with limited duration). In the case of BLOB storage,
 * selecting matching BLOBs can be sped up by maintaining a simple metaindex
 * side table for the most frequent access patterns. See the <a
 * href="#performance">performance results</a> below.
 * <p>
 * While the Java API is considered stable, the bnux data format should be
 * considered a black box: Its internals are under-documented and may change
 * without notice from release to release in backwards-incompatible manners. It
 * is unlikely that support for reading data written with older Nux versions
 * will ever be available. bnux is an exchange format but not an
 * interoperability format. Having said that, the data format is machine
 * architecture/platform independent. For example a bnux file can be moved back
 * and forth between a 32 bit Intel little-endian machine and a 64 bit PowerPC
 * big-endian machine; it remains parseable no matter where.
 * <p>
 * This approach is expressly <b>not intended </b>as a replacement for standard
 * textual XML in loosely coupled systems where maximum long-term
 * interoperability is the overarching concern. It is also expressly <b>not
 * intended </b>for long-term data storage. If you store data in bnux format
 * there's every chance you won't be able to read it back a year or two from
 * now, or even earlier. Finally, it is probably unwise to use this class if
 * your application's performance requirements are not particularly stringent,
 * or profiling indicates that the bottleneck is not related to XML
 * serialization/deserialization anyway.
 * <p>
 * The bnux serialization algorithm is a fully streaming block-oriented
 * algorithm, ideal for large numbers of very small to arbitrarily large 
 * XML documents.
 * <p>
 * The bnux deserialization algorithm is a fully streaming algorithm and can
 * optionally be pushed through a {@link nu.xom.NodeFactory}. This enables
 * efficient filtering and can avoid the need to build a main memory tree, which
 * is particularly useful for arbitrarily large documents. For example, streaming 
 * XQueries over binary XML can be expressed via the NodeFactory generated by a
 * {@link nux.xom.xquery.StreamingPathFilter}. In streaming mode, the binary
 * codec exactly mimics the NodeFactory based behaviour of the XOM
 * {@link nu.xom.Builder}.
 * 
 * <h4>Faithfully Preversing XML</h4>
 * 
 * Any and all arbitrary XOM XML documents are supported, and no schema is
 * required. A XOM document that is serialized and subsequently deserialized by
 * this class is <i>exactly the same </i> as the original document, preserving
 * "as is" all names and data for elements, namespaces, additional namespace
 * declarations, attributes, texts, document type, comments, processing
 * instructions, whitespace, Unicode characters including surrogates, etc. As a
 * result, the W3C XML Infoset and the W3C Canonical XML representation is
 * guaranteed to be preserved. In particular there always holds:
 * 
 * <pre>
 * java.util.Arrays.equals(XOMUtil.toCanonicalXML(doc), XOMUtil
 * 		.toCanonicalXML(deserialize(serialize(doc))));
 * </pre>
 * 
 * <h4>Optional ZLIB Compression</h4>
 * 
 * The bnux algorithm considerably compresses XML data with little CPU
 * consumption, by its very design. However, bnux also has an option to further
 * compress/decompress its output/input with the ZLIB compression algorithm.
 * ZLIB is based on Huffman coding and also used by the popular
 * <code>gzip</code> (e.g. {@link java.util.zip.Deflater}). ZLIB compression
 * is rather CPU intensive, but it typically yields strong compression factors,
 * in particular for documents containing mostly narrative text (e.g. the
 * bible). For example, strong compression may be desirable over low-bandwith
 * networks or when bnux data is known to be accessed rather infrequently. On
 * the other hand, ZLIB compression probably kills performance in the presence
 * of high-bandwidth networks such as ESnet, Internet2/Abilene or 10 Gigabit
 * Ethernet/InfiniBand LANs, even with high-end CPUs. CPU drain is also a
 * scalability problem in the presence of large amounts of concurrent
 * connections. An option ranging from 0 (no ZLIB compression; best performance)
 * to 1 (little ZLIB compression; reduced performance) to 9 (strongest ZLIB
 * compression; worst performance) allows one to configure the CPU/memory
 * consumption trade-off.
 * 
 * <h4>Reliability</h4>
 * 
 * This class has been successfully tested against some 50000 extremely
 * weird and unique test documents, including the W3C XML conformance test
 * suite, and no bugs are known.
 * <p>
 * Serialization employs no error checking at all, since malformed XOM input
 * documents are impossible to produce given XOM's design: XOM strictly enforces
 * wellformedness anyway. Deserialization employs some limited error checking,
 * throwing exceptions for any improper API usage, non-bnux input data, data
 * format version mismatch, or general binary data corruption. Beyond this,
 * deserialization relies on XOM's hard-wired wellformedness checks, just like
 * serialization does. Barring one of the above catastrophic situations, the
 * bnux algorithm will always correctly and faithfully reconstruct the exact
 * same well-formed XOM document.
 * 
 * <h4>Example Usage:</h4>
 * 
 * <pre>
 * // parse standard textual XML, convert to binary format, round-trip it and compare results
 * Document doc = new Builder().build(new File("samples/data/periodic.xml"));
 * BinaryXMLCodec codec = new BinaryXMLCodec();
 * byte[] bnuxDoc = codec.serialize(doc, 0);
 * Document doc2 = codec.deserialize(bnuxDoc);
 * boolean isEqual = java.util.Arrays.equals(
 *     XOMUtil.toCanonicalXML(doc), XOMUtil.toCanonicalXML(doc2));
 * System.out.println("isEqual = " + isEqual);
 * System.out.println(doc2.toXML());
 * 
 * // write binary XML document to file
 * OutputStream out = new FileOutputStream("/tmp/periodic.xml.bnux");
 * out.write(bnuxDoc);
 * out.close();
 * 
 * // read binary XML document from file
 * bnuxDoc = FileUtil.toByteArray(new FileInputStream("/tmp/periodic.xml.bnux"));
 * Document doc3 = codec.deserialize(bnuxDoc);
 * System.out.println(doc3.toXML());
 * </pre>
 * 
 * <a name="performance"/>
 * <h4>Performance</h4>
 * 
 * This class has been carefully profiled and optimized. Preliminary performance
 * results over a wide range of real-world documents are given below. A more
 * detailed presentation can be found at the Global Grid Forum <a
 * target="_blank"
 * href="http://www.ggf.org/GGF15/ggf_events_schedule_WSPerform.htm">Web
 * Services Performance Workshop</a>.
 * <p>
 * Contrasting bnux BinaryXMLCodec with the XOM Builder and Serializer:
 * <ul>
 * <li>Tree Deserialization speedup: 40-100 MB/s vs. 3-30 MB/s</li>
 * <li>Streaming Deserialization speedup: 60-500 MB/s vs. 3-30 MB/s</li>
 * <li>Tree Serialization speedup: 50-150 MB/s vs. 5-20 MB/s</li>
 * <li>Data compression factor: 1.0 - 4</li>
 * </ul>
 * For meaningful comparison, MB/s and compression factors are always given
 * normalized in relation to the original standard textual XML file size.
 * <ul>
 * <li>Benchmark test data: A wide variety of small to medium large XML
 * documents is used, including SOAP documents heavily using namespaces ( <a
 * target="_blank" href="http://xbis.sourceforge.net/">XBIS </a>), simple XML
 * formatted web server logs using no namespaces, RDF documents with lots of
 * attributes and namespaces, the periodic table, documents consisting of large
 * narrative text ( <a target="_blank"
 * href="http://www.oasis-open.org/cover/bosakShakespeare200.html">Shakespeare
 * </a>), publication citations ( <a target="_blank"
 * href="http://dblp.uni-trier.de/xml/">DBLP </a>), music title databases ( <a
 * target="_blank" href="http://www.freedb.org/">FreeDB </a>), Japanese
 * documents (XML conformance test suite), SVG image files, etc.</li>
 * 
 * <li>Benchmark configuration: no ZLIB compression, xom-1.2, non-validating
 * XOM Builder using xerces-2.8.0, no DTD or schema, Sun JDK 1.5.0 server VM,
 * commodity PC 2004, Dual Pentium 4, 3.4 GHz, Redhat 9</li>
 * </ul>
 * 
 * Example Interpretation:
 * <ul>
 * <li>Small documents: Results translate, for example, to ping-pong
 * round-tripping of typical 500 byte SOAP request/response message documents at
 * a rate of 25000 msg/s, compared to 2500 msg/s with XOM (excluding network
 * latency). More pronounced, 500 (150) byte documents with few namespaces
 * translate to 35000 (120000) msg/s, compared to 3500 (5000) msg/s with XOM.
 * Consequently, XML serialization and deserialization are probably nomore your
 * application's bottleneck, leaving, say, 95% CPU headroom free for other
 * application modules.</li>
 * 
 * <li>Medium documents: When having a main-memory cache of several thousand 1
 * MB documents, each containing highly structured complex data, one can
 * deserialize, XQuery and serve from the cache at a rate of 50 documents/s,
 * compared to 5 documents/s with XOM.</li>
 * 
 * </ul>
 * Note that in contrast to other algorithms, these measurements include XOM
 * tree building and walking, hence measures delivering data to and from actual
 * XML applications, rather than merely to and from a low-level SAX event stream
 * (which is considerably cheaper and deemed less useful).
 * <p>
 * The deserialization speedup is further multiplied when DTDs or schema
 * validation is used while parsing standard textual XML.
 * <p>
 * This class relies on advanced Java compiler optimizations, which take
 * considerable time to warm up. Hence, for comparative benchmarks, use a
 * server-class VM and make sure to repeat runs for at least 30 seconds.
 * <p>
 * Further, you will probably want to eliminate drastic XOM hotspots by
 * compiling XOM with "ant -Dfat=true jar" to maintain an internal String
 * instead of an UTF-8 encoded byte array in {@link nu.xom.Text}, which
 * eliminates the expensive character conversions implied for each access to a
 * Text object. This increases performance at the expense of memory footprint.
 * The measurements above report numbers using these patches, both for xom and
 * bnux. If you're curious about the whereabouts of bottlenecks, run java with
 * the non-perturbing '-server -agentlib:hprof=cpu=samples,depth=10' flags, then
 * study the trace log and correlate its hotspot trailer with its call stack
 * headers (see <a target="_blank"
 * href="http://java.sun.com/developer/technicalArticles/Programming/HPROF.html">
 * hprof tracing </a>).
 * <p>
 * Use class {@link nux.xom.sandbox.BinaryXMLTest} to reproduce results, verify
 * correctness or to evaluate performance for your own datasets.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek $
 * @version $Revision: 1.179 $, $Date: 2006/06/18 21:25:02 $
 */
public class BinaryXMLCodec {
	
	/*
	 * TODO: add a StAX interface on top of bnux?
	 * e.g. createXMLStreamReader(byte[]), similar for XMLStreamWriter
	 * TODO: add coalescing of adjacent Text nodes on deserialization?
	 */
	
	/* 
	 * TODO: My impression is that there is remaining potential for some speedup,
	 * both for serialization and deserialization performance. Ideas towards this 
	 * end include:
	 * 
	 * - Add option to always use UTF16 (level=-1)
	 * - Micro caches may become obsolete once XOM has its own internal QName LRU cache
	 * - Use a better low level namespace iteration
	 * - Split unified symbolTable into several smaller symbolTables for Text, Attributes, and other.
	 * - other?
	 */ 

	// for deserialization: factory to stream (push) into
	private NodeFactory factory;

	// for deserialization: unique symbols from deserialized symbolTable
	private String[] symbols;

	// for deserialization: are pages ZLIB compressed?
	private boolean isCompressed;	

	// for deserialization and serialization: page buffer
	private ArrayByteList page; // multi-byte integers are ALWAYS in big-endian
	
	// for serialization:
	// holds unique strings found in document (qnames, texts, uris, etc.)
	private SymbolTable symbolTable; 
	
	// for serialization:
	// byte-level node type tokens in XML document order; also length of next index(es)
	private ArrayByteList nodeTokens;
	
	// for serialization: indexes into symbolTable/entries
	private ArrayIntList indexData; 
	
	// for serialization: has first page of current document already been written?
	private boolean isFirstPage = true;

	// ZLIB
	private Inflater decompressor; // ZLIB
	private Deflater compressor;   // ZLIB
	private int compressionLevel = -1; // initialize to "undefined"
	
	// for deserialization: avoids reverification of PCDATA
	private Text[] textCache;
	
	// for deserialization: avoids reverification of namespace URIs
	private String[] nameCache;
	private LRUHashMap1 internedNames;
	
	// for deserialization:
	// avoids reverification of qname and URI, as well as indexOf() calls, and saves string memory
	private NodeBuilder nodeBuilder;	
	
	// for streaming serialization
	private OutputStream out;
	
	/**
	 * For serialization: (approximate) maximum number of bytes per page.
	 * <p>
	 * To enable true streaming, a serialized document consists of one or more
	 * independent consecutive pages. Each page contains a portion of the XML
	 * document, in document order. More specifically, each page consists of a
	 * tokenized byte array and corresponding symbols. Once a page has been
	 * read/written related (heavy) state can be discarded, freeing memory. No
	 * more than one page needs to be held in memory at any given time. For very
	 * large documents this page design reduces memory consumption, increases
	 * throughput and reduces latency. For small to medium sized documents it
	 * makes next to no difference.
	 * <p>
	 * A small page capacity (e.g. 128 bytes) leads to lower latency per page
	 * but also lower throughput and lower compression overall. Conversely, a
	 * large page capacity (e.g. 1 MB) leads to higher throughput and higher
	 * compression, at the expense of higher latency. However, a very large page
	 * capacity (e.g. 10 MB) leads to memory subsystem pressure on streaming.
	 * Thus, here we use a happy medium, small enough to generate little memory
	 * subsystem pressure, and large enough to gain high throughput, retain
	 * almost all compression stemming from redundancy eliminating tokenization,
	 * with near zero overhead for small to medium sized documents, and
	 * outstanding performance for very large documents.
	 */
	private static final int MAX_PAGE_CAPACITY = 64 * 1024;
//	private static final int MAX_PAGE_CAPACITY = 1; // DEBUG only
	
	// low 3 bits for node token types
	// high 4 bits typically hold number of bytes of the next two indexes
	private static final int TEXT = 0;
	private static final int ATTRIBUTE = 1;
	private static final int BEGIN_ELEMENT = 2;
	private static final int END_ELEMENT = 3;
	private static final int COMMENT = 4;
	private static final int NAMESPACE_DECLARATION = 5;	
	private static final int PROCESSING_INSTRUCTION = 6;
	private static final int DOC_TYPE = 7;
	
	private static final int BNUX_MAGIC = createMagicNumber(); // for sanity checks
	private static final byte VERSION = 7; // version of bnux data format
	private static final int DOCUMENT_HEADER_SIZE = 4 + 1; // in bytes
	private static final int PAGE_HEADER_SIZE = 4 + 4 + 4 + 4 + 4; // in bytes
	
	/**
	 * Marker for non-existant systemID or publicID in DocType. XML and the
	 * nu.xom.Verifier regard " " as an illegal ID, so it can never occur in
	 * practice. Thus we can use it as an unambigous marker identifying a
	 * serialized null value.
	 */
	private static final String DOCTYPE_NULL_ID = " ";
		
	private static final boolean DEBUG = false; // VM does dead code elimination
	
	/** Reinitializes instance variables to virgin state. */
	private void reset() {
		// deserialization state:
		internedNames = null; 
		nodeBuilder = null;
		factory = null;
		
		// serialization state:
		symbolTable = null;
		page = null;
		nodeTokens = null;
		indexData = null;
		isFirstPage = true;
		out = null;

		// better safe than sorry:
		try {
			if (decompressor != null) decompressor.end(); // free resources
		} finally {
			decompressor = null;
			try {
				if (compressor != null) compressor.end(); // free resources
			} finally {
				compressor = null;
			}
		}
	}
	
	/**
	 * Constructs an instance; An instance can be reused serially, but is not
	 * thread-safe, just like a {@link nu.xom.Builder}.
	 */
	public BinaryXMLCodec() {
	}
	
	/**
	 * Constructs a new streaming serializer that serializes bnux binary XML to
	 * the given underlying output stream, using the given ZLIB compression
	 * level.
	 * <p>
	 * An optional zlib compression level ranging from 0 (no ZLIB compression;
	 * best performance) to 1 (little ZLIB compression; reduced performance) to
	 * 9 (strongest ZLIB compression; worst performance) allows one to configure
	 * the CPU/memory consumption trade-off.
	 * <p>
	 * Unless there is a good reason to the contrary, you should always use
	 * level 0: the bnux algorithm typically already precompresses considerably.
	 * 
	 * @param out
	 *            the underlying output stream to write to
	 * @param zlibCompressionLevel
	 *            a number in the range 0..9
	 * @return a streaming serializer
	 */
	public StreamingSerializer createStreamingSerializer(OutputStream out, int zlibCompressionLevel) {
		return new StreamingBinaryXMLSerializer(this, out, zlibCompressionLevel);
	}
	
	/**
	 * Returns whether or not the given input stream contains a bnux document.
	 * <p>
	 * A peek into the first 4 bytes is sufficient for unambigous detection, as
	 * standard textual XML cannot start with any arbitrary four byte
	 * combination.
	 * <p>
	 * Finally, the read bytes are put back onto the stream, so they can be
	 * reread as part of subsequent parsing attempts. Therefore, the input
	 * stream must support <code>input.mark()</code> and
	 * <code>input.reset()</code>. For example, a
	 * {@link java.io.BufferedInputStream} is a good choice.
	 * 
	 * @param input
	 *            the stream to read from
	 * @return true if the stream contains a bnux document
	 * @throws IllegalArgumentException
	 *             if the underlying stream does not support
	 *             <code>input.mark()</code> and <code>input.reset()</code>.
	 * @throws IOException
	 *             if the underlying input stream encounters an I/O error
	 * @see InputStream#mark(int)
	 */
	public boolean isBnuxDocument(InputStream input) throws IOException {
		if (input == null) 
			throw new IllegalArgumentException("input stream must not be null");
		if (!input.markSupported()) 
			throw new IllegalArgumentException("markSupported() must be true");
		
		int magicBytes = 4;
		input.mark(magicBytes);
		try {
			ArrayByteList list = new ArrayByteList(magicBytes);
			if (!list.ensureRemaining(input, magicBytes)) {
				return false; // stream contains less than 4 bytes
			}
			return list.getInt() == BNUX_MAGIC;
		} finally {
			input.reset(); // unread the header
		}
	}
	
	/**
	 * Equivalent to
	 * <code>deserialize(new ByteArrayInputStream(input), new NodeFactory())</code>.
	 * 
	 * @param bnuxDocument
	 *            the bnux document to deserialize.
	 * @return the new XOM document obtained from deserialization.
	 * @throws BinaryParsingException
	 *             if the bnux document is unreadable or corrupt for some reason
	 */
	public Document deserialize(byte[] bnuxDocument) throws BinaryParsingException {
		if (bnuxDocument == null) 
			throw new IllegalArgumentException("bnuxDocument must not be null");
		
		try {
			return deserialize(new ByteArrayInputStream(bnuxDocument), null);
		} catch (IOException e) {
			throw new BinaryParsingException(e); // can never happen
		}
	}
	
	/**
	 * Returns the XOM document obtained by deserializing the next binary XML
	 * document from the given input stream.
	 * <p>
	 * If the document is in ZLIB compressed bnux format, it will be
	 * auto-detected and auto-decompressed as part of deserialization.
	 * <p>
	 * This method exactly mimics the NodeFactory based behaviour of the XOM
	 * {@link nu.xom.Builder}. A NodeFactory enables efficient filtering and
	 * can avoid the need to build a main memory tree, which is particularly
	 * useful for large documents. For example, streaming XQueries over binary
	 * XML can be expressed via the NodeFactory generated by a
	 * {@link nux.xom.xquery.StreamingPathFilter}. Binary XML files can be
	 * converted to and from standard textual XML files via a
	 * {@link nux.xom.pool.XOMUtil#getRedirectingNodeFactory(StreamingSerializer)}. For
	 * other example factories, see {@link nux.xom.pool.XOMUtil}.
	 * <p>
	 * Bnux is a self-framing data format: It knows where the end of a document
	 * occurs. An input stream can contain any number of independent documents,
	 * one after another. Thus, this method reads from the stream as many bytes
	 * as required for the current document, but no more than that. Unlike SAX
	 * XML parsers and unlike a {@link nu.xom.Builder}, it does not read until
	 * end-of-stream (EOS), and it does not auto-close the input stream. If this
	 * method returns successfully, the input stream has been positioned one
	 * byte past the current bnux document, ready to deserialize the following
	 * document, if any. It is the responsibility of the caller to ensure the
	 * input stream gets properly closed when deemed appropriate.
	 * 
	 * @param input
	 *            the stream to read and deserialize from
	 * @param factory
	 *            the node factory to stream into. May be <code>null</code> in
	 *            which case the default XOM NodeFactory is used, building the
	 *            complete XML document tree.
	 * @return the new XOM document obtained from deserialization.
	 * @throws BinaryParsingException
	 *             if the bnux document is unreadable or corrupt for some reason
	 * @throws IOException
	 *             if the underlying input stream encounters an I/O error
	 */
	public Document deserialize(InputStream input, NodeFactory factory) 
			throws BinaryParsingException, IOException {
		
		if (input == null) 
			throw new IllegalArgumentException("input stream must not be null");
		if (factory == null) factory = new NodeFactory();
		
		// read document header
		if (page == null) page = new ArrayByteList(256);
		page.clear();
		if (!page.ensureRemaining(input, 4 + 1 + 1 + 4)) 
			throw new BinaryParsingException("Missing bnux document header");

		int magic = page.getInt();
		if (magic != BNUX_MAGIC) throw new BinaryParsingException(
			"Bnux magic number mismatch: " + magic + ", must be: " + BNUX_MAGIC);
		
		int version = page.get();
		isCompressed = version < 0;
		if (isCompressed) version = -version;
		if (version != VERSION) throw new BinaryParsingException(
			"Bnux data format version mismatch: " + version + ", must be: " + VERSION);
		if (isCompressed) {
			if (decompressor == null) decompressor = new Inflater();
		}
		
		if (page.get() != DOC_TYPE) // surrogate hack to identify BEGIN_PAGE
			throw new BinaryParsingException("Illegal bnux page header marker");
		
		// prepare
		if (internedNames == null) internedNames = new LRUHashMap1(128);
		if (nodeBuilder == null) nodeBuilder = new NodeBuilder();
				
		// parse node token data and packed indexes, building the XOM tree
		this.factory = factory;
		try {
			return readDocument(page, input);
		} catch (Throwable t) { 
			reset(); // better safe than sorry
			if (t instanceof Error) {
				throw (Error) t;
			} else if (t instanceof BinaryParsingException) {
				throw (BinaryParsingException) t;
			} else if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new BinaryParsingException(t);
			}
		} finally {
			this.symbols = null; // help gc
			this.textCache = null; // help gc
			this.nameCache = null; // help gc
			this.factory = null; // help gc
//			if (decompressor != null) decompressor.end();
//			decompressor = null;
		}
	}
	
	/**
	 * Returns the bnux binary XML document obtained by serializing the given
	 * XOM document.
	 * <p>
	 * An optional zlib compression level ranging from 0 (no ZLIB compression;
	 * best performance) to 1 (little ZLIB compression; reduced performance) to
	 * 9 (strongest ZLIB compression; worst performance) allows one to configure
	 * the CPU/memory consumption trade-off.
	 * <p>
	 * Unless there is a good reason to the contrary, you should always use
	 * level 0: the bnux algorithm typically already precompresses considerably.
	 * 
	 * @param document
	 *            the XOM document to serialize
	 * @param zlibCompressionLevel
	 *            a number in the range 0..9
	 * @return the bnux document obtained from serialization.
	 * @throws IllegalArgumentException
	 *             if the compression level is out of range.
	 */
	public byte[] serialize(Document document, int zlibCompressionLevel) 
			throws IllegalArgumentException {
		
		ByteArrayOutputStream result = new ByteArrayOutputStream(256);
		try {
			serialize(document, zlibCompressionLevel, result);
		} catch (IOException e) {
			throw new RuntimeException(e); // can never happen
		}
		return result.toByteArray();
	}
	
	/**
	 * Serializes the given XOM document as a bnux binary XML document onto 
	 * the given output stream.
	 * <p>
	 * An optional zlib compression level ranging from 0 (no ZLIB compression;
	 * best performance) to 1 (little ZLIB compression; reduced performance) to
	 * 9 (strongest ZLIB compression; worst performance) allows one to configure
	 * the CPU/memory consumption trade-off.
	 * <p>
	 * Unless there is a good reason to the contrary, you should always use
	 * level 0: the bnux algorithm typically already precompresses considerably.
	 * 
	 * @param document
	 *            the XOM document to serialize
	 * @param zlibCompressionLevel
	 *            a number in the range 0..9
	 * @param out
	 * 			 the output stream to write to
	 * @throws IllegalArgumentException
	 *             if the compression level is out of range.
	 * @throws IOException
	 *             if the underlying output stream encounters an I/O error
	 */
	public void serialize(Document document, int zlibCompressionLevel, 
			OutputStream out) throws IllegalArgumentException, IOException {
		
		if (document == null) 
			throw new IllegalArgumentException("XOM document must not be null");
		if (zlibCompressionLevel < 0 || zlibCompressionLevel > 9)
			throw new IllegalArgumentException("Compression level must be 0..9");
		if (out == null) 
			throw new IllegalArgumentException("Output stream must not be null");
		
		try {			
			setOutputStream(zlibCompressionLevel, out);
			writeDocument(document); // generate output
		} catch (Throwable t) { 
			reset(); // better safe than sorry
			if (t instanceof Error) {
				throw (Error) t;
			} else if (t instanceof RuntimeException) {
				throw (RuntimeException) t;
			} else if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new RuntimeException(t);
			}
		} finally {
			this.symbolTable = null; // help gc	
			this.out = null;
		}	
	}
	
	final void setOutputStream(int zlibCompressionLevel, OutputStream out) {
		if (zlibCompressionLevel > 0) {
			if (compressor == null || zlibCompressionLevel != compressionLevel) {
				if (compressor != null) compressor.end(); // free resources
				compressor = new Deflater(zlibCompressionLevel);
			}
		}
		compressionLevel = zlibCompressionLevel;
		this.out = out;
	}
	
	/** Prepares reading from the next page. */
	private void readPage(ArrayByteList src, InputStream input) 
			throws BinaryParsingException, IOException {
		
		if (DEBUG) System.err.println("reading page");
		if (!src.ensureRemaining(input, 4))
			throw new BinaryParsingException("Missing remaining bnux page size");
		int pageSize = src.getInt();
		if (src.remaining() != 0) 
			throw new IllegalStateException("Internal codec bug");
		
		boolean isLastPage = pageSize < 0;
		if (isLastPage) pageSize = -pageSize;
//		if (DEBUG) System.err.println("pageSize = " + pageSize);
		if (!isLastPage) { 
			pageSize++; // read one byte past page, fetching PAGE_BEGIN marker
		}
		if (!src.ensureRemaining(input, pageSize)) 
			throw new BinaryParsingException("Missing remaining bnux page body");
		
		if (isCompressed) decompress(src);
		
		int symbolTableSize = src.getInt();
		if (symbolTableSize < 0) 
			throw new BinaryParsingException("Negative symbol table size");
		int decodedSize = src.getInt();
		if (decodedSize < 0) 
			throw new BinaryParsingException("Negative decodedSize");
		int encodedSize = src.getInt();
		if (encodedSize < 0) 
			throw new BinaryParsingException("Negative encodedSize");
		
		// read symbolTable
		this.symbols = null; // help gc
		if (decodedSize == encodedSize) { // safe trick, faster
			// Note that 7 bit ASCII is a proper subset of UTF-8
			this.symbols = src.getASCIIStrings(symbolTableSize); 
		} else { 
			this.symbols = src.getUTF8Strings(symbolTableSize);
		}
//		this.symbols = src.getUTF16Strings(symbolTableSize);
		if (DEBUG) System.err.println("read symbols = " + Arrays.asList(symbols));
		
		int magic = src.getInt();
		if (magic != BNUX_MAGIC) throw new BinaryParsingException(
			"Bnux magic number mismatch: " + magic + ", must be: " + BNUX_MAGIC);
		
		// reset caches in preparation for XML token decoding
		if (this.nameCache == null) {
			nameCache = new String[Math.min(64, symbolTableSize)];
		} else {
			for (int i=nameCache.length; --i >= 0; ) nameCache[i] = null;
		}
		
		if (factory.getClass() == NodeFactory.class) { // fast path
			if (this.textCache == null) {
				textCache = new Text[Math.min(256, symbolTableSize)];
			} else {
				for (int i=textCache.length; --i >= 0; ) textCache[i] = null;
			}
		}		
//		this.nameCache = null; // help gc
//		this.nameCache = new String[Math.min(64, symbolTableSize)];	
//		this.textCache = null; // help gc
//		if (factory.getClass() == NodeFactory.class) { // fast path
//			this.textCache = new Text[Math.min(128, symbolTableSize)];
//		}
	}
	
	private void decompress(ArrayByteList src) throws BinaryParsingException {
		if (nodeTokens == null) nodeTokens = new ArrayByteList();
		nodeTokens.clear();
		
		try {
			nodeTokens.add(decompressor, src);
		} catch (DataFormatException e) {
			String s = e.getMessage();
		    throw new BinaryParsingException(
		    		s != null ? s : "Invalid ZLIB data format", e);
		}

		src.swap(nodeTokens); // replace src with nodeTokens
		nodeTokens.clear();
	}
	
	/** Parses document from encoded src buffer; tokens appear in document order. */
	private Document readDocument(ArrayByteList src, InputStream input) 
			throws BinaryParsingException, IOException {
		
		if (DEBUG) System.err.println("reading document");
		readPage(src, input);
		Document doc = factory.startMakingDocument();
//		doc.setBaseURI(symbols[src.getInt()]);
		doc.setBaseURI(getInternedName(src.getInt()));
		boolean hasRootElement = false;
		int i = 0;
		
		// add children of document, retaining the exact same order found in input
		while (src.remaining() > 0) {
			Nodes nodes;
			int type = src.get(); // look ahead
			if (DEBUG) System.err.println("reading type = " + toString(type));
			switch (type & 0x07) { // three low bits indicate node type
				case TEXT: {
					throw new BinaryParsingException("Unreachable text");
				}
				case ATTRIBUTE: {
					throw new BinaryParsingException("Unreachable attribute");
				}
				case BEGIN_ELEMENT: {
					if (factory.getClass() == NodeFactory.class) { // fast path
						Element root = readStartTag(src, type);
						readElement(src, root, input); // reads entire subtree
						nodes = new Nodes(root);
					} else { // slow path
						Element root = readStartTagF(src, type, true);
						if (root == null) {
							throw new NullPointerException("Factory failed to create root element.");
						}
						doc.setRootElement(root);
						readElementF(src, root, input);
						nodes = factory.finishMakingElement(root);
					}
					break;
				}
				case END_ELEMENT: {
					throw new BinaryParsingException("Unreachable end of element");
				}
				case COMMENT: {
					nodes = readCommentF(src, type);
					break;
				}
				case NAMESPACE_DECLARATION: {
					throw new BinaryParsingException("Unreachable namespace declaration");
				}
				case PROCESSING_INSTRUCTION: {
					nodes = readProcessingInstructionF(src);
					break;
				}
				case DOC_TYPE: {
					nodes = readDocTypeF(src);
					break;
				}
				default: {
					throw new BinaryParsingException("Illegal node type code=" + type);
				}
			}

			// append nodes:
			for (int j=0; j < nodes.size(); j++) {
				Node node = nodes.get(j);
				if (node instanceof Element) { // replace fake root with real root
					if (hasRootElement) {
						throw new IllegalAddException(
							"Factory returned multiple root elements");
					}
					doc.setRootElement((Element) node);
					hasRootElement = true;
				} else {
					doc.insertChild(node, i);
				}
				i++;
			}
		}
		
		if (!hasRootElement) throw new WellformednessException(
				"Factory attempted to remove the root element");
		factory.finishMakingDocument(doc);
		if (DEBUG) System.err.println("finished reading document");
		return doc;
	}
	
	/** Reads start tag and returns a corresponding empty element */
	private Element readStartTag(ArrayByteList src, int type) {
		String qname = readString(src, 4, type);
		String namespaceURI = readName(src, 6, type);
		return this.nodeBuilder.createElement(qname, namespaceURI);
//		return new Element(qname, namespaceURI);
	}
	
	private Element readStartTagF(ArrayByteList src, int type, boolean isRoot) {
		String qname = readString(src, 4, type);
		String namespaceURI = readName(src, 6, type);
		return isRoot ?
			factory.makeRootElement(qname, namespaceURI) :
			factory.startMakingElement(qname, namespaceURI);
	}
	
	/** Iterative pull parser reading an entire element subtree. */
	private void readElement(ArrayByteList src, Element current, InputStream input) 
		throws BinaryParsingException, IOException {
		
		while (true) {
			Node node = null;
			Element down = null;
			int type = src.get(); // look ahead
//			if (DEBUG) System.err.println("reading type = " + toString(type));
			switch (type & 0x07) { // three low bits indicate node type
				case TEXT: {
					node = readText(src, type);
					break;
				}
				case ATTRIBUTE: {
					readAttribute(src, current, type);
					continue;
				}
				case BEGIN_ELEMENT: {
					down = readStartTag(src, type);
					node = down;
					break;
				}
				case END_ELEMENT: {
					current = (Element) current.getParent();
					if (current == null) return; // we're done with the root element
					continue;
				}
				case COMMENT: {
					node = readComment(src, type);
					break;
				}
				case NAMESPACE_DECLARATION: {
					readNamespaceDeclaration(src, current, type);
					continue;
				}
				case PROCESSING_INSTRUCTION: {
					node = readProcessingInstruction(src);
					break;
				}
				case DOC_TYPE: { // surrogate hack to identify BEGIN_PAGE
					readPage(src, input);
					continue;
				}
			}

//			if (DEBUG) System.err.println("read node=" + node.toXML());
			
			current.insertChild(node, current.getChildCount());
			
			if (down != null) current = down; // recurse down
		}
	}
	
	/** Iterative pull parser reading an entire element subtree (using NodeFactory). */
	private void readElementF(ArrayByteList src, Element current, InputStream input) 
			throws BinaryParsingException, IOException {
		
//		final ArrayList stack = new ArrayList();
		final FastStack stack = new FastStack();
		stack.push(current);
		boolean addAttributesAndNamespaces = true;
		
		while (true) {
			Nodes nodes = null;
			int type = src.get(); // look ahead
//			if (DEBUG) System.err.println("reading type = " + toString(type));

			switch (type & 0x07) { // three low bits indicate node type
				case TEXT: {
					nodes = readTextF(src, type);
					break;
				}
				case ATTRIBUTE: {
					Element elem = addAttributesAndNamespaces ? current : null;
					nodes = readAttributeF(src, elem, type);
					break;
				}
				case BEGIN_ELEMENT: {
					Element elem = readStartTagF(src, type, false);
					stack.push(elem); // even if it's null
					if (elem != null) {
						current.insertChild(elem, current.getChildCount());
						current = elem; // recurse down
					}
					addAttributesAndNamespaces = elem != null;
					continue;
				}
				case END_ELEMENT: {
					Element elem = stack.pop();
					if (elem == null) {
						continue; // skip element
					}
					ParentNode parent = elem.getParent();
					if (parent == null) throwTamperedWithParent();
					if (parent instanceof Document) {
						return; // we're done with the root element
					}
					
					current = (Element) parent; // recurse up
					nodes = factory.finishMakingElement(elem);
										 
					if (nodes.size()==1 && nodes.get(0)==elem) { // same node? (common case)
						continue; // optimization: no need to remove and then readd same element
					}
					
					if (current.getChildCount()-1 < 0) throwTamperedWithParent();				
					current.removeChild(current.getChildCount()-1);
					break;
				}
				case COMMENT: {
					nodes = readCommentF(src, type);
					break;
				}
				case NAMESPACE_DECLARATION: {
					Element elem = addAttributesAndNamespaces ? current : null;
					readNamespaceDeclaration(src, elem, type);
					continue;
				}
				case PROCESSING_INSTRUCTION: {
					nodes = readProcessingInstructionF(src);
					break;
				}
				case DOC_TYPE: { // surrogate hack for BEGIN_PAGE
					readPage(src, input); 
					continue;
				}
			}
		
			appendNodes(current, nodes);
		}
	}
	
	private static void appendNodes(Element elem, Nodes nodes) {
		if (nodes != null) {
			int size = nodes.size();
			for (int i=0; i < size; i++) {
				Node node = nodes.get(i);
				if (node instanceof Attribute) {
					elem.addAttribute((Attribute) node);
				} else {
					elem.insertChild(node, elem.getChildCount());
				}
			}
		}
	}
	
	private static void throwTamperedWithParent() {
		throw new XMLException("Factory has tampered with a parent pointer " + 
				"of ancestor-or-self in finishMakingElement()");
	}
	
	private void readAttribute(ArrayByteList src, Element dst, int type) throws BinaryParsingException {
		String qname = readString(src, 4, type);
		String namespaceURI = readName(src, 6, type);
		String value = readString(src, 4, src.get());
		Attribute.Type attrType = Util.getAttributeType(src.get());
		Attribute attr = this.nodeBuilder.createAttribute(qname, namespaceURI, value, attrType);
//		Attribute attr = new Attribute(qname, namespaceURI, value, attrType);
		dst.addAttribute(attr);		
	}
	
	private Nodes readAttributeF(ArrayByteList src, Element dst, int type) throws BinaryParsingException {
		String qname = readString(src, 4, type);
		String namespaceURI = readName(src, 6, type);
		String value = readString(src, 4, src.get());
		Attribute.Type attrType = Util.getAttributeType(src.get());
		if (dst == null) return null; // NONE;
		return factory.makeAttribute(qname, namespaceURI, value, attrType);
	}
	
	private Comment readComment(ArrayByteList src, int type) {
		return new Comment(readString(src, 4, type));
	}
	
	private Nodes readCommentF(ArrayByteList src, int type) {
		return factory.makeComment(readString(src, 4, type));
	}
	
	private void readNamespaceDeclaration(ArrayByteList src, Element dst, int type) {
		String prefix = readString(src, 4, type);
		String uri = readName(src, 6, type);
		if (dst != null) dst.addNamespaceDeclaration(prefix, uri);
	}
	
	private ProcessingInstruction readProcessingInstruction(ArrayByteList src) {
		int type = src.get(src.position() - 1);
		String target = readString(src, 4, type);
		String value = readString(src, 6, type);
		return new ProcessingInstruction(target, value);
	}
	
	private Nodes readProcessingInstructionF(ArrayByteList src) {
		int type = src.get(src.position() - 1);
		String target = readString(src, 4, type);
		String value = readString(src, 6, type);
		return factory.makeProcessingInstruction(target, value);
	}
	
	/** Does not pack indexes of doctype (infrequent anyway) */
	private Nodes readDocTypeF(ArrayByteList src) {
		String rootElementName = symbols[src.getInt()];
		String publicID = symbols[src.getInt()];
		if (DOCTYPE_NULL_ID.equals(publicID)) publicID = null;
		String systemID = symbols[src.getInt()];
		if (DOCTYPE_NULL_ID.equals(systemID)) systemID = null;
		String internalDTDSubset = symbols[src.getInt()];
		if (internalDTDSubset.length() == 0) internalDTDSubset = null;
		
		Nodes nodes = factory.makeDocType(rootElementName, publicID, systemID);
		for (int i=0; i < nodes.size(); i++) {
			if (nodes.get(i) instanceof DocType) {
				DocType docType = (DocType) nodes.get(i);
				if (docType.getInternalDTDSubset().length() == 0) {
					try {
						docType.setInternalDTDSubset(internalDTDSubset);
					} catch (IllegalAccessError e) {
						; // ignore; setInternalDTDSubset() is private in xom < 1.1 
					}
				}
			}
		}
		return nodes;
	}
	
	// try to avoid XML reverification by caching repetitive Texts
	private Text readText(ArrayByteList src, int type) {
		int i = readSymbol(src, 4, type);
		Text text;
		if (i < textCache.length) {
			text = textCache[i];
			if (text != null) return new Text(text);
		}
		text = new Text(symbols[i]);
		if (i < textCache.length) textCache[i] = text;
		return text;
	}
	
	private Nodes readTextF(ArrayByteList src, int type) {
		return factory.makeText(readString(src, 4, type));
	}
	
	/** Reads string via packed index from symbolTable */
	private String readString(ArrayByteList src, int shift, int type) {
		int i = readSymbol(src, shift, type);
		if (i < 0) return "";
		return symbols[i];
	}
	
	/** Reads string via packed index from symbolTable */
	private static int readSymbol(ArrayByteList src, int shift, int type) {
		// assert shift == 4 || shift == 6		
		if (Util.isInlinedIndex(type)) {
			if (shift == 6) return -1;
			return Util.getInlinedIndex(type);			
		}
		
		switch ((type >>> shift) & 0x03) { // look at two bits indicating index length
			case 0 : return Util.getUnsignedByte(src.get());
			case 1 : return Util.getUnsignedShort(src.getShort());
			case 2 : return -1;
			default: return src.getInt();
		}
	}
	
	private String readName(ArrayByteList src, int shift, int type) {
		int i = readSymbol(src, shift, type);
		if (i < 0) return "";
		if (i < nameCache.length) {
			String name = nameCache[i];
			if (name == null) { // cache miss
				name = getInternedName(i);
				nameCache[i] = name;
			}
			return name;
		}
		return symbols[i];
	}

	private String getInternedName(int i) {
		String name = symbols[i];
		if (name.length() == 0) {
			name = "";
		} else {
			name = (String) internedNames.get(name);
			if (name == null) {
				name = symbols[i];
				internedNames.put(name, name);
			}
		}
		return name;
	}
	
	/** Writes nodeTokens, indexData, symbolTable to output stream */
	private void writePage(boolean isLastPage) throws IOException {
		if (DEBUG) System.err.println("writing page");
		Entry[] entries = symbolTable.getEntries();
		int numChars = symbolTable.numCharacters();
		
		// reorder entries and update indexData accordingly.
		packSort(entries, indexData);
//		if (DEBUG) printStatistics(entries);
		
		// add header to page
		page.ensureCapacity(page.size() + 1 + 4 + 4 + 4 + 4 + 4 + numChars*4 + entries.length + 
				nodeTokens.size() + indexData.size() + numChars/100); // an educated guess
		page.add((byte) DOC_TYPE); // BEGIN_PAGE marker surrogate		
		page.addInt(0); // pageSize dummy placeholder
		int pageOffset = page.size();		
		page.addInt(entries.length);
		page.addInt(numChars + entries.length);  // decodedSize (+zero terminator)
		page.addInt(0); // encodedSize dummy placeholder
		int encodedOffset = page.size();		
		
		// add symbolTable to page
		encodeSymbols(entries, page); // assert: no need to expand underlying array
		int encodedSize = page.size() - encodedOffset;
		page.setInt(encodedOffset-4, encodedSize); // replace dummy placeholder
		entries = null; // help gc
		
		// add node tokens and packed symbol indexes to page
		page.addInt(BNUX_MAGIC);
		encodeTokens(nodeTokens, indexData.asArray(), page);
		
		int pageSize;
		nodeTokens.clear();
		if (compressionLevel > 0) { // compress page body
			page.position(pageOffset);
			nodeTokens.add(compressor, page);
			page.remove(pageOffset, page.size());
			pageSize = nodeTokens.size();
		} else {
			pageSize = page.size() - pageOffset;
		}
		if (isLastPage) pageSize = -pageSize;
		page.setInt(pageOffset-4, pageSize); // replace pageSize dummy placeholder

		// having filled the buffers, flush them onto underlying output stream
		page.write(out);
		nodeTokens.write(out);
		
		// reset 
		if (!isLastPage) symbolTable.clear();
		page.clear();
		nodeTokens.clear();
		indexData.clear();
		if (DEBUG) System.err.println("finished writing page");
	}
	
	private void writeDocument(Document doc) throws IOException {
		if (DEBUG) System.err.println("writing document");
		writeXMLDeclaration(doc.getBaseURI());
		for (int i = 0; i < doc.getChildCount(); i++) {
			Node node = doc.getChild(i);
			if (node instanceof Element) {
				writeElement((Element) node);
			} else if (node instanceof Comment) {
				writeComment((Comment) node);
			} else if (node instanceof ProcessingInstruction) {
				writeProcessingInstruction((ProcessingInstruction) node);
			} else if (node instanceof DocType) {
				writeDocType((DocType) node);
			} else {
				throw new IllegalAddException("Cannot write node type: " + node);
			}
		}
		writeEndDocument();
		if (DEBUG) System.err.println("finished writing document");
	}
	
	final void writeXMLDeclaration(String baseURI) {
		if (baseURI == null) baseURI = "";
		
		// setup
		symbolTable = new SymbolTable();
		if (nodeTokens == null) nodeTokens = new ArrayByteList();
		nodeTokens.clear();
		if (indexData == null) indexData = new ArrayIntList(); 
		indexData.clear();

		// write bnux document header
		if (page == null) page = new ArrayByteList(256);
		page.clear();
		page.ensureCapacity(DOCUMENT_HEADER_SIZE + PAGE_HEADER_SIZE + 1);
		page.addInt(BNUX_MAGIC);
		int version = VERSION;
		if (compressionLevel > 0) version = -version;
		page.add((byte)version);
		
		isFirstPage = true;
		writeIndex(baseURI);		
	}
	
	final void writeEndDocument() throws IOException {
		flush(true);
	}
	
	// assertion: must not be called with !isLastPage 
	// if we're not at a safe point, i.e. at nesting depth == 0
	final void flush(boolean isLastPage) throws IOException {
		try {
			if (nodeTokens.size() > 0) { // anything remaining to be written?
				writePage(isLastPage);
			}
			out.flush();
		} finally {
			if (isLastPage) {
				this.symbolTable = null; // help gc
				this.out = null;
			}
			nodeTokens.clear();
		}
	}
	
	/** Encodes a node into the intermediate unpacked binary form */
	private void writeChild(Node node) throws IOException {
		if (node instanceof Element) {
			writeElement((Element) node);
		} else if (node instanceof Text) {
			writeText((Text) node);
		} else if (node instanceof Comment) {
			writeComment((Comment) node);
		} else if (node instanceof ProcessingInstruction) {
			writeProcessingInstruction((ProcessingInstruction) node);
		} else {
			throw new IllegalAddException("Cannot write node type: " + node);
		}
	}

	final void writeElement(Element elem) throws IOException {	
		writeStartTag(elem);
		
		for (int i = 0; i < elem.getChildCount(); i++) {
			writeChild(elem.getChild(i));
		}
				
		writeEndTag();
	}
	
	final void writeStartTag(Element elem) {
		writeIndex(elem.getNamespacePrefix(), elem.getLocalName());

		int type = BEGIN_ELEMENT;
		if (elem.getNamespaceURI().length() == 0) {
			type = Util.noNamespace(type);
		} else {
			writeIndex(elem.getNamespaceURI());
		}
		nodeTokens.add((byte)type);
		
		for (int i = 0; i < elem.getAttributeCount(); i++) {
			writeAttribute(elem.getAttribute(i));
		}
		
		writeNamespaceDeclarations(elem);
	}

	final void writeEndTag() throws IOException {
		if (nodeTokens.size() + indexData.size() + symbolTable.numCharacters() 
				+ symbolTable.size() >= MAX_PAGE_CAPACITY) {
			writePage(false); // write nodeTokens, indexData, symbolTable to output stream
		}
		
		nodeTokens.add((byte)END_ELEMENT);		
	}

	private void writeAttribute(Attribute attr) {
		writeIndex(attr.getNamespacePrefix(), attr.getLocalName());
		
		int type = ATTRIBUTE;
		if (attr.getNamespaceURI().length() == 0) {
			type = Util.noNamespace(type);
		} else {
			writeIndex(attr.getNamespaceURI());
		}
		
		writeIndex(attr.getValue());
		nodeTokens.add((byte)type);
		nodeTokens.add(Util.getAttributeTypeCode(attr));
	}

	final void writeComment(Comment comment) {
		nodeTokens.add((byte)COMMENT);
		writeIndex(comment.getValue());
	}
	
	/** Does not pack indexes of doctype (infrequent anyway) */
	final void writeDocType(DocType docType) {
		nodeTokens.add((byte)DOC_TYPE);
		writeIndex(docType.getRootElementName());		
		writeIndex(docType.getPublicID() == null ? DOCTYPE_NULL_ID : docType.getPublicID());
		writeIndex(docType.getSystemID() == null ? DOCTYPE_NULL_ID : docType.getSystemID());
		writeIndex(docType.getInternalDTDSubset() == null ? "" : docType.getInternalDTDSubset());
	}	
	
	private void writeNamespaceDeclarations(Element elem) {
		int count = elem.getNamespaceDeclarationCount();
		if (count == 1) 
			return; // elem.getNamespaceURI() has already been written

		for (int i = 0; i < count; i++) {
			String prefix = elem.getNamespacePrefix(i);
			String uri = elem.getNamespaceURI(prefix);
			if (prefix.equals(elem.getNamespacePrefix()) && uri.equals(elem.getNamespaceURI())) {
//				if (DEBUG) System.err.println("********** NAMESPACE IGNORED ON WRITE ***************\n");
				continue;
			}
			nodeTokens.add((byte)NAMESPACE_DECLARATION);
			writeIndex(prefix);
			writeIndex(uri);
		}
	}

	final void writeProcessingInstruction(ProcessingInstruction pi) {
		nodeTokens.add((byte)PROCESSING_INSTRUCTION);
		writeIndex(pi.getTarget());
		writeIndex(pi.getValue());
	}
	
	final void writeText(Text text) {
		nodeTokens.add((byte)TEXT);
		writeIndex(text.getValue());
	}
	
	/** Puts symbol into symbolTable and appends the string's index to indexData */
	private final void writeIndex(String symbol) {
		writeIndex("", symbol);
	}

	/** Puts symbol into symbolTable and appends the string's index to indexData */
	private final void writeIndex(String prefix, String localName) {
		int index = symbolTable.addSymbol(prefix, localName);
		indexData.add(index);
	}

	/** Converts strings of symbolTable into UTF-8 bytes */
	private void encodeSymbols(Entry[] entries, ArrayByteList dst) {		
		/*
		 * As an optimization, one could use dst.ensureCapacity(dst.size + 4 *
		 * sum(entries.key.length) + entries.length) then use plain array
		 * accesses via dst.addUTF8Entries(entries) instead of many small
		 * dst.add(byte) calls. Update: I benchmarked various variants of this
		 * idea; none of them turned out to be worthwhile, so, for the time
		 * being, we'll keep the simple version below.
		 */
//		if (DEBUG) System.err.println("encoding symbols = " + toString(entries));
		int len = entries.length;
		for (int i=0; i < len; i++) {
			Entry entry = entries[i];
			dst.addUTF8String(entry.getKey1(), entry.getKey2());
			//dst.addUTF16String(entry.getKey1(), entry.getKey2());
		}		
	}
	
	/**
	 * Sorts entries descending by symbol frequency (# of occurances) and
	 * updates indexData accordingly. This allows to compress frequent 4 byte
	 * indexes into a single byte. FAQ: this sort is *not* the bottleneck.
	 */
	private void packSort(Entry[] entries, ArrayIntList indexData) {
		if (!DEBUG && entries.length <= 256) { // 0, 1, ... , 255
			return; // no need to sort indexes - all fit into one unsigned byte anyway
		}

		// Swap entries with frequency == 1 to end of array (typically Text nodes).
		// There's no need to sort those with O(N log N).
		int head = entries.length;
		for (int i=entries.length; --i >= 0; ) {
			Entry e = entries[i];
			if (e.getFrequency() == 1) {
				head--;
				entries[i] = entries[head];
				entries[head] = e;
			}
		}
//		if (DEBUG) System.err.println("len=" + entries.length + ", #f>1=" + (100.0f * head / entries.length));
		
		// sort remaining entries descending by frequency.
		Arrays.sort(entries, 0, head,
			new Comparator() {
				public final int compare(Object e1, Object e2) { 
					int f1 = ((Entry) e1).getFrequency();
					int f2 = ((Entry) e2).getFrequency();
					return f2 - f1;
				}
			}
		);
		
		// reorder indexData with sorted indexes
		// since sort has moved entries[indexData[k]] to entries[i]
		int[] indexes = new int[entries.length];
		for (int i=entries.length; --i >= 0; ) {
			indexes[entries[i].getIndex()] = i;
		}
		
		int[] ix = indexData.asArray();
		for (int i=indexData.size(); --i >= 0; ) {
			ix[i] = indexes[ix[i]];
		}
		// post-condition: entries[indexData[k]] corresponds to entries[i]
	}	
	
	/**
	 * Writes nodeTokens in document order; in the process stitches in packed
	 * indexes referring to symbols in the symbolTable.
	 */	
	private void encodeTokens(ArrayByteList tokenList, int[] indexes, ArrayByteList dst) {
		byte[] tokens = tokenList.asArray();
		int size = tokenList.size();
		int i = 0;
		int j = 0;
		if (isFirstPage) dst.addInt(indexes[i++]); // document baseURI
		isFirstPage = false;

		while (j < size) {
			int type = tokens[j++];
			dst.add((byte)type);

//			if (DEBUG) System.err.println("encoding type = " + toString(type));
			switch (type & 0x07) {			
				case TEXT: {
					Util.packOneIndex(dst, indexes[i++], type); // value
					break;
				}
				case ATTRIBUTE: {
					if (Util.hasNoNamespace(type)) { // qname
						Util.packOneIndex(dst, indexes[i++], type); 
					} else { // qname, URI
						Util.packTwoIndexes(dst, indexes[i++], indexes[i++], type);
					}
					dst.add((byte)0);
					Util.packOneIndex(dst, indexes[i++], 0); // value
					dst.add(tokens[j++]); // attrType
					break;
				}
				case BEGIN_ELEMENT: {
					if (Util.hasNoNamespace(type)) {
						Util.packOneIndex(dst, indexes[i++], type); // qname
					} else {
						Util.packTwoIndexes(dst, indexes[i++], indexes[i++], type); // qname, URI
					}
					break;
				}
				case END_ELEMENT: {
					break; // nothing to do
				}				
				case COMMENT: {
					Util.packOneIndex(dst, indexes[i++], type); // value
					break;
				}
				case NAMESPACE_DECLARATION: { // prefix, URI
					Util.packTwoIndexes(dst, indexes[i++], indexes[i++], type); 
					break;
				}
				case PROCESSING_INSTRUCTION: { // target, value
					Util.packTwoIndexes(dst, indexes[i++], indexes[i++], type);
					break;
				}
				case DOC_TYPE: { // infrequent; no need to pack indexes
					dst.addInt(indexes[i++]); // rootElementName
					dst.addInt(indexes[i++]); // publicID
					dst.addInt(indexes[i++]); // systemID
					dst.addInt(indexes[i++]); // internalDTDSubset
					break;
				}
				default: {
					throw new IllegalArgumentException("illegal node type");
				}				
			}			
		}
	}

	private static int createMagicNumber() {
		// 0xE0, 0x01, 0xDF, 0xFE = -32, 1, -33, -2 = -536748034
		// Thanks to Paul.Sandoz@Sun.COM
		ArrayByteList magic = new ArrayByteList(4);
		magic.add((byte)0xE0);
		magic.add((byte)0x01);
		magic.add((byte)0xDF);
		magic.add((byte)0xFE);
		return magic.getInt();
	}
	
	private static String toString(int type) { // DEBUG only
		switch (type & 0x07) {
			case TEXT: return "TEXT";
			case ATTRIBUTE: return "ATTRIBUTE";
			case BEGIN_ELEMENT: return "BEGIN_ELEMENT";
			case END_ELEMENT: return "END_ELEMENT";
			case COMMENT: return "COMMENT";
			case NAMESPACE_DECLARATION: return "NAMESPACE_DECLARATION";
			case PROCESSING_INSTRUCTION: return "PROCESSING_INSTRUCTION";
			case DOC_TYPE: return "DOC_TYPE";
			default: {
				throw new IllegalArgumentException(
						"Illegal node type code=" + (type & 0x07));
			}
		}
	}
		
	private static String toString(Entry[] entries) { // DEBUG only
		ArrayList list = new ArrayList();
		for (int i=0; i < entries.length; i++) {
			list.add(entries[i].getQualifiedName());
		}
		return list.toString();
	}
	

	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Table of unique symbols for tokenization on XML serialization. This is a
	 * classic text book hash algorithm, adapted to meet our specific
	 * performance needs. It's close to the one used by the JDK HashMap.
	 * Maintains a map of (String, String) ==> (index, frequency) associations.
	 */
	private static final class SymbolTable { // not a public class!

		private static final float LOAD_FACTOR = 0.75f;
		private static final int INITIAL_CAPACITY = 16;
		private Entry[] entries = new Entry[INITIAL_CAPACITY];
		private int threshold = (int) (INITIAL_CAPACITY * LOAD_FACTOR);
		private int size = 0;
		private int numChars = 0;
		
		/** Constructs and returns a table with default parameters. */
		public SymbolTable() {
		}
		
		/** Removes all entries from this table, retaining the current capacity. */
		public void clear() {
			size = 0;
			numChars = 0;
			Entry[] src = entries;
			for (int i=src.length; --i >= 0; ) src[i] = null;
		}
		
		/** Returns the total number of characters occupied by all symbol strings. */
		public int numCharacters() {
			return numChars;
		}
		
		/** Returns the number of symbols. */
		public int size() {
			return size;
		}

		/**
		 * Adds the given symbol to the table if not already present. Otherwise
		 * increments its frequency counter.
		 * 
		 * A symbol is structured like a lexical XML QName.
		 * symbol: key1 + ":" + key2   (if key1 is non-empty string)
		 * symbol: key2                (if key1 is empty string)
		 *  
		 * @return a sequence number N >= 0 indicating that the symbol was added
		 *         to this table as the N-th entry, in order.
		 */
		public int addSymbol(String key1, String key2) {
			// assert: key1 and key2 are non-null
			int hash = hash(key1, key2);
			int i = hash & (entries.length - 1);
			Entry entry = findEntry(key1, key2, entries[i], hash);
			if (entry != null) {
				entry.frequency++;
				return entry.index;				
			}
			
			// not found; add entry for key --> (index=size, freq=1) mapping
			// new entry is inserted at head of chain
//			if (DEBUG) checkNULChar(key1);
//			if (DEBUG) checkNULChar(key2);
			numChars += key1.length() + key2.length();
			if (key1.length() != 0) numChars++;
			entries[i] = new Entry(key1, key2, hash, entries[i], size); 
			if (size >= threshold) rehash();
			return size++;
		}
		
		private static Entry findEntry(String key1, String key2, Entry cursor, int hash) {
			while (cursor != null) { // scan collision chain
				if (hash == cursor.hash && eq(key2, cursor.key2) && eq(key1, cursor.key1)) { 
					cursor.key1 = key1; // speeds up future lookups: equals() vs. ==
					cursor.key2 = key2; // speeds up future lookups: equals() vs. ==
					return cursor;
				}
				cursor = cursor.next;
			}
			return null;		
		}
		
		/**
		 * Expands the capacity of this table, rehashing all entries into
		 * corresponding new slots.
		 */
		private void rehash() {
			Entry[] src = entries;
			int capacity = 2 * src.length;
			Entry[] dst = new Entry[capacity];
			
			for (int i = src.length; --i >= 0; ) {
				Entry e = src[i];
				while (e != null) { // walk collision chain
					int j = e.hash & (capacity - 1);
					Entry next = e.next;
					e.next = dst[j];
					dst[j] = e; // insert e at head of chain
					e = next;
				}
			}
			entries = dst;
			threshold = (int) (capacity * LOAD_FACTOR);
		}

		/**
		 * Returns all table entries, sorted ascending by entry.index. The
		 * result can subsequently be used to sort by symbol frequency, or
		 * similar. Much faster than an entrySet().iterator().next() loop would
		 * be.
		 */
		public Entry[] getEntries() {
			Entry[] dst = new Entry[size];
			Entry[] src = entries;
			for (int i = src.length; --i >= 0; ) {
				Entry e = src[i];
				while (e != null) { // walk collision chain
					dst[e.index] = e;
					e = e.next;
				}
			}
			return dst;
		}
		
		private static int hash(String key1, String key2) {
			int h = key2.hashCode();
			if (key1 != "") h = key1.hashCode() ^ h;
			return auxiliaryHash(h);
//			return auxiliaryHash(key1.hashCode() ^ key2.hashCode());
		}

		/**
		 * Auxiliary hash function that defends against poor base hash
		 * functions. Ensures more uniform hash distribution, hence reducing the
		 * probability of pathologically long collision chains, in particular
		 * for short key symbols that are quite similar to each other, or XML 
		 * boundary whitespace (worst case scenario).
		 */
		private static int auxiliaryHash(int h) {
			h += ~(h << 9);
			h ^= (h >>> 14);
			h += (h << 4);
			h ^= (h >>> 10);
			return h;
		}

		private static boolean eq(String x, String y) {
			return x == y || x.equals(y);
		}
			
		/** Sanity check; Unnecessary since NULs have already been checked by nu.xom.Verifier. */
		private static void checkNULChar(String key) { 
			int i = key.indexOf((char)0);
			if (i >= 0) {
				throw new IllegalArgumentException(
					"Symbol must not contain C0 control character NUL (char 0x00) [index:" + i 
					+ " within '" + key + "']");
			}
		}
		
	}
		
	/**
	 * A value in the SymbolTable.
	 */
	private static final class Entry {

		String key1; // prefix or ""
		String key2; // localName or Text or Comment or similar
		final int hash;   // cache for symbol's hash code
		final int index;  // index to correlate Entry with indexList on XML serialization
		int frequency = 1;// number of occurances of symbol within current XML document
		Entry next;       // successor in collision chain, mapping to the same hash slot

		public Entry(String key1, String key2, int hash, Entry next, int index) {
			this.key1 = key1;
			this.key2 = key2;
			this.hash = hash;
			this.next = next;
			this.index = index;
		}

		public String getKey1()    { return key1; }
		public String getKey2()    { return key2; }
		public int getIndex()     { return index; }
		public int getFrequency() { return frequency; }
		
		public String getQualifiedName() { // DEBUG only
			if (key1.length() == 0) return key2;
			return key1 + ':' + key2;
		}
		public String toString() { // DEBUG only
			return "[key1=" + key1 + ", key2=" + key2 + ", freq=" + frequency + "]";
		}
		
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Fast replacement for ArrayList and java.util.Stack. Possibly premature
	 * and unnecessary?
	 */
	private static final class FastStack {
		private Element[] elements = new Element[10];
		private int size = 0;
		
		public Element pop() {
			Element elem = elements[size-1];
			elements[--size] = null; // help gc
			return elem;
		}
		
		public void push(Element elem) {
			if (size == elements.length) ensureCapacity(size + 1);
			elements[size++] = elem;
		}
		
		private void ensureCapacity(int minCapacity) {
			if (minCapacity > elements.length) {
				int newCapacity = Math.max(minCapacity, 2 * elements.length + 1);
				elements = subArray(0, size, newCapacity);
			}
		}
		
		/** Small helper method eliminating redundancy. */
		private Element[] subArray(int from, int length, int capacity) {
			Element[] subArray = new Element[capacity];
			System.arraycopy(elements, from, subArray, 0, length);
			return subArray;
		}
		
	}
	
}