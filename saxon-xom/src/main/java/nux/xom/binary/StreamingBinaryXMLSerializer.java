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

import java.io.IOException;
import java.io.OutputStream;

import nu.xom.Comment;
import nu.xom.DocType;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.IllegalAddException;
import nu.xom.Node;
import nu.xom.ProcessingInstruction;
import nu.xom.Text;
import nux.xom.io.StreamingSerializer;

/**
 * {@link nux.xom.io.StreamingSerializer} implementation that serializes bnux
 * binary XML to the given underlying output stream, using the given ZLIB
 * compression level.
 * 
 * @author whoschek@lbl.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.17 $, $Date: 2004/05/24 20:56:16
 */
final class StreamingBinaryXMLSerializer implements StreamingSerializer {

	private final BinaryXMLCodec codec;
	private final OutputStream out;
	private final int zlibCompressionLevel;
	
	/** The codec doesn't perform wellformedness checking, so we do it here. */
	private final StreamingVerifier verifier;
	
	private static final boolean DEBUG = false;
		
	StreamingBinaryXMLSerializer(BinaryXMLCodec codec, OutputStream out, int zlibCompressionLevel) {
		if (codec == null) 
			throw new IllegalArgumentException("codec must not be null");
		if (out == null) 
			throw new IllegalArgumentException("output stream must not be null");
		if (zlibCompressionLevel < 0 || zlibCompressionLevel > 9)
			throw new IllegalArgumentException("Compression level must be 0..9");
		
		this.codec = codec;
		this.out = out;
		this.zlibCompressionLevel = zlibCompressionLevel;
		this.verifier = new StreamingVerifier();
	}
	
	private void reset() {
		verifier.reset();
	}
		
	/** {@inheritDoc} */
	public void flush() throws IOException {
		if (verifier.depth() > 0) codec.flush(false);
	}
	
	/** {@inheritDoc} */
	public void writeStartTag(Element elem) throws IOException {
		verifier.writeStartTag(elem);
		codec.writeStartTag(elem);
	}
	
	/** {@inheritDoc} */
	public void writeEndTag() throws IOException {
		verifier.writeEndTag();
		codec.writeEndTag();
	}
	
	/** {@inheritDoc} */
	public void write(Document doc) throws IOException {
		writeXMLDeclaration();
		for (int i = 0; i < doc.getChildCount(); i++) {
			writeChild(doc.getChild(i));
		}
		writeEndDocument();
	}
	
	/** {@inheritDoc} */
	public void write(Element element) throws IOException {
		// fast path:
		verifier.writeStartTag(element);
		codec.writeElement(element);
		verifier.writeEndTag();
		
//		// slow path:
//		writeStartTag(element);
//		for (int i=0; i < element.getChildCount(); i++) {
//			writeChild(element.getChild(i));
//		}
//		writeEndTag();
	}
	
	/** {@inheritDoc} */
	public void write(Text text) throws IOException {
		verifier.write(text);
		codec.writeText(text); // TODO: coalesce Texts if indent > 0
	}
	
	/** {@inheritDoc} */
	public void write(Comment comment) throws IOException {
		verifier.write(comment);
		codec.writeComment(comment);
	}
	
	/** {@inheritDoc} */
	public void write(ProcessingInstruction instruction) throws IOException {
		verifier.write(instruction);
		codec.writeProcessingInstruction(instruction);
	}
	
	/** {@inheritDoc} */
	public void write(DocType docType) throws IOException {
		verifier.write(docType);
		codec.writeDocType(docType);
	}
	
	/** {@inheritDoc} */
	public void writeEndDocument() throws IOException {
		for (int i=verifier.depth(); --i >= 0; ) {
			writeEndTag(); // close all remaining open tags 
		}
		verifier.writeEndDocument();
		codec.writeEndDocument(); // implicitly calls codec.flush()
		reset();
	}

	/** {@inheritDoc} */
	public void writeXMLDeclaration() throws IOException {
		verifier.writeXMLDeclaration(); // do sanity check
		reset();
		verifier.writeXMLDeclaration(); // sets hasXMLDeclaration = true
		codec.setOutputStream(zlibCompressionLevel, out);
		codec.writeXMLDeclaration("");
	}
	
	private void writeChild(Node node) throws IOException {
		if (node instanceof Element) {
			write((Element) node);
		} else if (node instanceof Text) {
			write((Text) node);
		} else if (node instanceof Comment) {
			write((Comment) node);
		} else if (node instanceof ProcessingInstruction) {
			write((ProcessingInstruction) node);
		} else if (node instanceof DocType) {
			write((DocType) node);
		} else {
			throw new IllegalAddException("Cannot write node type: " + node);
		}
	}
}
