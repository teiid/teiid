package com.metamatrix.connector.xml.streaming;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.NodeFactory;
import nu.xom.Nodes;
import nux.xom.pool.XOMUtil;
import nux.xom.xquery.StreamingTransform;

public class StreamingMultiPathFilter {
	
	private PathPackages pathPackages;
	
	public StreamingMultiPathFilter(List<String> paths, Map<String, String> prefixes) 
		throws InvalidPathException {

		pathPackages = new PathPackages();
		Iterator<String> iter = paths.iterator();
		while(iter.hasNext()) {
			pathPackages.addPackage(getPathPackage(iter.next(), prefixes));
		}
	}
	
	private PathPackage getPathPackage(String locationPath, Map<String, String> prefixes) throws InvalidPathException {
		if (locationPath == null) 
			throw new InvalidPathException("locationPath must not be null");
		if (locationPath.indexOf("//") >= 0)
			throw new InvalidPathException("DESCENDANT axis is not supported");
		
		String path = locationPath.trim();
		if (path.startsWith("/")) path = path.substring(1);
		if (path.endsWith("/")) path = path.substring(0, path.length() - 1);
		path = path.trim();
		if (path.equals("")) path = "*:*"; // fixup "match anything"
		String[] localNames = path.split("/");
		String[] namespaceURIs = new String[localNames.length];

		// parse prefix:localName pairs and resolve prefixes to namespaceURIs
		for (int i = 0; i < localNames.length; i++) {
			int k = localNames[i].indexOf(':');
			if (k >= 0 && localNames[i].indexOf(':', k+1) >= 0)
				throw new InvalidPathException(
					"QName must not contain more than one colon: "
					+ "qname='" + localNames[i] + "', path='" + path + "'");
			if (k <= 0) {
				namespaceURIs[i] = ""; // no namespace
			} else {
				String prefix = localNames[i].substring(0, k).trim();
				if (k >= localNames[i].length() - 1)
					throw new InvalidPathException(
						"Missing localName for prefix: " + "prefix='"
						+ prefix + "', path='" + path + "', prefixes=" + prefixes);
				if (prefix.equals("*")) {
					// namespace is irrelevant (does not matter)
					namespaceURIs[i] = null;
				} else {
					// lookup namespace of uri
					if (prefixes == null) 
						throw new InvalidPathException("prefixes must not be null");
					Object uri = prefixes.get(prefix);
					if (uri == null)
						throw new InvalidPathException(
							"Missing namespace for prefix: "
							+ "prefix='" + prefix + "', path='" + path
							+ "', prefixes=" + prefixes);
					namespaceURIs[i] = uri.toString().trim();
				}
			} // end if
			
			localNames[i] = localNames[i].substring(k + 1).trim();
			//if (localNames[i].equals("*")) {
				// localName is irrelevant (does not matter)
			//	localNames[i] = null;
			//}
		}
		return new PathPackage(localNames, namespaceURIs);
	}

	public NodeFactory createNodeFactory(NodeFactory childFactory, StreamingTransform transform) {
		if (childFactory == null) 
			childFactory = XOMUtil.getIgnoreWhitespaceOnlyTextNodeFactory();
		return new StreamingMultiPathFilterNodeFactory(pathPackages, childFactory, transform);
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////
	private class StreamingMultiPathFilterNodeFactory extends NodeFactory {
		
		private PathPackages pathPackages;
		private NodeFactory child;
		private StreamingTransform transform;

		private int level; // current nesting level = current location path step
		private Element mismatch; // last element that did not match path

		private final Nodes NONE = new Nodes();
		private static final boolean DEBUG = false;
		
		public StreamingMultiPathFilterNodeFactory(PathPackages packages, 
				NodeFactory child, StreamingTransform transform) {
			this.pathPackages = packages;
			this.child = child;
			this.transform = transform;
		}
		
		@Override
		public Document startMakingDocument() {
			// reset state
			level = -1;
			mismatch = null;
			return child.startMakingDocument();
		}
		
		@Override
		public Element startMakingElement(String qname, String namespaceURI) {
			level++;
			if (mismatch == null && level < pathPackages.longestPath) {
				if (!pathPackages.isRequired(level,qname, namespaceURI)) {
					mismatch = super.startMakingElement(qname, namespaceURI);
					return mismatch;
				}
			}
			if (mismatch == null) {
				return super.startMakingElement(qname, namespaceURI);
			} else {
				level--;
				return null;
			}
		}
		
		@Override
		public Nodes finishMakingElement(Element elem) {
			if (level == 0) {
				// check for / match
				if (pathPackages.isMatch(level,elem.getQualifiedName(), elem.getNamespaceURI())) {
					return transformMatch(elem);
				} //causes nu.xom.WellformednessException: Factory attempted to remove the root element on the request
				mismatch = null;
				level--;
				return super.finishMakingElement(elem);
			}
			if (elem == mismatch) {
				mismatch = null;
				level--;
				return NONE;
			}
			if (pathPackages.isMatch(level,elem.getQualifiedName(), elem.getNamespaceURI())) {
				return transformMatch(elem);
			}

			level--;
			if (level < pathPackages.getLongestPath() -1 && !hasChildElements(elem)) {
				return NONE;
			}
			return super.finishMakingElement(elem);
		}

		private Nodes transformMatch(Element elem) {
			level--;
			if (transform == null) return super.finishMakingElement(elem);
			Nodes results = transform.transform(elem);
			
			if(results.size() == 0) {
				results = new Nodes(elem);
			} else {
				for (int i = results.size(); --i >= 0; ) {
					Node node = results.get(i);
					if (node != elem) node.detach();
				}
			}
			return results;
		}
		
		private boolean hasChildElements(Element elem) {
			for (int i = elem.getChildCount(); --i >= 0;) {
				if (elem.getChild(i) instanceof Element) return true;
			}
			return false;
		}

		@Override
		public Nodes makeComment(String data) {
			return mismatch == null ? child.makeComment(data) : NONE;
		}

		@Override
		public Nodes makeText(String data) {
			if (mismatch == null) {
				if (level == 0 && isWhitespaceOnly(data)) 
					return NONE; // avoid accumulating whitespace garbage in root element (i.e. avoid hidden memory leak)
				else 
					return child.makeText(data);
			}
			return NONE;
		}

		@Override
		public Nodes makeAttribute(String qname, String URI, String value, Attribute.Type type) {
			return mismatch == null ? child.makeAttribute(qname, URI, value, type) : NONE;
		}

		@Override
		public Nodes makeProcessingInstruction(String target, String data) {
			return mismatch == null ? child.makeProcessingInstruction(target, data) : NONE;
		}
		
		@Override
		public Nodes makeDocType(String rootElementName, String publicID, String systemID) {
			return child.makeDocType(rootElementName, publicID, systemID);
		}

		@Override
		public void finishMakingDocument(Document document) {
			child.finishMakingDocument(document);
		}
		
		/** see XML spec */
		private boolean isWhitespace(char c) {
			switch (c) {
				case '\t': return true;
				case '\n': return true;
				case '\r': return true;
				case ' ' : return true;
				default  : return false;
			}
		}
		
		private boolean isWhitespaceOnly(String str) {
			for (int i=str.length(); --i >= 0; ) {
				if (!isWhitespace(str.charAt(i))) return false; 
			}
			return true;
		}

	}
	
	/**
	 * Contains matched XML local names and namespace URIs for a single path.
	 * 
	 *
	 */
	private class PathPackage {

		String[] localNames;
		String[] namespaceURIs;
		
		public PathPackage(String[] localNames, String[] namespaceURIs) {
			this.localNames = localNames;
			this.namespaceURIs = namespaceURIs;
		}

		/**
		 *  
		 * @return the length of the path in local names.
		 */
		public int getPathLength() {
			return localNames.length;
		}

		/**
		 * Determines if an element is in the hierarchy of matching elements.
		 * @param level
		 * @param localName
		 * @param namespaceURI
		 * @return
		 */
		public boolean isRequired(int level, String localName, String namespaceURI) {
			String name = localNames[level];
			String uri = namespaceURIs[level];
			if(level == 0 && name.equals("*")) {
				return true;
			} else {
			return
				(name == null || name.equals(localName)) && 
				(uri == null || uri.equals(namespaceURI));
			}
		}

		public boolean hasLevelMatch(int level) {
			return level == localNames.length -1;
		}

		public boolean isMatch(int level, String localName, String namespaceURI) {
			if(level < getPathLength()) {
				if(level == localNames.length -1) {
					return isRequired(level, localName, namespaceURI);
				}
			}
			return false;
		}
		
	}
	
	private class PathPackages {
		
		private List<PathPackage> packages;
		private int longestPath = 0;
		
		public PathPackages() {
			packages = new ArrayList<PathPackage>();
		}
		
		public boolean isMatch(int level, String qname,
				String namespaceURI) {
			int i = qname.indexOf(':') + 1;
			String localName = qname.substring(i);
			Iterator<PathPackage> iter = packages.iterator();
			while (iter.hasNext()) {
				PathPackage pack = iter.next();
				if(pack.isMatch(level, localName, namespaceURI)) {
					return true;
				}
			}
			return false;
		}

		public boolean hasMatch(Element elem) {
			return false;
		}

		public boolean hasLevelMatch(int level) {
			Iterator<PathPackage> iter = packages.iterator();
			while (iter.hasNext()) {
				PathPackage pack = iter.next();
				if(pack.hasLevelMatch(level)) {
					return true;
				}
			}
			return false;
		}

		/**
		 *  * Determines if an element is in the hierarchy of matching elements.
		 * @param level
		 * @param qname
		 * @param namespaceURI
		 * @return
		 */
		public boolean isRequired(int level, String qname, String namespaceURI) {
			int i = qname.indexOf(':') + 1;
			String localName = qname.substring(i);
			Iterator<PathPackage> iter = packages.iterator();
			while (iter.hasNext()) {
				PathPackage pack = iter.next();
				if(level < pack.getPathLength() && pack.isRequired(level, localName, namespaceURI)) {
					return true;
				}
			}
			return false;
		}

		PathPackages addPackage(PathPackage pack) {
			if(null != pack) {
				packages.add(pack);
				if(pack.getPathLength() > longestPath) {
					longestPath = pack.getPathLength();
				}
			}
			return this;
		}
		
		public int getLongestPath() {
			return longestPath;
		}
	}
}
