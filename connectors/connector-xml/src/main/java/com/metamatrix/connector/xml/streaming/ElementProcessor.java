package com.metamatrix.connector.xml.streaming;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import nu.xom.Attribute;
import nu.xom.DocType;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.ProcessingInstruction;
import nu.xom.Text;

import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.xml.Document;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.OutputXPathDesc;
import com.metamatrix.connector.xml.http.Messages;

/**
 * The ElementProcessor extracts data from a Node based upon paths defined in
 * an ExecutionInfo (aka columns in a query request).  The processor is also 
 * responsible putting the cacheKey in the response row.
 *
 */
public class ElementProcessor {

	private ExecutionInfo info;
	private Object[] row;
	private Map<String, OutputXPathDesc> resultPaths;
	private OutputXPathDesc cacheKeyColumn;
    
	public ElementProcessor(ExecutionInfo info) throws ConnectorException {
		this.info = info;
		resultPaths = generateXPaths();
	}
	
	/**
	 * Iterate down the element getting column data from the matching paths.
	 * @param element
	 * @param xml
	 * @return a single result row
	 */
	public Object[] process(Node element) {
		row = new Object[resultPaths.size()];
		listChildren(element, "");
		return row;
	}
	
	public void insertResponseId(Document xml, List<Object[]> result) {
		if (null != cacheKeyColumn) {
			Object[] aRow;
			if (!result.isEmpty()) {
				for (Iterator<Object[]> iter = result.iterator(); iter.hasNext();) {
					aRow = iter.next();
					aRow[cacheKeyColumn.getColumnNumber()] = xml.getCachekey();
				}
			} else {
				aRow = new Object[resultPaths.size()];
				aRow[cacheKeyColumn.getColumnNumber()] = xml.getCachekey();
				result.add(aRow);
			}
		}
	}
	
	private void insertProjectedParameters() {
		//TODO insertProjectedParameters
	}

	/**
	 * Match the current path against the Map of requested paths and add
	 * the matches to the result row.
	 * @param current
	 * @param path
	 */
	private void listChildren(Node current, String path) {
		   
	    if (current instanceof Element) {
	        Element temp = (Element) current;
	        for (int i = 0; i < temp.getAttributeCount(); i++) {
	          Attribute attribute = temp.getAttribute(i);
	          String attrPath = path + '@' + attribute.getQualifiedName();
	          if(resultPaths.containsKey(attrPath)) {
	        	  getColumn(attribute, attrPath);
	          }
	        }
	    }
	    else if (current instanceof ProcessingInstruction) {
	        ProcessingInstruction temp = (ProcessingInstruction) current;
	        temp.getTarget();   
	    }
	    else if (current instanceof DocType) {
	        DocType temp = (DocType) current;
	        path = path + '/' + temp.getRootElementName();   
	    }
	    else if (current instanceof Text /*|| current instanceof Comment*/) {
	    	String textPath = path + "/text()";
	    	if(resultPaths.containsKey(textPath)) {
	        	  getColumn(current, textPath);
	          }
	    }
	    
	    for (int i = 0; i < current.getChildCount(); i++) {
	    	Node next = current.getChild(i);
	    	String childPath = path;
	    	if (next instanceof Element) {
		        Element temp = (Element) next;
		        if(path.isEmpty()) {
		        	childPath = temp.getQualifiedName();
		        } else {
		        	childPath= path + '/' + temp.getQualifiedName();
		        }
	    	}
	    	listChildren(next, childPath);
	    }
	  }

	
    private void getColumn(Node node, String path) {
		OutputXPathDesc columnMetadata = resultPaths.get(path);
    	int columnNum = columnMetadata.getColumnNumber();
    	//TODO: type conversion
		row[columnNum] = node.getValue();
		
	}

	private Map<String, OutputXPathDesc> generateXPaths() throws ConnectorException {
		Map<String, OutputXPathDesc> xpaths = new HashMap<String, OutputXPathDesc>();
		OutputXPathDesc xPathDesc = null;
		for (Iterator<OutputXPathDesc> iter = info.getRequestedColumns().iterator(); iter.hasNext(); ) {
			xPathDesc = iter.next();
			String xpathString = null;
			if (!xPathDesc.isResponseId()) {
				xpathString = xPathDesc.getXPath();
				if (xpathString != null) {
					xpathString = relativizeAbsoluteXpath(xpathString);
				}
			} else {
				cacheKeyColumn = xPathDesc;
			}
			xpaths.put(xpathString, xPathDesc);
		}
		return xpaths;
	}

    private String relativizeAbsoluteXpath(String xpath)
			throws ConnectorException {
		String retval;
		if (xpath.indexOf('|') != -1 && xpath.indexOf('(') == -1) {
			// We are forcing compound XPaths to have parents, first reason is
			// that we
			// should never produce them in the importer, second reason is that
			// it makes
			// this function easier to fix under our current time constraints.
			throw new ConnectorException(Messages
					.getString("Executor.unsupported.compound.xpath"));//$NON-NLS-1$ 
		} else if (xpath.equals("/")) {//$NON-NLS-1$ 
			retval = ".";//$NON-NLS-1$ 
		} else if (xpath.startsWith("/") && !(xpath.startsWith("//"))) {//$NON-NLS-1$ //$NON-NLS-2$ 
			retval = xpath.substring(1);
		} else if (xpath.startsWith("(")) {//$NON-NLS-1$ 
			xpath = xpath.replaceAll("\\(/", "("); // change (/ to ( //$NON-NLS-1$ //$NON-NLS-2$  
			xpath = xpath.replaceAll("\\|/", "|"); // change |/ to | //$NON-NLS-1$ //$NON-NLS-2$
			retval = xpath;
		} else {
			retval = xpath;
		}
		return retval;
	}


}
