package org.teiid.translator.xml.streaming;

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

import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.CriteriaDesc;
import org.teiid.translator.xml.Document;
import org.teiid.translator.xml.ExecutionInfo;
import org.teiid.translator.xml.OutputXPathDesc;
import org.teiid.translator.xml.XMLPlugin;


/**
 * The ElementProcessor extracts data from a Node based upon XPaths defined in
 * an ExecutionInfo (aka columns in a query request) ro build a single result row.
 * In this context Node is equivalent to a table in the model.  
 * 
 * The processor is also responsible putting the cacheKey in the response row,
 * inserting projected parameters, and applying = criteria.
 *
 */
public class ElementProcessor {

	private ExecutionInfo info;
	private Object[] row;
	private Map<String, OutputXPathDesc> resultPaths;
	private OutputXPathDesc cacheKeyColumn;
	private Map<String, String> namespacesToPrefixMap;
	private boolean rowExcluded = false;
    
	public ElementProcessor(ExecutionInfo info) throws TranslatorException {
		this.info = info;
		resultPaths = generateXPaths(info.getRequestedColumns());
		namespacesToPrefixMap = info.getNamespaceToPrefixMap();
	}
	
	/**
	 * Iterate down the element getting column data from the matching paths.
	 * @param element the Node representing the Table in the model.
	 * @return a single result row
	 */
	public Object[] process(Node element) {
		setRowExcluded(false);
		row = new Object[resultPaths.size()];
		listChildren(element, "");
		return row;
	}
	
	/**
	 * Iterate over the result and insert the ResponseId in the correct column
	 * @param xml the XML Document
	 * @param result the result batch for the query
	 */
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
		
		if(current.getDocument().equals(current.getParent())) {
			path = getLocalQName(current);
		}
		
	    if (current instanceof Element) {
	        Element temp = (Element) current;
	        for (int i = 0; i < temp.getAttributeCount(); i++) {
	          Attribute attribute = temp.getAttribute(i);
	          String attrPath = path + "/@" + getLocalQName(attribute);
	          if(resultPaths.containsKey(attrPath)) {
		          handleNode(attribute, attrPath);
		          if(isRowExcluded()) {
		        	  return;
		          }
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
	    else if (current instanceof Text) {
	    	String textPath = path + "/text()";
	    	if(resultPaths.containsKey(textPath)) {
	        	  handleNode(current, textPath);
	        	  if(isRowExcluded()) {
		        	  return;
		          }
	          }
	    }
	    
	    for (int i = 0; i < current.getChildCount(); i++) {
	    	if(isRowExcluded()) {
	        	  return;
	        }
	    	Node next = current.getChild(i);
	    	String childPath = path;
	    	if (next instanceof Element) {
		        Element temp = (Element) next;
		        if(path.isEmpty()) {
		        	childPath = getLocalQName(temp);
		        } else {
		        	childPath= path + '/' + getLocalQName(temp);
		        }
	    	}
	    	listChildren(next, childPath);
	    }
	  }

	/**
	 * Get the qualified name for the Element, but replace the prefixes
	 * from the actual doc with the matching prefix from the model.  Without
	 * this prefix we can't do a proper path comparison.
	 * @throws TranslatorException 
	 */
	private String getLocalQName(Node node) {
		String namespaceURI = null;
		String localName = null;
		if(node instanceof Element) {
			Element element = (Element)node;
			namespaceURI = element.getNamespaceURI();
			localName = element.getLocalName();
		} else if (node instanceof Attribute) {
			Attribute attribute = (Attribute)node;
			namespaceURI = attribute.getNamespaceURI();
			localName = attribute.getLocalName();
		}
		if(null == namespaceURI) {
			throw new Error("namespce URI not found in model namespaces");
		}
		String prefix = namespacesToPrefixMap.get(namespaceURI);
		String result;
		if(null == prefix) {
			result = localName;
		} else {
			result = prefix + ':' + localName;
		}
		return result;
	}
	
	private void handleNode(Node node, String parentPath) {
		OutputXPathDesc columnMetadata = resultPaths.get(parentPath);
		int columnNum = columnMetadata.getColumnNumber();

		if(!passesCriteriaCheck(info.getCriteria(), node.getValue(), columnNum)) {
			setRowExcluded(true);
			return;
		} else {
			//TODO: type conversion
			row[columnNum] = node.getValue();
		}
	}
	
    /**
     * Tests the value against the criteria to determine if the value should be
     * included in the result set.
     * 
     * @param criteriaPairs
     * @param value
     * @param colNum
     * @return
     * @throws TranslatorException
     */
    private static boolean passesCriteriaCheck(List<CriteriaDesc> criteriaPairs,
            String value, int colNum) {
        // Need to test this code
        for (CriteriaDesc criteria: criteriaPairs) {
            if (colNum == criteria.getColumnNumber()) {
                return evaluate(value, criteria);
            }
        }
        return true;
    }
    
    public static boolean evaluate(String currentValue,
            CriteriaDesc criteria) {
        // this is the criteriaq for the col
        List values = criteria.getValues();
        for (Object criterion: values) {
            if (criterion.equals(currentValue)) {
                return true;
            }
        }
        return false; // no matching value

    }

	private Map<String, OutputXPathDesc> generateXPaths(List<OutputXPathDesc> columns) throws TranslatorException {
		Map<String, OutputXPathDesc> xpaths = new HashMap<String, OutputXPathDesc>();
		for (OutputXPathDesc xPathDesc: columns) {
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
			throws TranslatorException {
		String retval;
		if (xpath.indexOf('|') != -1 && xpath.indexOf('(') == -1) {
			// We are forcing compound XPaths to have parents, first reason is
			// that we
			// should never produce them in the importer, second reason is that
			// it makes
			// this function easier to fix under our current time constraints.
			throw new TranslatorException(XMLPlugin.getString("Executor.unsupported.compound.xpath"));//$NON-NLS-1$ 
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

	private void setRowExcluded(boolean excluded) {
		rowExcluded = excluded;
		row = null;
	}
	
	private boolean isRowExcluded() {
		return rowExcluded;
	}

}
