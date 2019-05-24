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

package org.teiid.xquery.saxon;

import java.io.IOException;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;

import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.util.CharsetUtils;
import org.teiid.util.WSUtil;

import net.sf.saxon.om.Item;
import net.sf.saxon.om.NameChecker;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.QNameException;
import net.sf.saxon.sxpath.XPathDynamicContext;
import net.sf.saxon.sxpath.XPathEvaluator;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;

public class XMLFunctions {

    private static final boolean USE_X_ESCAPE = PropertiesUtils.getHierarchicalProperty("org.teiid.useXMLxEscape", true, Boolean.class); //$NON-NLS-1$

    public static String xpathValue(Object doc, String xpath) throws XPathException, TeiidProcessingException {
        Source s = null;
        try {
            s = XMLSystemFunctions.convertToSource(doc);
            XPathEvaluator eval = new XPathEvaluator();
            // Wrap the string() function to force a string return
            XPathExpression expr = eval.createExpression(xpath);
            XPathDynamicContext context = expr.createDynamicContext(eval.getConfiguration().buildDocumentTree(s).getRootNode());
            Object o = expr.evaluateSingle(context);

            if(o == null) {
                return null;
            }

            // Return string value of node type
            if(o instanceof Item) {
                Item i = (Item)o;
                if (isNull(i)) {
                    return null;
                }
                return i.getStringValue();
            }

            // Return string representation of non-node value
            return o.toString();
        } finally {
            WSUtil.closeSource(s);
        }
    }

    /**
     * Validate whether the XPath is a valid XPath.  If not valid, an XPathExpressionException will be thrown.
     * @param xpath An xpath expression, for example: a/b/c/getText()
     */
    public static void validateXpath(String xpath) throws TeiidProcessingException {
        if(xpath == null) {
            return;
        }

        XPathEvaluator eval = new XPathEvaluator();
        try {
            eval.createExpression(xpath);
        } catch (XPathException e) {
            throw new TeiidProcessingException(e);
        }
    }

    public static String[] validateQName(String name) throws TeiidProcessingException {
        try {
            return NameChecker.getQNameParts(name);
        } catch (QNameException e) {
            throw new TeiidProcessingException(e);
        }
    }

    public static boolean isValidNCName(String prefix) {
        return NameChecker.isValidNCName(prefix);
    }

    public static boolean isNull(Item i) {
        if (i instanceof NodeInfo) {
            NodeInfo ni = (NodeInfo)i;
            return ni.getNodeKind() == net.sf.saxon.type.Type.ELEMENT && !ni.hasChildNodes() && Boolean.valueOf(ni.getAttributeValue(XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI, "nil")); //$NON-NLS-1$
            /* ideally we'd be able to check for nilled, but that doesn't work without validation
             if (ni.isNilled()) {
                tuple.add(null);
                continue;
            }*/
        }
        return false;
    }

    public static String escapeName(String name, boolean fully) {
        StringBuilder sb = new StringBuilder();
        char[] chars = name.toCharArray();
        int i = 0;
        boolean surrogatePair = false;
        if (fully && name.regionMatches(true, 0, "xml", 0, 3)) { //$NON-NLS-1$
            escapeChar(sb, name.charAt(0));
            sb.append(chars, 1, 2);
            i = 3;
        }
        for (; i < chars.length; i++) {
            char chr = chars[i];
            switch (chr) {
            case ':':
                if (fully || i == 0) {
                    escapeChar(sb, chr);
                    continue;
                }
                break;
            case '_':
                if (chars.length > i+1 && chars[i+1] == 'x') {
                    escapeChar(sb, chr);
                    continue;
                }
                break;
            default:
                int codepoint = chr;
                if (i+1 < chars.length && Character.isSurrogatePair(chr, chars[i+1])) {
                    i++;
                    codepoint = Character.toCodePoint(chr, chars[i]);
                    surrogatePair = true;
                }
                if (i == (surrogatePair?1:0)) {
                    if (!NameChecker.isNCNameStartChar(codepoint)) {
                        escapeChar(sb, codepoint);
                        continue;
                    }
                } else if (!NameChecker.isNCNameChar(codepoint)) {
                    escapeChar(sb, codepoint);
                    continue;
                }
                break;
            }
            sb.append(chr);
            if (surrogatePair) {
                sb.append(chars[i]);
            }
        }
        return sb.toString();
    }

    private static void escapeChar(StringBuilder sb, int chr) {
        boolean bmp = Character.isBmpCodePoint(chr);
        if (USE_X_ESCAPE) {
            sb.append("_x");  //$NON-NLS-1$
        } else {
            sb.append("_u");  //$NON-NLS-1$
        }
        try {
            if (!bmp) {
                byte high = (byte)(chr >> 16);
                CharsetUtils.toHex(sb, high);
            }
            CharsetUtils.toHex(sb, (byte)(chr >> 8));
            CharsetUtils.toHex(sb, (byte)chr);
        } catch (IOException e) {
            throw new TeiidRuntimeException(e);
        }
        sb.append("_");  //$NON-NLS-1$
    }

}
