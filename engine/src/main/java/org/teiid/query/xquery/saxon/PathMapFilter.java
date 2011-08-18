package org.teiid.query.xquery.saxon;

import java.util.LinkedList;
import java.util.List;

import net.sf.saxon.event.ProxyReceiver;
import net.sf.saxon.expr.AxisExpression;
import net.sf.saxon.expr.PathMap.PathMapArc;
import net.sf.saxon.expr.PathMap.PathMapNode;
import net.sf.saxon.expr.PathMap.PathMapRoot;
import net.sf.saxon.om.Axis;
import net.sf.saxon.pattern.NodeTest;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.Type;

/**
 * A filter that uses the PathMap to determine what should be included in the document
 * 
 * TODO: optimize filtering by not reconstructing the matchcontexts
 * TODO: we may still need to do xom/nux style handling of large results, but 
 *       that requires more analysis to determine subtree independence
 */
class PathMapFilter extends ProxyReceiver {
	
	static class MatchContext {
		List<PathMapArc> elementArcs;
		List<PathMapArc> attributeArcs;
		boolean matchedElement;
		boolean matchesText;
		boolean matchesComment;
		
		void bulidContext(PathMapNode node) {
			for (PathMapArc arc : node.getArcs()) {
    			processArc(arc);
			}
		}

		void processArc(PathMapArc arc) {
			AxisExpression ae = arc.getStep();
			NodeTest test = ae.getNodeTest();
			if (test == null) {
				addAnyNodeArc(arc);
			} else {
				switch (test.getPrimitiveType()) {
				case Type.TEXT:
					matchesText = true;
				case Type.NODE:
					addAnyNodeArc(arc);
					break;
				case Type.COMMENT:
					matchesComment = true;
					break;
				case Type.ELEMENT:
					addElementArc(arc);
					break;
				case Type.ATTRIBUTE:
					addAttributeArc(arc);
					break;
				}
			}
		}

		private void addAnyNodeArc(PathMapArc arc) {
			if (arc.getStep().getAxis() == Axis.ATTRIBUTE) {
				addAttributeArc(arc);
				return;	
			}
			addElementArc(arc);
			addAttributeArc(arc);
			matchesText = true;
			matchesComment = true;
		}

		private void addAttributeArc(PathMapArc arc) {
			if (attributeArcs == null) {
				attributeArcs = new LinkedList<PathMapArc>();
			}
			attributeArcs.add(arc);
		}

		private void addElementArc(PathMapArc arc) {
			if (elementArcs == null) {
				elementArcs = new LinkedList<PathMapArc>();
			}
			elementArcs.add(arc);
		}
	}

	
	private LinkedList<MatchContext> matchContext = new LinkedList<MatchContext>();
	
	public PathMapFilter(PathMapRoot root) {
		MatchContext mc = new MatchContext();
		mc.bulidContext(root);
		matchContext.add(mc);
	}
	
	@Override
	public void startElement(int nameCode, int typeCode,
			int locationId, int properties)
			throws XPathException {
		MatchContext mc = matchContext.getLast();
		MatchContext newContext = new MatchContext();
		if (mc.elementArcs != null) {
			for (PathMapArc arc : mc.elementArcs) {
				AxisExpression ae = arc.getStep();
				NodeTest test = ae.getNodeTest();
				if (test == null || test.matches(Type.ELEMENT, nameCode, typeCode)) {
					newContext.bulidContext(arc.getTarget());
					newContext.matchedElement = true;
				} 
				if (ae.getAxis() == Axis.DESCENDANT || ae.getAxis() == Axis.DESCENDANT_OR_SELF) {
					newContext.processArc(arc);
				}
			}
		}
		matchContext.add(newContext);
		if (newContext.matchedElement) {
			super.startElement(nameCode, typeCode, locationId, properties);
		}
	}
	
	@Override
	public void attribute(int nameCode, int typeCode,
			CharSequence value, int locationId, int properties)
			throws XPathException {
		MatchContext mc = matchContext.getLast();
		if (!mc.matchedElement) {
			return;
		}
		if (mc.attributeArcs != null) {
			for (PathMapArc arc : mc.attributeArcs) {
				AxisExpression ae = arc.getStep();
				NodeTest test = ae.getNodeTest();
				if (test == null || test.matches(Type.ATTRIBUTE, nameCode, typeCode)) {
					super.attribute(nameCode, typeCode, value, locationId, properties);
					return;
				} 
			}
		}
	}

	@Override
	public void characters(CharSequence chars, int locationId,
			int properties) throws XPathException {
		MatchContext context = matchContext.getLast();
		if (context.matchedElement && context.matchesText) {
			super.characters(chars, locationId, properties);
		}
	}

	@Override
	public void comment(CharSequence chars, int locationId,
			int properties) throws XPathException {
		MatchContext context = matchContext.getLast();
		if (context.matchedElement && context.matchesComment) {
			super.comment(chars, locationId, properties);
		}
	}

	@Override
	public void processingInstruction(String target,
			CharSequence data, int locationId, int properties)
			throws XPathException {
		MatchContext context = matchContext.getLast();
		if (context.matchedElement) {
			super.processingInstruction(target, data, locationId, properties);
		}
	}

	@Override
	public void namespace(int namespaceCode, int properties)
			throws XPathException {
		MatchContext context = matchContext.getLast();
		if (context.matchedElement) {
			super.namespace(namespaceCode, properties);
		}
	}

	@Override
	public void endElement() throws XPathException {
		MatchContext context = matchContext.removeLast();
		if (context.matchedElement) {
			super.endElement();
		}
	}
	
	@Override
	public void startContent() throws XPathException {
		MatchContext context = matchContext.getLast();
		if (context.matchedElement) {
			super.startContent();
		}
	}

}