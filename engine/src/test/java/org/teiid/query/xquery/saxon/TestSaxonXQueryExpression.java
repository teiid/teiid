package org.teiid.query.xquery.saxon;

import java.util.Arrays;
import java.util.HashMap;

import net.sf.saxon.om.SequenceIterator;

import org.junit.Ignore;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.XMLNamespaces;

@Ignore
public class TestSaxonXQueryExpression {

    public static void main(String[] args) throws Exception {
    	String xquery = 
        "/Catalogs/Catalog/Items"; //$NON-NLS-1$   
    	
    	String inputdoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +  //$NON-NLS-1$
            "<Catalogs xmlns=\"foo\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n" + //$NON-NLS-1$
            "   <Catalog>\n" +  //$NON-NLS-1$
            "      <Items>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"001\" x=\"1\">\n" +  //$NON-NLS-1$
            "            <Name>Lamp</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>5</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"002\">\n" +  //$NON-NLS-1$
            "            <Name>Screwdriver</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>100</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"003\">\n" +  //$NON-NLS-1$
            "            <Name>Goat</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>4</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "      </Items>\n" +  //$NON-NLS-1$
            "   </Catalog>\n" +  //$NON-NLS-1$
            "</Catalogs>"; //$NON-NLS-1$
    	
/*    	Configuration config = new Configuration();
    	XPathExpression exp = new XPathEvaluator(config).createExpression("text()");
    	Expression expr = exp.getInternalExpression();
    	PathMap map = new PathMap(expr);
    	map.diagnosticDump(System.out);
    	PathMapRoot root = map.getContextRoot();
    	StringBuilder sb = new StringBuilder();
    	SaxonXQueryExpression.showArcs(sb, root.getArcs(), 0);
    	System.out.println(sb);
    	Source s = config.buildDocument(new StreamSource(new StringReader("<a Name='foo'><b><c Name='bar'>hello<d>world</d></c></b></a>")));
    	Object o = new XPathEvaluator(config).createExpression("//c").evaluateSingle(s);
    	XPathDynamicContext dc = exp.createDynamicContext((Item)o);
    	System.out.println(exp.evaluateSingle(dc).getStringValue());
*/    	
    	XMLNamespaces namespaces = new XMLNamespaces(Arrays.asList(new XMLNamespaces.NamespaceItem("foo", null)));
		SaxonXQueryExpression se = new SaxonXQueryExpression(xquery, namespaces, Arrays.asList(new DerivedColumn(null, new ElementSymbol("x"))), Arrays.asList(new XMLTable.XMLColumn("y", "string", "/elem", null), new XMLTable.XMLColumn("x", "string", "../@attr", null)));
		HashMap<String, Object> values = new HashMap<String, Object>();
		values.put("y", new SQLXMLImpl(inputdoc));
		SequenceIterator iter = se.evaluateXQuery(new XMLType(new SQLXMLImpl(inputdoc)), values);
	}

	
}
