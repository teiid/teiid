package com.metamatrix.connector.xml;

import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.connector.xml.streaming.InvalidPathException;
import com.metamatrix.connector.xml.streaming.XPathSplitter;

public class TestXPathSplitter extends TestCase {

	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}

	/**
	 *  ((((/po:purchaseOrder) | (/po:purchaseOrders/order))/items) | (/po:purchaseOrder/items3) | (/po:purchaseOrders/order/items4))/item
	 *  
	 *   becomes
	 *   /po:purchaseOrder/items/item
	 *   /po:purchaseOrders/order/items/item
	 *   /po:purchaseOrder/items/item
	 *   /po:purchaseOrders/order/items/item
	 *   becomes
	 *   
	 *  /po:purchaseOrder/items/item
	 *	/po:purchaseOrders/order/items/item
	 */
	public void testSplit() {
		XPathSplitter splitter = new XPathSplitter();
		try {
			List paths = splitter.split("((((/po:purchaseOrder) | (/po:purchaseOrders/order))/items) | (/po:purchaseOrder/items) | (/po:purchaseOrders/order/items))/item");
			assertEquals(2, paths.size());
		} catch (InvalidPathException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSplitSimple() {
		XPathSplitter splitter = new XPathSplitter();
		try {
			List paths = splitter.split("/po:purchaseOrders/order/items/item");
			assertEquals(1, paths.size());
		} catch (InvalidPathException e) {
			fail(e.getMessage());
		}
	}
	
	public void testSplitCompund() {
		XPathSplitter splitter = new XPathSplitter();
		try {
			List paths = splitter.split("(/po:purchaseOrders/order/items/item)|(/po:purchaseOrders/order/items)");
			assertEquals(2, paths.size());
		} catch (InvalidPathException e) {
			fail(e.getMessage());
		}
	}

	public void testSplitCompund2() {
		XPathSplitter splitter = new XPathSplitter();
		try {
			List paths = splitter.split("(/po:purchaseOrders/order/items/item)|((/po:purchaseOrders/order/items)|(/po:purchaseOrders/order/item))");
			assertEquals(3, paths.size());
		} catch (InvalidPathException e) {
			fail(e.getMessage());
		}
	}
	
	/**
	 * ((/po:purchaseOrders/order/items)|(/po:purchaseOrders/order/item))|((/po:purchaseOrders/order/items)|(/po:purchaseOrders/order/item))
	 * 
	 * /po:purchaseOrders/order/items
	 * /po:purchaseOrders/order/item
	 * 
	 */
	public void testSplitCompund3() {
		XPathSplitter splitter = new XPathSplitter();
		try {
			List paths = splitter.split("((/po:purchaseOrders/order/items)|(/po:purchaseOrders/order/item))|((/po:purchaseOrders/order/items)|(/po:purchaseOrders/order/item))");
			assertEquals(2, paths.size());
		} catch (InvalidPathException e) {
			fail(e.getMessage());
		}
	}
}
