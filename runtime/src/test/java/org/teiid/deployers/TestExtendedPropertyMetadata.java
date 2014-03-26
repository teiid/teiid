/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.deployers;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("nls")
public class TestExtendedPropertyMetadata {

	@Test
	public void testDefault() {
		ExtendedPropertyMetadata metadata = new ExtendedPropertyMetadata("x", "java.lang.String", "some-name", null);
		
		Assert.assertEquals("some-name", metadata.display());
		Assert.assertEquals(null, metadata.description());
		Assert.assertEquals(false, metadata.advanced());
		Assert.assertEquals(false, metadata.required());
		Assert.assertEquals(false, metadata.masked());
		Assert.assertEquals(false , metadata.readOnly());
	}
	
	@Test
	public void testFormatted() {
		ArrayList<String> allowed =  new ArrayList<String>();
		allowed.add("get");
		allowed.add("post");
		
		ExtendedPropertyMetadata metadata = new ExtendedPropertyMetadata("x", "java.lang.String", "{$display:\"Is Immutable\",$description:\"True if the source never changes.\",$allowed:[\"get\",\"post\"], $required:\"true\",$advanced:\"true\"}", null);
		
		Assert.assertEquals("Is Immutable", metadata.display());
		Assert.assertEquals("True if the source never changes.", metadata.description());
		Assert.assertEquals(true, metadata.advanced());
		Assert.assertEquals(true, metadata.required());
		Assert.assertEquals(false, metadata.masked());
		Assert.assertEquals(false , metadata.readOnly());		
		Assert.assertEquals(allowed , Arrays.asList(metadata.allowed()));
	}
	
	@Test
	public void testFormattedExtraCommasAndColons() {
		ArrayList<String> allowed =  new ArrayList<String>();
		allowed.add("get");
		allowed.add("post");
		
		ExtendedPropertyMetadata metadata = new ExtendedPropertyMetadata("x", "java.lang.String","{$display:\"Is Immu:table\",$description:\"True if the, source never changes.\",$allowed:[\"get\",\"post\"], $required:\"true\",$advanced:\"true\"}", null);
		
		Assert.assertEquals("Is Immu:table", metadata.display());
		Assert.assertEquals("True if the, source never changes.", metadata.description());
		Assert.assertEquals(true, metadata.advanced());
		Assert.assertEquals(true, metadata.required());
		Assert.assertEquals(false, metadata.masked());
		Assert.assertEquals(false , metadata.readOnly());		
		Assert.assertEquals(allowed , Arrays.asList(metadata.allowed()));
	}	
	
	@Test
	public void testBlankProperties() {
		ArrayList<String> allowed =  new ArrayList<String>();
		allowed.add("get");
		allowed.add("post");
		
		ExtendedPropertyMetadata metadata = new ExtendedPropertyMetadata("x", "java.lang.String","{$display:\"Is Immutable\",$description:\"\",$allowed:[\"get\",\"post\"], $required:\"true\",$advanced:\"true\"}", null);
		
		Assert.assertEquals("Is Immutable", metadata.display());
		Assert.assertEquals("", metadata.description());
		Assert.assertEquals(true, metadata.advanced());
		Assert.assertEquals(true, metadata.required());
		Assert.assertEquals(false, metadata.masked());
		Assert.assertEquals(false , metadata.readOnly());		
		Assert.assertEquals(allowed , Arrays.asList(metadata.allowed()));
	}	
}
