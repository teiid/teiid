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
