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

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.Logger;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.Column;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;

@SuppressWarnings("nls")
public class TestTranslatorUtil {

    @Test
    public void testInitialSetValueExecutionFactory() throws Exception {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();

        tm.setExecutionFactoryClass(MyTranslator2.class);
        MyTranslator2 my = (MyTranslator2)TranslatorUtil.buildExecutionFactory(tm);

        assertEquals("original-assigned", my.getSomeProperty());
    }

    @Test
    public void testBuildExecutionFactory() throws Exception {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();

        tm.addProperty("MyProperty", "correctly-assigned");
        tm.setExecutionFactoryClass(MyTranslator.class);
        MyTranslator my = (MyTranslator)TranslatorUtil.buildExecutionFactory(tm);

        assertEquals("correctly-assigned", my.getMyProperty());

        assertNull(my.other());

        tm.addProperty("other", "foo");
        tm.setExecutionFactoryClass(MyTranslator.class);
        my = (MyTranslator)TranslatorUtil.buildExecutionFactory(tm);

        assertEquals("foo", my.other());
    }

    @Test
    public void testImportProperties() throws Exception {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();
        tm.setExecutionFactoryClass(MyTranslator.class);
        tm.addProperty("MyProperty", "correctly-assigned");

        MyTranslator my = (MyTranslator)TranslatorUtil.buildExecutionFactory(tm);

        VDBTranslatorMetaData translator = TranslatorUtil.buildTranslatorMetadata(my, "my-module");
        ExtendedPropertyMetadataList props = translator.getAttachment(ExtendedPropertyMetadataList.class);

        ArrayList<ExtendedPropertyMetadata> importProperties = new ArrayList<ExtendedPropertyMetadata>();
        for (ExtendedPropertyMetadata prop:props) {
            if (prop.category().equals(PropertyType.IMPORT.name())) {
                importProperties.add(prop);
            }
        }
        assertEquals(7, importProperties.size());
        assertEquals("importer.ImportProperty", importProperties.get(0).name);
        assertEquals("java.lang.String", importProperties.get(0).dataType);
        assertEquals(false, importProperties.get(0).required);
        assertEquals(false, importProperties.get(0).advanced);
        assertEquals("", importProperties.get(0).description);
        assertEquals("Import Property", importProperties.get(0).displayName);
        assertEquals(true, importProperties.get(0).editable);
        assertEquals(false, importProperties.get(0).masked);
        assertEquals("default-import-property", importProperties.get(0).defaultValue);
    }

    @Test
    public void testInject() throws Exception {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();
        tm.setExecutionFactoryClass(MyTranslator.class);
        tm.addProperty("MyProperty", "correctly-assigned");

        MyTranslator my = (MyTranslator)TranslatorUtil.buildExecutionFactory(tm);
        assertEquals("correctly-assigned", my.getMyProperty());

        VDBTranslatorMetaData metadata = TranslatorUtil.buildTranslatorMetadata(my, "my-module");
        metadata.addProperty("MyProperty", "correctly-assigned");

        Logger logger = Mockito.mock(Logger.class);
        Mockito.stub(logger.isEnabled(Mockito.anyString(), Mockito.anyInt())).toReturn(true);
        Mockito.doThrow(new RuntimeException("fail")).when(logger).log(Mockito.eq(MessageLevel.WARNING), Mockito.eq(LogConstants.CTX_RUNTIME), Mockito.anyString());
        LogManager.setLogListener(logger);
        try {
            TranslatorUtil.buildExecutionFactory(metadata);
        } finally {
            LogManager.setLogListener(null);
        }
    }

    @Test
    public void testExtensionMetadataProperties() throws Exception {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();
        tm.setExecutionFactoryClass(MyTranslator.class);
        tm.addProperty("MyProperty", "correctly-assigned");

        MyTranslator my = (MyTranslator)TranslatorUtil.buildExecutionFactory(tm);

        VDBTranslatorMetaData translator = TranslatorUtil.buildTranslatorMetadata(my, "my-module");
        ExtendedPropertyMetadataList props = translator.getAttachment(ExtendedPropertyMetadataList.class);

        ArrayList<ExtendedPropertyMetadata> importProperties = new ArrayList<ExtendedPropertyMetadata>();
        for (ExtendedPropertyMetadata prop:props) {
            if (prop.category().equals(PropertyType.EXTENSION_METADATA.name())) {
                importProperties.add(prop);
            }
        }
        assertEquals("{http://teiid.org}/my-extension-property", importProperties.get(0).name());
        assertEquals("java.lang.String", importProperties.get(0).datatype());
        assertEquals("org.teiid.metadata.Column", importProperties.get(0).owner());
        assertArrayEquals(new String[] {"TOBE", "NOTTOBE"}, importProperties.get(0).allowed());
    }

    @Test
    public void testBuildExecutionFactoryWithDefaults() throws Exception {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();
        VDBTranslatorMetaData parent = new VDBTranslatorMetaData();
        parent.addProperty("myProperty", "default");
        parent.setExecutionFactoryClass(MyTranslator.class);
        tm.setParent(parent);
        tm.addProperty("MyProperty", "correctly-assigned");

        MyTranslator my = (MyTranslator)TranslatorUtil.buildExecutionFactory(tm);

        assertEquals("correctly-assigned", my.getMyProperty());
    }

    @Test public void testBuildExecutionFactoryCaseInsensitive() throws Exception {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();

        tm.addProperty("myproperty", "correctly-assigned");
        tm.setExecutionFactoryClass(MyTranslator.class);
        MyTranslator my = (MyTranslator)TranslatorUtil.buildExecutionFactory(tm);

        assertEquals("correctly-assigned", my.getMyProperty());
    }

    @Test public void testBuildExecutionFactory1() throws Exception {
        VDBTranslatorMetaData tm = new VDBTranslatorMetaData();

        tm.addProperty("someproperty", "correctly-assigned");
        tm.setExecutionFactoryClass(MyTranslator1.class);
        MyTranslator1 my = (MyTranslator1)TranslatorUtil.buildExecutionFactory(tm);

        assertNull(my.getMyProperty());
        assertEquals("correctly-assigned", my.getSomeProperty());
    }

    @Test(expected=TeiidRuntimeException.class) public void testReadOnly() throws Exception {
        TranslatorUtil.buildTranslatorMetadata(new MyTranslatorInvalid(), "x");
    }

    @Translator(name="my-translator")
    public static class MyTranslator extends ExecutionFactory<Object, Object> {
        @ExtensionMetadataProperty(applicable = Column.class, datatype = String.class,
                description = "description", required = true, allowed = "TOBE, NOTTOBE")
        public static final String EXTENSION_PROP = "{http://teiid.org}/my-extension-property";

        String mine;
        String other;

        @TranslatorProperty(display="my-property", required=true)
        public String getMyProperty() {
            return mine;
        }

        public void setMyProperty(String value) {
            this.mine = value;
        }

        @TranslatorProperty(display="other")
        public String other() {
            return other;
        }

        public void setOther(String other) {
            this.other = other;
        }

        @Override
        public MetadataProcessor<Object> getMetadataProcessor() {
            return new MetadataProcessor<Object> () {

                @Override
                public void process(MetadataFactory metadataFactory,Object connection) throws TranslatorException {
                }

                @TranslatorProperty(display="Import Property", category=PropertyType.IMPORT, readOnly=true)
                public String getImportProperty() {
                    return "default-import-property";
                }
            };
        }
    }

    public interface SomeProperty {

        @TranslatorProperty(display="my-property", required=false)
        String getSomeProperty();

        void setSomeProperty(String value);

    }

    @Translator(name="my-translator1")
    public static class MyTranslator1 extends MyTranslator implements SomeProperty {

        private String someProperty;

        @Override
        @TranslatorProperty(display="my-property", required=false)
        public String getMyProperty() {
            return super.getMyProperty();
        }

        @Override
        public String getSomeProperty() {
            return someProperty;
        }

        @Override
        public void setSomeProperty(String value) {
            this.someProperty = value;
        }

    }

    @Translator(name="my-translator-invalid")
    public static class MyTranslatorInvalid extends ExecutionFactory<Object, Object> {

        @TranslatorProperty(display="my-property")
        public String getMyProperty() {
            return "x";
        }

    }

    @Translator(name="my-translator2")
    public static class MyTranslator2 extends MyTranslator1{
        public MyTranslator2() {
            setSomeProperty("original-assigned");
        }
    }
}
