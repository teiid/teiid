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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.Logger;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.Column;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.*;
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
        assertEquals(1, importProperties.size());
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
        TranslatorUtil.buildExecutionFactory(metadata);
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
	
	@Translator(name="my-translator")
	public static class MyTranslator extends ExecutionFactory<Object, Object> {
	    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, description="description", required=true)
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

                @TranslatorProperty(display="Import Property", category=PropertyType.IMPORT)
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
	
	@Translator(name="my-translator2")
	public static class MyTranslator2 extends MyTranslator1{
		public MyTranslator2() {
			setSomeProperty("original-assigned");
		}
	}	
}
