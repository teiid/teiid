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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ExecutionFactoryProvider;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.DelegatingExecutionFactory;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;

public class TranslatorUtil {

    public static final String DEPLOYMENT_NAME = "deployment-name"; //$NON-NLS-1$

    static Map<Method, TranslatorProperty> getTranslatorProperties(Class<?> attachmentClass) {
        Map<Method, TranslatorProperty> props = new TreeMap<Method,  TranslatorProperty>(new Comparator<Method>() {
            @Override
            public int compare(Method o1, Method o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        buildTranslatorProperties(attachmentClass, props);
        return props;
    }

    static Map<Field, ExtensionMetadataProperty> getExtensionMetadataProperties(Class<?> attachmentClass) {
        Map<Field, ExtensionMetadataProperty> props = new TreeMap<Field,  ExtensionMetadataProperty>(new Comparator<Field>() {
            @Override
            public int compare(Field o1, Field o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        buildExtensionMetadataProperties(attachmentClass, props);
        return props;
    }

    private static void buildTranslatorProperties(Class<?> attachmentClass, Map<Method, TranslatorProperty> props){
        Class<?>[] baseInterfaces = attachmentClass.getInterfaces();
        for (Class<?> clazz:baseInterfaces) {
            buildTranslatorProperties(clazz, props);
        }
        Class<?> superClass = attachmentClass.getSuperclass();
        if (superClass != null) {
            buildTranslatorProperties(superClass, props);
        }
        Method[] methods = attachmentClass.getMethods();
        for (Method m:methods) {
            TranslatorProperty tp = m.getAnnotation(TranslatorProperty.class);
            if (tp != null) {
                props.put(m, tp);
            }
        }
    }

    private static void buildExtensionMetadataProperties(Class<?> attachmentClass, Map<Field, ExtensionMetadataProperty> props){
        Class<?>[] baseInterfaces = attachmentClass.getInterfaces();
        for (Class<?> clazz:baseInterfaces) {
            buildExtensionMetadataProperties(clazz, props);
        }
        Class<?> superClass = attachmentClass.getSuperclass();
        if (superClass != null) {
            buildExtensionMetadataProperties(superClass, props);
        }

        Field[] fields = attachmentClass.getDeclaredFields();
        for (Field f:fields) {
            ExtensionMetadataProperty tp = f.getAnnotation(ExtensionMetadataProperty.class);
            if (tp != null) {
                f.setAccessible(true);
                props.put(f, tp);
            }
        }
    }

    public static ExecutionFactory buildExecutionFactory(VDBTranslatorMetaData data) throws TeiidException {
        ExecutionFactory executionFactory;
        try {
            Class<?> executionClass = data.getExecutionFactoryClass();
            Object o = executionClass.newInstance();
            if(!(o instanceof ExecutionFactory)) {
                 throw new TeiidException(RuntimePlugin.Event.TEIID40024, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40024, executionClass));
            }
            executionFactory = (ExecutionFactory)o;
            synchronized (executionFactory) {
                injectProperties(executionFactory, data);
                ClassLoader orginalCL = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(executionFactory.getClass().getClassLoader());
                    executionFactory.start();
                } finally {
                    Thread.currentThread().setContextClassLoader(orginalCL);
                }
            }
            return executionFactory;
        } catch (InvocationTargetException e) {
            throw new TeiidException(RuntimePlugin.Event.TEIID40025, e);
        } catch (IllegalAccessException e) {
            throw new TeiidException(RuntimePlugin.Event.TEIID40026, e);
        } catch (InstantiationException e) {
            throw new TeiidException(CorePlugin.Event.TEIID10036, e);
        }
    }

    public static ExecutionFactory<Object, Object> buildDelegateAwareExecutionFactory(
            VDBTranslatorMetaData translator, ExecutionFactoryProvider provider)
            throws ConnectorManagerException {
        ExecutionFactory<Object, Object> ef = null;
        try {
            ef = buildExecutionFactory(translator);
        } catch (TeiidException e) {
            throw new ConnectorManagerException(e);
        }
        if (ef instanceof DelegatingExecutionFactory) {
            DelegatingExecutionFactory delegator = (DelegatingExecutionFactory)ef;
            String delegateName = delegator.getDelegateName();
            if (delegateName != null) {
                ExecutionFactory<Object, Object> delegate = provider.getExecutionFactory(delegateName);
                if (delegate == null) {
                    throw new ConnectorManagerException(
                            RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40155, delegateName));
                }
                ((DelegatingExecutionFactory<Object, Object>) ef).setDelegate(delegate);
            }
        }
        return ef;
    }

    private static void injectProperties(ExecutionFactory ef, final VDBTranslatorMetaData data) throws InvocationTargetException, IllegalAccessException, TeiidException{
        Map<Method, TranslatorProperty> props = TranslatorUtil.getTranslatorProperties(ef.getClass());
        Map<String, String> p = data.getPropertiesMap();
        TreeMap<String, String> caseInsensitiveProps = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        /*
        VDBTranslatorMetaData parent = data.getParent();
        while (parent != null) {
            for (Map.Entry<String, String> entry : parent.getPropertiesMap().entrySet()) {
                if (!caseInsensitiveProps.containsKey(entry.getKey()) && entry.getValue() != null) {
                    caseInsensitiveProps.put(entry.getKey(), entry.getValue());
                }
            }
            parent = parent.getParent();
        }
        */
        synchronized (p) {
            caseInsensitiveProps.putAll(p);
        }
        caseInsensitiveProps.remove(DEPLOYMENT_NAME);
        for (Method method:props.keySet()) {
            TranslatorProperty tp = props.get(method);
            String propertyName = getPropertyName(method);
            String value = caseInsensitiveProps.remove(propertyName);

            if (value != null) {
                Method setterMethod = getSetter(ef.getClass(), method);
                setterMethod.invoke(ef, convert(value, method.getReturnType()));
            } else if (tp.required()) {
                 throw new TeiidException(RuntimePlugin.Event.TEIID40027, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40027, tp.display()));
            }
        }
        caseInsensitiveProps.remove(Translator.EXECUTION_FACTORY_CLASS);
        if (!caseInsensitiveProps.isEmpty()) {
            LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40001, caseInsensitiveProps.keySet(), data.getName()));
        }
    }

    public static String getPropertyName(Method method) {
        String result = method.getName();
        if (result.startsWith("get")) { //$NON-NLS-1$
            return result.substring(3);
        }
        else if (result.startsWith("is")) { //$NON-NLS-1$
            return result.substring(2);
        }
        return result;
    }

    public static Method getSetter(Class<?> clazz, Method method) throws SecurityException, TeiidException {
        String setter = method.getName();
        if (method.getName().startsWith("get")) { //$NON-NLS-1$
            setter = "set"+setter.substring(3);//$NON-NLS-1$
        }
        else if (method.getName().startsWith("is")) { //$NON-NLS-1$
            setter = "set"+setter.substring(2); //$NON-NLS-1$
        }
        else {
            setter = "set"+method.getName().substring(0,1).toUpperCase()+method.getName().substring(1); //$NON-NLS-1$
        }
        try {
            return clazz.getMethod(setter, method.getReturnType());
        } catch (NoSuchMethodException e) {
            try {
                return clazz.getMethod(method.getName(), method.getReturnType());
            } catch (NoSuchMethodException e1) {
                 throw new TeiidException(RuntimePlugin.Event.TEIID40028, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40028, setter, method.getName()));
            }
        }
    }

    private static Object convert(Object value, Class<?> type) {
        if(value.getClass() == type) {
            return value;
        }

        if (value instanceof String) {
            String str = (String)value;
            return StringUtil.valueOf(str, type);
        }
        return value;
    }

    public static String getTranslatorName(ExecutionFactory factory) {
        org.teiid.translator.Translator translator = factory.getClass().getAnnotation(org.teiid.translator.Translator.class);
        if (translator == null) {
            return null;
        }
        return translator.name();
    }

    public static VDBTranslatorMetaData buildTranslatorMetadata(ExecutionFactory factory, String moduleName) {
        return buildTranslatorMetadata(factory, moduleName, true);
    }

    public static VDBTranslatorMetaData buildTranslatorMetadata(ExecutionFactory factory, String moduleName, boolean useNewInstance) {

        org.teiid.translator.Translator translator = factory.getClass().getAnnotation(org.teiid.translator.Translator.class);
        if (translator == null) {
            return null;
        }

        VDBTranslatorMetaData metadata = new VDBTranslatorMetaData();
        String see = translator.deprecated();
        if (see != null && see.length() > 0) {
            metadata.addProperty("deprecated", see); //$NON-NLS-1$
        }
        metadata.setName(translator.name());
        metadata.setDescription(translator.description());
        metadata.setExecutionFactoryClass(factory.getClass());
        metadata.setModuleName(moduleName);
        ExtendedPropertyMetadataList propertyDefns = new ExtendedPropertyMetadataList();

        try {
            Object instance = factory;
            if (useNewInstance) {
                instance = factory.getClass().newInstance();
            }
            buildTranslatorProperties(factory, metadata, propertyDefns, instance);
            buildExtensionMetadataProperties(factory, metadata, propertyDefns, instance);
        } catch (InstantiationException e) {
            // ignore
        } catch (IllegalAccessException e) {
            // ignore
        }

        metadata.addAttachment(ExtendedPropertyMetadataList.class, propertyDefns);
        return metadata;
    }

    private static void buildExtensionMetadataProperties(ExecutionFactory factory, VDBTranslatorMetaData metadata, ExtendedPropertyMetadataList propertyDefns, Object instance) {
        Class clazz = factory.getClass();
        readExtensionPropertyMetadataAsExtendedMetadataProperties(propertyDefns, clazz);

        MetadataProcessor metadataProcessor = factory.getMetadataProcessor();
        if (metadataProcessor != null) {
            clazz = metadataProcessor.getClass();
            readExtensionPropertyMetadataAsExtendedMetadataProperties(propertyDefns, clazz);
        }

    }

    private static void buildTranslatorProperties(ExecutionFactory factory, VDBTranslatorMetaData metadata, ExtendedPropertyMetadataList propertyDefns, Object instance) {
        Class clazz = factory.getClass();
        readTranslatorPropertyAsExtendedMetadataProperties(metadata, propertyDefns, instance, clazz);

        MetadataProcessor metadataProcessor = factory.getMetadataProcessor();
        if (metadataProcessor != null) {
            clazz = metadataProcessor.getClass();
            readTranslatorPropertyAsExtendedMetadataProperties(metadata, propertyDefns, metadataProcessor, clazz);
            readTranslatorPropertyAsExtendedMetadataProperties(metadata, propertyDefns, new MetadataFactory(), MetadataFactory.class);
        }
    }

    private static void readExtensionPropertyMetadataAsExtendedMetadataProperties(
            ExtendedPropertyMetadataList propertyDefns, Class clazz) {
        Map<Field, ExtensionMetadataProperty> tps = TranslatorUtil.getExtensionMetadataProperties(clazz);
        for (Field f:tps.keySet()) {

            ExtensionMetadataProperty tp = tps.get(f);
            ExtendedPropertyMetadata epm = new ExtendedPropertyMetadata();
            epm.category = PropertyType.EXTENSION_METADATA.name();

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < tp.applicable().length; i++) {
                sb.append(tp.applicable()[i].getName());
                if (tp.applicable().length-1 > i) {
                    sb.append(","); //$NON-NLS-1$
                }
            }
            epm.owner = sb.toString();
            try {
                epm.name = (String)f.get(null);
            } catch (IllegalArgumentException e) {
                continue;
            } catch (IllegalAccessException e) {
                continue;
            }
            epm.description = tp.description();
            epm.displayName = tp.display();
            epm.required = tp.required();
            epm.dataType = tp.datatype().getName();

            // allowed values
            if (tp.allowed() != null && !tp.allowed().isEmpty()) {
                epm.allowed = new ArrayList<String>();
                StringTokenizer st = new StringTokenizer(tp.allowed(), ","); //$NON-NLS-1$
                while (st.hasMoreTokens()) {
                    epm.allowed.add(st.nextToken().trim());
                }
            }
            propertyDefns.add(epm);
        }
    }

    private static void readTranslatorPropertyAsExtendedMetadataProperties(
            VDBTranslatorMetaData metadata,
            ExtendedPropertyMetadataList propertyDefns, Object instance,
            Class clazz) {
        Map<Method, TranslatorProperty> tps = TranslatorUtil.getTranslatorProperties(clazz);
        for (Method m:tps.keySet()) {

            Object defaultValue = getDefaultValue(instance, m, tps.get(m));

            TranslatorProperty tp = tps.get(m);
            boolean importProperty = tp.category()==TranslatorProperty.PropertyType.IMPORT;
            if (defaultValue != null && !importProperty) {
                metadata.addProperty(getPropertyName(m), defaultValue.toString());
            }

            ExtendedPropertyMetadata epm = new ExtendedPropertyMetadata();
            epm.category = tp.category().name();
            epm.name = importProperty?"importer."+getPropertyName(m):getPropertyName(m); //$NON-NLS-1$
            epm.description = tp.description();
            epm.advanced = tp.advanced();
            if (defaultValue != null) {
                epm.defaultValue = defaultValue.toString();
            }
            epm.displayName = tp.display();
            epm.masked = tp.masked();
            epm.required = tp.required();
            epm.dataType = m.getReturnType().getCanonicalName();

            // allowed values
            if (m.getReturnType().isEnum()) {
                epm.allowed = new ArrayList<String>();
                Object[] constants = m.getReturnType().getEnumConstants();
                for( int i=0; i<constants.length; i++ ) {
                    epm.allowed.add(((Enum<?>)constants[i]).name());
                }
                epm.dataType = "java.lang.String"; //$NON-NLS-1$
            }
            propertyDefns.add(epm);
        }
    }

    private static Object convert(Object instance, Method method, TranslatorProperty prop) {
        Class<?> type = method.getReturnType();
        String[] allowedValues = null;
        Method getter = null;
        boolean readOnly = prop.readOnly();
        if (type == Void.TYPE) { //check for setter
            Class<?>[] types = method.getParameterTypes();
            if (types.length != 1) {
                 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40029, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40029, method));
            }
            type = types[0];
            try {
                getter = instance.getClass().getMethod("get" + method.getName(), (Class[])null); //$NON-NLS-1$
            } catch (Exception e) {
                try {
                    getter = instance.getClass().getMethod("get" + method.getName().substring(3), (Class[])null); //$NON-NLS-1$
                } catch (Exception e1) {
                    //can't find getter, won't set the default value
                }
            }
        } else if (method.getParameterTypes().length != 0) {
             throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40029, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40029, method));
        } else {
            getter = method;
            try {
                TranslatorUtil.getSetter(instance.getClass(), method);
            } catch (Exception e) {
                if (!readOnly) {
                    throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40146, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40146, method));
                }
            }
        }
        Object defaultValue = null;
        if (prop.required()) {
            if (prop.advanced()) {
                 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40031, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40031,method));
            }
        } else if (getter != null) {
            try {
                defaultValue = getter.invoke(instance, (Object[])null);
            } catch (Exception e) {
                //no simple default value
            }
        }
        if (type.isEnum()) {
            Object[] constants = type.getEnumConstants();
            allowedValues = new String[constants.length];
            for( int i=0; i<constants.length; i++ ) {
                allowedValues[i] = ((Enum<?>)constants[i]).name();
            }
            type = String.class;
            if (defaultValue != null) {
                defaultValue = ((Enum<?>)defaultValue).name();
            }
        }
        if (!(defaultValue instanceof Serializable)) {
            defaultValue = null; //TODO
        }
        return defaultValue;
    }

    public static Object getDefaultValue(Object instance, Method method, TranslatorProperty prop) {
        return convert(instance, method, prop);
    }

    @SuppressWarnings({"rawtypes","unchecked"})
    public static ExecutionFactory<Object, Object> getExecutionFactory(String name, TranslatorRepository vdbRepo, TranslatorRepository repo, VDBMetaData deployment, IdentityHashMap<Translator, ExecutionFactory<Object, Object>> map, HashSet<String> building) throws ConnectorManagerException {
        if (!building.add(name)) {
            throw new ConnectorManagerException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40138, building));
        }
        VDBTranslatorMetaData translator = vdbRepo.getTranslatorMetaData(name);
        if (translator == null) {
            translator = repo.getTranslatorMetaData(name);
        }
        if (translator == null) {
            return null;
        }
        ExecutionFactory<Object, Object> ef = map.get(translator);
        if ( ef == null) {
            try {
                ef = TranslatorUtil.buildExecutionFactory(translator);
            } catch (TeiidException e) {
                throw new ConnectorManagerException(e);
            }
            if (ef instanceof DelegatingExecutionFactory) {
                DelegatingExecutionFactory delegator = (DelegatingExecutionFactory)ef;
                String delegateName = delegator.getDelegateName();
                if (delegateName != null) {
                    ExecutionFactory<Object, Object> delegate = getExecutionFactory(delegateName, vdbRepo, repo, deployment, map, building);
                    if (delegate == null) {
                        throw new ConnectorManagerException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40136, delegateName));
                    }
                    ((DelegatingExecutionFactory<Object, Object>) ef).setDelegate(delegate);
                }
            }
            map.put(translator, ef);
        }
        return ef;
    }

}
