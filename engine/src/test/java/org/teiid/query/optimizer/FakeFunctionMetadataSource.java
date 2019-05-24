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

package org.teiid.query.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.AggregateAttributes;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionParameter;
import org.teiid.query.function.FunctionMetadataSource;

@SuppressWarnings("nls")
public class FakeFunctionMetadataSource implements FunctionMetadataSource {

    public Collection<org.teiid.metadata.FunctionMethod> getFunctionMethods() {
        List<org.teiid.metadata.FunctionMethod> methods = new ArrayList<org.teiid.metadata.FunctionMethod>();
        methods.add(new FunctionMethod("xyz", "", "misc", PushDown.MUST_PUSHDOWN,  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                       FakeFunctionMetadataSource.class.getName(), "xyz", //$NON-NLS-1$
                                       null,
                                       new FunctionParameter("out", "integer"), true, Determinism.DETERMINISTIC)); //$NON-NLS-1$ //$NON-NLS-2$

        FunctionParameter p1 = new FunctionParameter("astring", "string");  //$NON-NLS-1$  //$NON-NLS-2$
        FunctionParameter result = new FunctionParameter("trimstring", "string"); //$NON-NLS-1$  //$NON-NLS-2$

        FunctionMethod method = new FunctionMethod("MYRTRIM", "", "", FakeFunctionMetadataSource.class.getName(), "myrtrim", new FunctionParameter[] {p1}, result);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        method.setPushdown(PushDown.CAN_PUSHDOWN);
        methods.add(method);

        FunctionMethod method2 = new FunctionMethod("misc.namespace.func", "", "", null, null, new FunctionParameter[] {p1}, result);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        method2.setPushdown(PushDown.MUST_PUSHDOWN);
        methods.add(method2);

        FunctionMethod method3 = new FunctionMethod("parsedate_", "", "", null, null, new FunctionParameter[] {p1}, new FunctionParameter("", DataTypeManager.DefaultDataTypes.DATE));  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        method3.setPushdown(PushDown.MUST_PUSHDOWN);
        methods.add(method3);

        FunctionMethod method4 = new FunctionMethod("FIRST_VALUE", "", "", null, null, new FunctionParameter[] {p1},result);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        method4.setPushdown(PushDown.MUST_PUSHDOWN);
        method4.setAggregateAttributes(new AggregateAttributes());
        methods.add(method4);

        return methods;
    }

    public Class<?> getInvocationClass(String className) throws ClassNotFoundException {
        return Class.forName(className, true, this.getClass().getClassLoader());
    }

    // dummy function
    public static Object xyz() {
        return null;
    }

    /** defect 15348*/
    public static Object myrtrim(Object astring) {
        String string = (String)astring;
        return string.trim();
    }

    @Override
    public ClassLoader getClassLoader() {
        return null;
    }
}
