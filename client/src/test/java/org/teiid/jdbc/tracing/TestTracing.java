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

package org.teiid.jdbc.tracing;

import static org.junit.Assert.*;

import org.junit.Test;

import io.opentracing.Scope;
import io.opentracing.mock.MockTracer;

@SuppressWarnings("nls")
public class TestTracing {

    @Test public void testSpanContextInjection() {
        MockTracer tracer = new MockTracer();
        assertNull(GlobalTracerInjector.getSpanContext(tracer));
        Scope ignored = tracer.buildSpan("x").startActive(true);
        try {
            assertEquals("{\"spanid\":\"2\",\"traceid\":\"1\"}", GlobalTracerInjector.getSpanContext(tracer));
        } finally {
            ignored.close();
        }
    }

}
