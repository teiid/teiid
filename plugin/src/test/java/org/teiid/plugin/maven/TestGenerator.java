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
package org.teiid.plugin.maven;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.plugin.maven.Generator;

public class TestGenerator {
    
    private static Generator generator;
    
    private boolean dumpDependencies = false;
    
    @BeforeClass
    public static void init() throws IOException {
        Path codeDir = Paths.get("../");
        Set<String> modules = new HashSet<>();
        try(BufferedReader br = new BufferedReader(new InputStreamReader(Generator.class.getClassLoader().getResourceAsStream("teiid-codebase-modules.txt")))){ //$NON-NLS-1$
            for(String line; (line = br.readLine()) != null; ){
                Path modulePath = Paths.get(codeDir.toString(), line.trim());
                modules.add(modulePath.toString());
            }
        }
        Path targetDir = Paths.get("target", "unit-test", "modules/system/layers/dv");
        if(!Files.exists(targetDir)){
            Files.createDirectories(targetDir);
        }
        Path pomPath = Paths.get("src/test/resources/feature-pack-pom.xml");
        generator = new Generator(targetDir, pomPath, modules);
//        System.out.println();
    }
    
    @Test
    public void testCopyModules() throws IOException {
        generator.processGeneratorTargets();
        Path targetDir = Paths.get("target", "unit-test", "modules/system/layers/dv");
        final AtomicInteger count = new AtomicInteger(0);
        Files.walkFileTree(targetDir, new SimpleFileVisitor<Path>(){

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if(file.endsWith("module.xml")){
                    count.incrementAndGet();
                }
                return super.visitFile(file, attrs);
            }});
        // current code base contain 85 modules, each time add new module, feature pack generator should aware it. 
        assertEquals(count.get(), 85);     
    }

    @Test
    public void testArtifactsDependencies() throws IOException {
        List<String> dependencies = generator.getArtifactsDependencies();
        // current feature pack has 148 dependencies, each time add new module, the reference dependency also should be added
        assertEquals(149, dependencies.size());
        if(dumpDependencies){
            for(String str : dependencies){
                System.out.println(str);
            }
        }   
    }
    
    @Test
    public void testTeiidMainModuleReplacementMap() throws IOException {
        List<String> dependencies = generator.getArtifactsDependencies();
        Path path = Paths.get("src/test/resources/modules/teiid.main.xml");
        Map<String, String> replacementMap = generator.getReplacementMap(dependencies, path);
        assertTrue(replacementMap.keySet().contains("<resource-root path=\"teiid-engine-${project.version}.jar"));
        assertEquals("<artifact name=\"${org.jboss.teiid:teiid-engine}", replacementMap.get("<resource-root path=\"teiid-engine-${project.version}.jar"));
        assertEquals("", replacementMap.get("<resource-root path=\"deployments"));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testErrorModuleReplacementMap() throws IOException {
        List<String> dependencies = generator.getArtifactsDependencies();
        Path path = Paths.get("src/test/resources/modules/modules-error.xml");
        generator.getReplacementMap(dependencies, path);
    }
    
    @Test
    public void testReplacementDirect() throws IOException {
        Charset charset = StandardCharsets.UTF_8;
        Path path = Paths.get("src/test/resources/modules/teiid.main.xml");
        String content = new String(Files.readAllBytes(path), charset);
        assertTrue(content.contains("<resource-root path=\"teiid-engine-${project.version}.jar"));
        content = content.replace("<resource-root path=\"teiid-engine-${project.version}.jar", "<artifact name=\"${org.jboss.teiid:teiid-engine}");
        assertFalse(content.contains("<resource-root path=\"teiid-engine-${project.version}.jar"));
    }

}
