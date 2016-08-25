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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.maven.model.Dependency;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.jboss.shrinkwrap.descriptor.spi.node.Node;
import org.jboss.shrinkwrap.descriptor.spi.node.NodeImporter;
import org.jboss.shrinkwrap.descriptor.spi.node.dom.XmlDomNodeImporterImpl;


/**
 * A pojo class used to collect module.xml from Teiid code base or Teiid wildfly-dist
 * and replace module.xml resource's 'resource-root' to 'artifact'.
 * @author kylin
 *
 */
public class Generator {
            
    private Path modulesTargetDir;
    
    private Path pomPath;
    
    private Set<String> modules;
    
    private List<String> dependencyArtifact;
    
    private Log log;
        
    private Charset charset = StandardCharsets.UTF_8;
    
    protected Generator(Path modulesTargetDir, Path pomPath, Set<String> modules) {
        this(modulesTargetDir, pomPath, modules, null, new SystemStreamLog());
    }
    
    protected Generator(Path modulesTargetDir, Path pomPath, Set<String> modules, List<String> dependencyArtifact, Log log) {
        this.modulesTargetDir = modulesTargetDir;
        this.pomPath = pomPath;
        this.modules = modules;
        this.dependencyArtifact = dependencyArtifact;
        this.log = log;
    }

    /**
     * Two aspects functions supplied:
     *   a. Replace wildfly modules's resources from resource-root to artifact, for example, a module.xml has define
     *      resource-root like
     *          <resources>
     *              <resource-root path="teiid-engine-9.1.0.Alpha2.jar" />
     *          </resources>
     *      it will be replace to
     *          <resources>
     *              <artifact name="${org.jboss.teiid:teiid-engine}" />
     *          </resources>
     *   b. Verify modules system and wildfly feature pack pom.xml, for example, if the modules system can be 
     *      'TEIID_SERVER_HOME/modules/system/layers/dv/', then feature pack pom.xml can be verified whether
     *      a module in modules system have a reference dependency in feature pack pom.xml, this can guarantee
     *      feature pack build success
     * @throws IOException
     */
    public void processGeneratorTargets() throws IOException {
        
        for(String path : modules) {
            copy(Paths.get(path).toFile(), modulesTargetDir.toFile());
            log.debug("Copy " + path + " to " + modulesTargetDir); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        if(dependencyArtifact == null){
            dependencyArtifact = getArtifactsDependencies();
        }

        Files.walkFileTree(modulesTargetDir, new SimpleFileVisitor<Path>(){

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                if(path.endsWith("module.xml")){ //$NON-NLS-1$
                    String content = new String(Files.readAllBytes(path), charset);
                 
                    Map<String, String> replacementMap = getReplacementMap(dependencyArtifact, path);
                    replacementMap.put("urn:jboss:module:1.0", "urn:jboss:module:1.3"); //$NON-NLS-1$ //$NON-NLS-2$
                    replacementMap.put("urn:jboss:module:1.1", "urn:jboss:module:1.3"); //$NON-NLS-1$ //$NON-NLS-2$
                    for(String key : replacementMap.keySet()){
                        if(replacementMap.get(key).equals("")){ //$NON-NLS-1$
                            if(content.contains(key + "\" />")){ //$NON-NLS-1$
                                content = content.replace(key + "\" />", replacementMap.get(key)); //$NON-NLS-1$
                            } else if(content.contains(key + "\"/>")){ //$NON-NLS-1$
                                content = content.replace(key + "\"/>", replacementMap.get(key)); //$NON-NLS-1$
                            }
                        } else {
                            content = content.replace(key, replacementMap.get(key));
                        }              
                    }
                    Files.write(path, content.getBytes(charset));
                    log.debug("rewrite " + path + " base on " + replacementMap); //$NON-NLS-1$ //$NON-NLS-2$
                }         
                return super.visitFile(path, attrs);
            }});
     
    }
    
    private void copy(File sourceLocation, File targetLocation) throws IOException {
        if (sourceLocation.isDirectory()) {
            copyDirectory(sourceLocation, targetLocation);
        } else {
            copyFile(sourceLocation, targetLocation);
        }
    }

    private void copyDirectory(File source, File target) throws IOException {
        if (!target.exists()) {
            target.mkdir();
        }

        for (String f : source.list()) {
            copy(new File(source, f), new File(target, f));
        }
    }

    private void copyFile(File source, File target) throws IOException {        
        try (
                InputStream in = new FileInputStream(source);
                OutputStream out = new FileOutputStream(target)
        ) {
            byte[] buf = new byte[1024];
            int length;
            while ((length = in.read(buf)) > 0) {
                out.write(buf, 0, length);
            }
        }
    }

    protected Map<String, String> getReplacementMap(List<String> dependencies, Path path) throws IllegalArgumentException, IOException{
        
        Map<String, String> replacementMap = new HashMap<>();
        
        NodeImporter importer = new XmlDomNodeImporterImpl();
        Node node = importer.importAsNode(Files.newInputStream(path), true);
        Node resources = null;
        
        // find all resources
        for(Node subNode : node.getChildren()){
            if(subNode.getName().equals("resources")){ //$NON-NLS-1$
                resources = subNode;
                break;
            }
        }
        
        if(resources == null){
            return replacementMap;
        }
        
        for(Node subNode : resources.getChildren()){
            String attr = subNode.getAttribute("path");
            if(attr != null){
                String key = "<resource-root path=\"" + attr; //$NON-NLS-1$
                String value = formReplacement(path, dependencies, attr);
                replacementMap.put(key, value);
            }
        }
             
        return replacementMap;
               
        /*
        node = node.getChildren().stream().filter(n -> n.getName().equals("resources")).collect(Collectors.toList()).get(0);
        Map<String, String> replacementMap = node.getChildren().stream().map(n -> n.getAttribute("path")).filter(p -> p != null)
            .collect(Collectors.toMap(p -> "<resource-root path=\"" + p, p -> {
                String artifactId = p.toString();
                String replacement = "";
                if(artifactId.contains(".jar")){                   
                    String suffix = findSuffix(artifactId);
                    replacement = dependencies.stream().filter(d -> d.endsWith(suffix)).findAny().orElse(null);
                    if(null != replacement){
                        replacement = "<artifact name=\"${" + replacement + "}";
                    } else {
                        throw new IllegalArgumentException(path + "'s resource-root " + p + " not define a dependency in wildfly feature pack");
                    }
                } else {
                    log.warning(path + "'s resource-root " + p + " not a jar resource, skipped");
                }
                return replacement;
            }));*/    
    }

    private String formReplacement(Path path, List<String> dependencies, String artifactId) {
        String replacement = ""; //$NON-NLS-1$
        if(artifactId.contains(".jar")){ //$NON-NLS-1$
            String suffix = findSuffix(artifactId);
            replacement = findReplacement(dependencies, suffix);
            if(null != replacement){
                replacement = "<artifact name=\"${" + replacement + "}"; //$NON-NLS-1$ //$NON-NLS-2$
            } else {
                throw new IllegalArgumentException(path + "'s resource-root " + artifactId + " not define a dependency in feature pack pom.xml"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } else {
            log.warn(path + "'s resource-root " + artifactId + " not a jar resource, skipped"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return replacement;
    }

    private String findReplacement(List<String> dependencies, String suffix) {
        for(String d : dependencies){
            if(d.endsWith(suffix)){
                return d;
            }
        }
        return null;
    }

    private String findSuffix(String artifactId) {
        if(artifactId.contains("-${")){ //$NON-NLS-1$ 
            artifactId = artifactId.substring(0, artifactId.indexOf("-${")); //$NON-NLS-1$ 
        } else {
            artifactId = artifactId.substring(0, artifactId.lastIndexOf("-")); //$NON-NLS-1$ 
            if(artifactId.endsWith("-teiid") || artifactId.endsWith("-redhat")){ //$NON-NLS-1$ 
                artifactId = artifactId.substring(0, artifactId.lastIndexOf("-")); //$NON-NLS-1$ 
            }
            while(Character.isDigit(artifactId.charAt(artifactId.length() - 1)) && artifactId.charAt(artifactId.length() - 2) == '.' && artifactId.lastIndexOf("-") > 0){ //$NON-NLS-1$  //$NON-NLS-2$ 
                artifactId = artifactId.substring(0, artifactId.lastIndexOf("-")); //$NON-NLS-1$ 
            }
        }      
        return ":" + artifactId;
    }

    protected List<String> getArtifactsDependencies() throws IOException{
        
        if(!Files.exists(pomPath)){
            throw new IllegalArgumentException("Wildfly Feature Pack pom.xml argument not exist, " + pomPath); //$NON-NLS-1$
        }
        
        MavenXpp3Reader mavenReader = new MavenXpp3Reader();
        Model model;
        try {
            model = mavenReader.read(Files.newInputStream(pomPath));
        } catch (XmlPullParserException e) {
            throw new RuntimeException(e);
        }
        MavenProject project = new MavenProject(model);  
        
        List<Dependency> dependencies = project.getDependencies();
//        return dependencies.stream().map(d -> d.getGroupId() + ":" + d.getArtifactId()).collect(Collectors.toList());
        List<String> dependencyArtifact = new ArrayList<>(dependencies.size());
        for(Dependency d : dependencies) {
            dependencyArtifact.add(d.getGroupId() + ":" + d.getArtifactId());
        }
        return dependencyArtifact;
    }
    
}
