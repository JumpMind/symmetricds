package org.jumpmind.symmetric.wrapper;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.jar.JarFile;

public class JavaAgent {
    private static Instrumentation inst;

    public static void premain(String args, Instrumentation instrumentation) {
        inst = instrumentation;
    }

    public static void addToClassPath(File jarFile) throws IOException {
        inst.appendToSystemClassLoaderSearch(new JarFile(jarFile));
    }
}
