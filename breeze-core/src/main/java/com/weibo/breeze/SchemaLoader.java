package com.weibo.breeze;

import com.weibo.breeze.message.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.www.protocol.file.FileURLConnection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by zhanglei28 on 2019/5/21.
 */
public class SchemaLoader {
    public static final String PATH = "META-INF/breeze/";
    public static final String SUFFIX = ".breeze";

    private static final Logger logger = LoggerFactory.getLogger(SchemaLoader.class);
    private static Map<String, Schema> schemas = new ConcurrentHashMap<>();
    private static final Schema noSchema = Schema.newSchema("noSchema");

    public static Schema loadSchema(String className) {
        className = Breeze.getCleanName(className);
        Schema schema = schemas.get(className);
        if (schema == null) {
            synchronized (className.intern()) {
                schema = schemas.get(className);
                if (schema == null) {
                    InputStream inputStream;
                    try {
                        inputStream = SchemaLoader.class.getResourceAsStream("/" + PATH + className + SUFFIX);
                        if (inputStream != null) {
                            schema = SchemaUtil.parseSchema(readContentAndClose(inputStream));
                        }
                    } catch (Exception e) {
                        logger.warn("read breeze schema fail. class:" + className + ", error:" + e.getMessage());
                    }
                    if (schema == null) {
                        schema = noSchema;
                    } else {
                        logger.info("read breeze schema success for class: " + className);
                    }
                    schemas.put(className, schema);
                }
            }
        }
        if (schema == noSchema) {
            schema = null;
        }
        return schema;
    }

    public static boolean loadAllSchema() {
        try {
            Enumeration<URL> enumeration = SchemaLoader.class.getClassLoader().getResources(PATH);
            while (enumeration.hasMoreElements()) {
                URLConnection connection = enumeration.nextElement().openConnection();
                if (connection instanceof JarURLConnection) {
                    try (JarFile jarFile = ((JarURLConnection) connection).getJarFile()) {
                        Enumeration<JarEntry> entryEnumeration = jarFile.entries();
                        while (entryEnumeration.hasMoreElements()) {
                            String name = entryEnumeration.nextElement().getName().trim();
                            if (name.startsWith(PATH) && name.endsWith(SUFFIX)) {
                                loadSchema(name.substring(PATH.length(), name.length() - SUFFIX.length()));
                            }
                        }
                    }
                } else if (connection instanceof FileURLConnection) {
                    String[] content = readContentAndClose(connection.getInputStream()).split("\n");
                    for (String name : content) {
                        name = name.trim();
                        if (name.endsWith(SUFFIX)) {
                            loadSchema(name.substring(0, name.length() - SUFFIX.length()));
                        }
                    }

                }
            }
            return true;
        } catch (IOException e) {
            logger.warn("load all breeze schema fail. error:" + e.getMessage());
            return false;
        }
    }

    private static String readContentAndClose(InputStream inputStream) throws IOException {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream(512);
            byte[] buf = new byte[512];
            int len;
            while ((len = inputStream.read(buf)) != -1) {
                output.write(buf, 0, len);
            }
            return output.toString("UTF-8");
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

}
