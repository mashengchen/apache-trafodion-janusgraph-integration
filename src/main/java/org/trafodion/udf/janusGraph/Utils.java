package org.trafodion.udf.janusGraph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class Utils {
    private static String host;
    private static int port;
    private static long timeStamp;
    private static String yamlFile;

    static {
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static String getHost() {
        if (isConfFileUpdated()) {
            reload();
        }
        return host;
    }

    public static int getPort() {
        if (isConfFileUpdated()) {
            reload();
        }
        return port;
    }

    private static boolean isConfFileUpdated() {
        File file = new File(yamlFile);
        long ts = file.lastModified();
        if (timeStamp != ts) {
            timeStamp = ts;
            // Yes, file is updated
            return true;
        }
        // No, file is not updated
        return false;
    }

    private static void reload() {
        try {
            Settings s = Settings.read(new FileInputStream(new File(yamlFile)));
            host = s.hosts.get(0);
            port = s.port;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    private static String genJanusGraphFileContent() {
        // hosts: [192.168.0.11]
        // port: 8182
        StringBuffer sb = new StringBuffer();
        // generally JanusGraph is in dcs master machine.
        host = System.getenv("DCS_MASTER_HOST");
        if (host == null || host.length() == 0) {
            host = "localhost";
        }
        port = 8182;
        sb.append("hosts: [").append(host).append("]").append(System.getProperty("line.separator")).append("port: ")
                .append(port);

        return sb.toString();
    }

    private static synchronized void init() throws IOException {
        String homeDir = System.getenv("MY_SQROOT");
        if (System.getenv("MY_SQROOT") == null || System.getenv("MY_SQROOT").length() == 0) {
            homeDir = System.getenv("TRAF_HOME");
        }
        String fileDir = homeDir + "/udr/public/external_libs";
        File path = new File(fileDir);
        if (!path.exists()) {
            path.mkdirs();
        }
        yamlFile = homeDir + "/udr/public/external_libs/janus.yaml";
        File yaml = new File(yamlFile);
        if (!yaml.exists()) {
            yaml.createNewFile();
            FileOutputStream out = new FileOutputStream(yaml);
            out.write(genJanusGraphFileContent().getBytes());
            out.flush();
            out.close();
        } else {
            reload();
        }
        timeStamp = yaml.lastModified();
    }
}
