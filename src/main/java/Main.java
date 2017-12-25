import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

public class Main {

    public static void main(String[] args) throws Exception {
        // regexTest();

        String query = "g.V().match(__.as('directors').hasLabel('person'),__.as('directors').outE('role').has('roletype',eq('director')).inV().as('directed_movies'),__.as('directed_movies').inE('role').has('roletype',eq('actor')).outV().as('directors')).select('directors','directed_movies').by(values('personid')).by(values('movieid'))";
        String query1 = "g.V().match(__.as('directors').hasLabel('person'),__.as('directors').outE('role').has('roletype',eq('director')).inV().as('directed_movies'),__.as('directed_movies').inE('role').has('roletype',eq('actor')).outV().as('directors')).select('directors','directed_movies').dedup().group().by(select('directors').by('personid')).by(select('directed_movies').by('movieid').fold()).unfold()";
        String query2 = "g.V().match(__.as('directors').hasLabel('person'),__.as('directors').outE('role').has('roletype',eq('director')).inV().as('directed_movies'),__.as('directed_movies').inE('role').has('roletype',eq('actor')).outV().as('directors')).select('directors').dedup().order().by('personid').values('personid')";
        String update = "graph.addVertex(label, 'person', 'personid', 1511); graph.tx().commit()";
        String query3 = "g.V().has('personid',1511).outE('role').has('roletype',eq('director')).inV().as('source').values('movieid').as('movies').select('source').inE().has('roletype',eq('actor')).outV().has('personid',1511).select('movies')";
        String query4 = "g.V().has('personid',4445).out('role').in('role').until(has('personid',5363)).repeat(out('role').in('role')).limit(2).path().by(valueMap())";
        if (args.length == 0) {
            args = new String[] { query4, "" };
        }

        Main m = new Main();
        m.remoteSubmit(args[0], args[1]);
        System.out.println("finish");
    }

    private static void regexTest() {
        String s = "aaa%1$s,%1$s,%2$s";
        String rex = "%(\\d+\\$)?([-#+ 0,(\\<]*)?(\\d+)?(\\.\\d+)?([tT])?([a-zA-Z%])";
        Pattern p = Pattern.compile(rex);
        Matcher matcher = p.matcher(s);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        System.out.println(count);

        s = String.format(s, new Object[] { "1", "2" });

        System.out.println(s);
    }

    private void remoteSubmit(String query, String filePath) throws FileNotFoundException {

        ClassLoader classLoader = Main.class.getClassLoader();
        URL resource = classLoader.getResource("remote-objects.yaml");
        String path = resource.getPath();
        System.out.println(path);
        File f = new File(path);
        if (!f.exists()) {
            f = new File(filePath);
            if (!f.exists()) {
                System.err.println("file not exist...current path is " + f.getAbsolutePath());
                return;
            }
        }
        GryoMapper mapper = GryoMapper.build().addRegistry(JanusGraphIoRegistry.getInstance()).create();

        Cluster cluster = Cluster.build(f).serializer(new GryoMessageSerializerV1d0(mapper)).create();
        Client client = cluster.connect();
        try {
            System.out.println("submit query:");
            System.out.println(query);
            List<Result> results = client.submit(query).all().get();
            System.out.println("##########");
            System.out.println(results);
            System.out.println("##########");

            int pathNum = 1;
            for (Result result : results) {
                Object obj = result.getObject();
                if (obj instanceof AbstractMap.SimpleEntry) {
                    AbstractMap.SimpleEntry entry = (AbstractMap.SimpleEntry) obj;
                    // System.out.println(result + " : entry value type=" +
                    // entry.getValue().getClass().toString());
                    String key = entry.getKey().toString();
                    if (entry.getValue() instanceof List) {
                        List list = (List) entry.getValue();
                        for (Object object : list) {
                            System.out.println(key + "\t" + object);
                        }
                    }

                } else if (obj instanceof Map) {
                    Map<String, String> map = (Map) obj;
                    int colSize = map.entrySet().size();
                    int index = 0;
                    StringBuffer sb = new StringBuffer();
                    for (Entry entry : map.entrySet()) {
                        if (index < colSize) {
                            sb.append(entry.getValue());

                        }
                        if (index != colSize - 1) {
                            sb.append("\t");
                        }
                        index++;
                    }
                    System.out.println(sb);
                } else if (obj instanceof DetachedPath) {
                    DetachedPath dPath = (DetachedPath) obj;

                    if (dPath.objects().get(0) instanceof Map) {
                        int pathSeq = 1;
                        List<List<Object>> resultDatas = new ArrayList<List<Object>>();
                        for (int i = 0; i < dPath.size(); i++) {
                            Object pathObj = dPath.objects().get(i);

                            List<Object> resultList = new ArrayList<Object>();
                            Map mPathObj = (Map) pathObj;
                            for (Object object : mPathObj.entrySet()) {
                                resultList.add(pathNum);

                                Entry entry = (Entry) object;
                                String key = entry.getKey().toString();
                                if (entry.getValue() instanceof List) {
                                    List<Object> valList = (List<Object>) entry.getValue();
                                    List<Object> tmpList;
                                    for (Object valObj : valList) {
                                        tmpList = Main.deepCopy(resultList);
                                        tmpList.add(pathSeq++);

                                        tmpList.add(valObj.toString());
                                        tmpList.add(key);

                                        resultDatas.add(tmpList);
                                    }
                                } else {
                                    resultList.add(pathSeq++);
                                    resultList.add(key);
                                    resultList.add(entry.getValue());
                                    resultDatas.add(resultList);
                                }
                            }
                        }
                        for (List<Object> list : resultDatas) {
                            for (int i = 0; i < list.size(); i++) {
                                Object o = list.get(i);
                                System.out.print(o + "\t");
                            }
                            System.out.println();
                        }

                        // if (dPath.objects().get(0) instanceof Map) {
                        // int pathName = 1;
                        // // get all keys
                        // Set<String> keySet = new HashSet<String>();
                        // for (Object object : dPath.objects()) {
                        // Map<String, Object> mObj = (Map) object;
                        // for (String key : mObj.keySet()) {
                        // keySet.add(key);
                        // }
                        // }
                        // List<String> keyList = new ArrayList<String>(keySet);
                        // int keySize = keyList.size();
                        //
                        // List<List<Object>> resultDatas = new
                        // ArrayList<List<Object>>();
                        // for (int i = 0; i < dPath.size(); i++) {
                        // Object pathObj = dPath.objects().get(i);
                        //
                        // List<Object> resultList = new ArrayList<Object>();
                        // Map mPathObj = (Map) pathObj;
                        // for (Object object : mPathObj.entrySet()) {
                        // resultList.add(pathNum);
                        //
                        // Entry entry = (Entry) object;
                        // if (entry.getValue() instanceof List) {
                        // List<Object> valList = (List<Object>)
                        // entry.getValue();
                        // List<Object> tmpList;
                        // for (Object valObj : valList) {
                        // tmpList = Main.deepCopy(resultList);
                        // tmpList.add(pathName++);
                        // int keyIndex = keyList.indexOf(entry.getKey()) + 2;
                        // while (keyIndex > tmpList.size()) {
                        // tmpList.add(tmpList.size(), null);
                        // }
                        // tmpList.add(keyIndex, valObj);
                        // while (tmpList.size() < keyList.size() + 2) {
                        // tmpList.add(tmpList.size(), null);
                        // }
                        // resultDatas.add(tmpList);
                        // }
                        // } else {
                        // resultList.add(pathName++);
                        // resultList.add(keyList.indexOf(entry.getKey()) + 2,
                        // entry.getValue());
                        // }
                        // }
                        // }
                        // for (List<Object> list : resultDatas) {
                        // System.out.println(list.toString());
                        // }
                    }
                    pathNum++;
                } else {
                    System.out.println(result + " : " + result.getObject().getClass().toString());

                }

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ResponseException) {
                // rollback
            }
            e.printStackTrace();
        }
        cluster.close();
    }

    public static <T> List<T> deepCopy(List<T> src) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream out;
        try {
            out = new ObjectOutputStream(byteOut);
            out.writeObject(src);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
        ObjectInputStream in = null;
        try {
            in = new ObjectInputStream(byteIn);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<T> dest = null;
        try {
            dest = (List<T>) in.readObject();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        return dest;
    }

}
