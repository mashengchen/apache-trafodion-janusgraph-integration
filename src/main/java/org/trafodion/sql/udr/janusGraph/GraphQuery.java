package org.trafodion.sql.udr.janusGraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.IllegalFormatConversionException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.sql.udr.TypeInfo.SQLTypeClassCode;
import org.trafodion.sql.udr.UDR;
import org.trafodion.sql.udr.UDRException;
import org.trafodion.sql.udr.UDRInvocationInfo;
import org.trafodion.sql.udr.UDRPlanInfo;

/**
 * 
 * @author shengchen.ma
 * 
 *         query with no input
 * 
 *         select person_id from UDF(graph_query(Gremlin query))
 * 
 *         query with input from EsgynDB table
 * 
 *         select movie_id from UDF(graph_query( table(select person_id from
 *         person where name = 'Clint Eastwood'), 'gremlin query with parameter
 *         %1$s'));
 * 
 */
public class GraphQuery extends UDR {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQuery.class);


    @Override
    public void describeParamsAndColumns(UDRInvocationInfo info) throws UDRException {
        // info.print();
        info.out().addVarCharColumn("A", 50, false); // column number 0
        info.out().addVarCharColumn("B", 50, false); // column number 1
        info.out().addVarCharColumn("C", 50, false); // column number 2
        info.out().addVarCharColumn("D", 50, false); // column number 3

        Utils.getHost();// this step let Utils to do static{} code

        Pattern p = Pattern.compile(Utils.rex);
        String gremlinQuery = info.par().getString(0);// gremlin query
        LOG.info("gremlinQuery : [" + gremlinQuery + "]");

        Matcher m = p.matcher(gremlinQuery);
        if (m.find()) {
            // query with input from EsgynDB table
            List<Object> params = new ArrayList<Object>();
            int numColumns = info.in().getNumColumns();
            for (int i = 0; i < numColumns; i++) {
                SQLTypeClassCode type = info.in().getSQLTypeClass(i);
                switch (type) {
                case CHARACTER_TYPE:
                    params.add("a");
                    break;
                case NUMERIC_TYPE:
                    params.add(1);
                    break;
                case BOOLEAN_TYPE:
                    params.add(1);
                    break;
                default:
                    params.add("a");
                    break;
                }
            }
            // check whether there will throw
            // java.util.IllegalFormatConversionException
            try {
                String.format(gremlinQuery, params.toArray());
            } catch (IllegalFormatConversionException e) {
                LOG.error(e.getMessage(), e);
                throw new UDRException(38000, "error : input gremlin has illegal parameter...[%s]", e.getMessage());
            }
        } else {
            // query with no input
        }

    }

    @Override
    public void processData(UDRInvocationInfo info, UDRPlanInfo plan) throws UDRException {
        LOG.info("entry.");

        String gremlinQuery = info.par().getString(0);// gremlin query

        GryoMapper.Builder kryo =
                GryoMapper.build().addRegistry(JanusGraphIoRegistry.getInstance());
        MessageSerializer serializer = new GryoMessageSerializerV1d0(kryo);

        Cluster cluster = Cluster.build(Utils.getHost()).port(Utils.getPort())
                .serializer(serializer).create();
        Client client = cluster.connect().init();
        String retVal = null;

        if (info.getNumTableInputs() == 0) {
            retVal = submit(info, client, gremlinQuery);
        } else {
            while (getNextRow(info)) {
                List<Object> params = new ArrayList<Object>();
                int numTables = info.getNumTableInputs();
                for (int i = 0; i < numTables; i++) {
                    int numColumns = info.in(i).getNumColumns();

                    for (int j = 0; j < numColumns; j++) {
                        SQLTypeClassCode type = info.in().getSQLTypeClass(j);
                        switch (type) {
                        case CHARACTER_TYPE:
                            params.add(info.in().getString(j));
                            break;
                        case NUMERIC_TYPE:
                            params.add(info.in().getLong(j));
                            break;
                        case BOOLEAN_TYPE:
                            params.add(info.in().getBoolean(j));
                            break;
                        default:
                            params.add(info.in().getString(j));
                            break;
                        }

                    }
                }

                String formattedQuery = String.format(gremlinQuery, params.toArray());
                retVal = submit(info, client, formattedQuery);
            }
        }

        if (cluster != null) {
            cluster.close();
        }

        if (retVal != null) {
            throw new UDRException(38000, retVal);
        }
    }

    private String submit(UDRInvocationInfo info, Client client, String query) throws UDRException {
        try {
            List<Result> results = client.submit(query).all().get();
            int pathNum = 1;
            for (Result result : results) {
                Object obj = result.getObject();
                LOG.debug("Result is : " + result + ". Result object type : " + obj.getClass());
                if (obj instanceof AbstractMap.SimpleEntry) {
                    AbstractMap.SimpleEntry entry = (AbstractMap.SimpleEntry) obj;
                    // System.out.println(result + " : entry value type=" +
                    // entry.getValue().getClass().toString());
                    String key = entry.getKey().toString();
                    if (entry.getValue() instanceof List) {
                        List list = (List) entry.getValue();
                        for (Object object : list) {
                            info.out().setString(0, key);
                            info.out().setString(1, object.toString());
                        }
                    }
                    emitRow(info);
                } else if (obj instanceof Map) {
                    Map<String, Object> map = (Map) obj;
                    int colSize = map.entrySet().size();
                    int index = 0;
                    for (Entry<String, Object> entry : map.entrySet()) {
                        if (index < colSize) {
                            info.out().setString(index, entry.getValue().toString());
                        }
                        index++;
                    }
                    emitRow(info);
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
                                        tmpList = GraphQuery.deepCopy(resultList);
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
                                info.out().setString(i, o.toString());
                            }
                            emitRow(info);
                        }
                    }
                    pathNum++;
                } else {
                    info.out().setString(0, obj.toString());
                    emitRow(info);
                }
            }
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
            return e.getMessage();
        } catch (ExecutionException e) {
            LOG.error(e.getMessage(), e);
            if (e.getCause() instanceof ResponseException) {
                // rollback
                return String.format("error : [%s]", e.getMessage());
            }
        }
        return null;
    }

    private static <T> List<T> deepCopy(List<T> src) {
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
