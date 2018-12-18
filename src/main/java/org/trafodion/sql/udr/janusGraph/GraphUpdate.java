package org.trafodion.sql.udr.janusGraph;

import java.util.ArrayList;
import java.util.IllegalFormatConversionException;
import java.util.List;
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
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistryV1d0;
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
 *         insert
 * 
 *         select * from UDF(graph_update( table(select person_id from(insert
 *         into person (name, facebook_likes) values ('Clint Eastwood', 1000))
 *         p), 'graph.addVertex(label, "person", "personid", %1$s)' ));
 * 
 *         delete
 * 
 *         select * from udf(graph_update( table(select person_id from (delete
 *         from person where name = 'Clint Eastwood') p), 'g.V().has("movie",
 *         "movieid", %1$s).drop()' ));
 * 
 *         update
 * 
 *         select * from udf(graph_update( table(select person_id from (update
 *         person set facebook_likes = 16001 where name = 'Clint Eastwood') p),
 *         'g.V().has("person", "personid", %1$s).property("personid",
 *         %1$s).iterate()’ ));
 *
 */
public class GraphUpdate extends UDR {
    private static final Logger LOG = LoggerFactory.getLogger(GraphUpdate.class);

    @Override
    public void describeParamsAndColumns(UDRInvocationInfo info) throws UDRException {
        Utils.getHost();// this step let Utils to do static{} code
        Pattern p = Pattern.compile(Utils.rex);
        String gremlinQuery = info.par().getString(0);// gremlin query
        LOG.info("gremlinQuery : [" + gremlinQuery + "]");

        // query has commit
        if (gremlinQuery.contains("commit()")) {
            int len = gremlinQuery.split(";").length;
            if (len < 2) {
                throw new UDRException(38000, "error : more than one gremlin should be separated by semicolon [;]");
            }
        }

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
                throw new UDRException(38000, "error : input gremlin has illegal parameter...[%s]", e.getMessage());
            }
        } else {
            // query with no input
        }
        info.out().addLongColumn("ROWS_INSERTED", false);
        // info.addPassThruColumns();
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

        int numRowsInserted = 0;
        String retVal = null;
        if (info.getNumTableInputs() == 0) {
            retVal = submit(client, gremlinQuery.trim());
            if (retVal == null) {
                numRowsInserted++;
            }else {
                throw new UDRException(38000,
                        "Error : " + retVal + ". gremlinQuery : " + gremlinQuery);
            }
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
                retVal = submit(client, formattedQuery.trim());
                if (retVal == null) {
                    numRowsInserted++;
                }else {
                    throw new UDRException(38000,
                            "Error : " + retVal + ". gremlinQuery : " + formattedQuery);
                }
            }
        }
        info.out().setLong(0, numRowsInserted);
        emitRow(info);

        if (cluster != null) {
            cluster.close();
        }
    }

    private String submit(Client client, String formattedQuery) {
        try {
            List<Result> results = client.submit(formattedQuery).all().get();
            LOG.debug(results.toString());
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
            return e.getMessage();
        } catch (ExecutionException e) {
            LOG.error(e.getMessage(), e);
            if (e.getCause() instanceof ResponseException) {
                ResponseException re = (ResponseException) e.getCause();
                submit(client, "graph.tx().rollback()");
                return String.format("error : [%s]", re.getMessage());
            }
        }
        return null;
    }

}
