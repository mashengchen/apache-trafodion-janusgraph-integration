Here are TMUDFs using JanusGraph build-in to provide graph capabilities.<br/>

JanusGraph used is v0.2.0(newest currently). the version-compat can be seen at http://docs.janusgraph.org/latest/version-compat.html.
There needs add janusgraph-core related jars and gremlin-driver related jars. following is the maven dependency.<br/>
```xml
<dependencies>
  <dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-core</artifactId>
    <version>0.2.0</version>
    <scope>provided</scope>
  </dependency>
  <dependency>
    <groupId>org.apache.tinkerpop</groupId>
    <artifactId>gremlin-driver</artifactId>
    <version>3.2.3</version>
    <scope>provided</scope>
  </dependency>
</dependencies>
```

In concept, JanusGraph support run gremlin through it's shell. Likely those TMUDFs also can run gremlin by give gremlin query as a parameter.<br/>

Also there need provide a yaml formatted file to give host and port, this file should in ${TRAF_HOME}/udr/public/conf. An example of yaml content named conf.yaml as follow:<br/>
```
hosts: [192.168.0.11]
port: 8182
```
<br/>
The TMUDF "graph_query" is for query, while "graph_update" is for update (include add & delete).<br/>

For the "graph_query", following are function ddl and examples:
* function ddl
```sql
create table_mapping function graph_query(query varchar(1000))
   external name 'org.trafodion.udf.janusGraph.graph_query' -- name of your class
   language java
   library JanusGraphlib;
```

* no table-mapping udf
```sql
select * from 
UDF(graph_query('g.V().match(__.as("directors").hasLabel("person"),__.as("directors").outE("role").has("roletype",eq("director")).inV().as("directed_movies"),__.as("directed_movies").inE("role").has("roletype",eq("actor")).outV().as("directors")).select("directors").dedup().order().by("personid").values("personid")'));
```
```sql
select * from 
UDF(graph_query('g.V().has("personid",4445).out("role").in("role").until(has("personid",5363)).repeat(out("role").in("role")).limit(1).path().by(valueMap())'));
```

* table-mapping udf
```sql
select m.movie_id, movie_title from movie m,
(select a as movie_id 
from udf(graph_query(table(select person_id from person where name = 'Clint Eastwood'),
'g.V().has("personid",%1$s).outE("role").has("roletype",eq("director")).inV().as("source").values("movieid").as("movies").select("source").inE().has("roletype",eq("actor")).outV().has("personid",%1$s).select("movies")'))) j
where j.movie_id = m.movie_id
order by 2;
```

For the "graph_update", following are function ddl and examples:
* function ddl
```sql
create table_mapping function graph_update(query varchar(1000))
   external name 'org.trafodion.udf.janusGraph.graph_update' -- name of your class
   language java
   library JanusGraphlib;
```

* update
```sql
select * from
udf(graph_update(
table(select person_id from (update person set facebook_likes = 16002 where name = 'Clint Eastwood') p),
'g.V().has("person", "personid", %1$s).property("personid", %1$s).iterate()'));
```
* insert
```sql
select * from
udf(graph_update(
table(select person_id from (insert into person (person_id, name, facebook_likes) values (8482, 'test person', 1000)) p),
'graph.addVertex(label, "person", "personid", %1$s)'))
```
* delete
```sql
select * from
udf(graph_update (
table(select person_id from (delete from person where name = 'Clint Eastwood') p),
'g.V().has("person", "personid", %1$s ).drop()'));
```
* update through multi-table as a table-mapping udf, and commit query contained gremlin query
```sql
select * from
udf(graph_update(
table(select person_id,m.movie_id from person, (select movie_id from movie where movie_title LIKE 'Unforgiven%') as m where name = 'Clint Eastwood'),
'g.V().has("person", "personid", %1$s).bothE().has("roletype", "actor").where(otherV().has("movie", "movieid", %2$s)).property("roletype","director"); graph.tx().commit()'));
```

Only one parameter is expected by the TMUDFs: the gremlin query.<br/>
<br/>
As of Traf 2.3, the dependency jars can put under $TRAF_HOME/udr/public/external_libs, and there will load the jars under the directory first.<br/>
