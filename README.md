# Astarte 
[![Build Status](https://api.travis-ci.com/harbby/astarte.svg?branch=master)](https://travis-ci.com/harbby/astarte)
[![codecov](https://codecov.io/gh/harbby/astarte/branch/master/graph/badge.svg?)](https://codecov.io/gh/harbby/astarte)
[![license](https://img.shields.io/badge/license-apache_v2-groon.svg)]()
[![language](https://img.shields.io/badge/language-java_8_11_17-green.svg)]()


Welcome to Astarte !


### Example
* WorldCount:
```
BatchContext mppContext = BatchContext.builder()
    .local(2)
    .getOrCreate();

DataSet<String> ds = mppContext.textFile("/tmp/.../README.md");
DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
    .filter(x -> !"".equals(x.trim()));

KvDataSet<String, Long> worldCounts = worlds.kvDataSet(x -> Tuple2.of(x, 1L))
    .reduceByKey((x, y) -> x + y);

worldCounts.collect()
    .forEach(x -> System.out.println(x.f1() + "," + x.f2()));
```
* PageRank
```
BatchContext mppContext = BatchContext.builder()
        .local(2)
        .getOrCreate();
int iters = 4;
String sparkHome = System.getenv("SPARK_HOME");

DataSet<String> lines = mppContext.textFile(sparkHome + "/data/mllib/pagerank_data.txt");

KvDataSet<String, Iterable<String>> links = lines.kvDataSet(s -> {
    String[] parts = s.split("\\s+");
    return new Tuple2<>(parts[0], parts[1]);
}).distinct().groupByKey().cache();

KvDataSet<String, Double> ranks = links.mapValues(v -> 1.0);
for (int i = 1; i <= iters; i++) {
    DataSet<Tuple2<String, Double>> contribs = links.join(ranks).values().flatMapIterator(it -> {
        Collection<String> urls = (Collection<String>) it.f1();
        Double rank = it.f2();

        long size = urls.size();
        return urls.stream().map(url -> new Tuple2<>(url, rank / size)).iterator();
    });

    ranks = KvDataSet.toKvDataSet(contribs).reduceByKey((x, y) -> x + y).mapValues(x -> 0.15 + 0.85 * x);
}

List<Tuple2<String, Double>> output = ranks.collect();
output.forEach(tup -> System.out.println(String.format("%s has rank:  %s .", tup.f1(), tup.f2())));
```

