# Ashtarte

Welcome to Ashtarte !

弹性分布式并行计算系统研究和探索

### Example
* WorldCount:
```
    BatchContext mppContext = BatchContext.builder()
        .setParallelism(2)
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
                .setParallelism(2)
                .getOrCreate();
        int iters = 4;  //迭代次数
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

### 内容:
包括和不限于以下内容概念:

operator(算子):
* map,flatMap,filter 等transform算子
* foreach,count,collect 等action算

常见概念:
* 有向无环图(DAG)
* job->stage->task
* 并行 pipeLine
* partition(split)
* Connector

* 窄依赖
* 宽依赖
* shuffle(HashShuffle)

网络层:
* netty
* rpc
* 缓冲区
* callback
* Nio

运行时
* Local
* LocalPCForkJVM
* YARN Cluster

高级
* collect(number)
* collect_Stream()
* job 提前结束
* Join
* Combiner
* 本地化
* 调度器(Job Scheduler)
* 内存布局(和使用情况)
* cache
* 溢写(disk<=容忍性<=memory)
* 资源管理(即系查询方向核心:支持并行任务调度的高优化资源管理,更加强大的内存管理、cache管理、元数据、优化器)
* Predicate PushDown
* Aggregate PushDown

技巧
* 数据倾斜的来龙去脉
* partition(split)优化
* Connector优化
* 函数式和闭包
* 流水线设计(pipeline,或管道化)
