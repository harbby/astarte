# Ashtarte

Welcome to Ashtarte !

弹性分布式并行计算系统研究和探索

### Example
* WorldCount:
```
    MppContext mppContext = MppContext.getOrCreate();

    DataSet<String> ds = mppContext.textFile("/tmp/.../README.md");
    DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
        .filter(x -> !"".equals(x.trim()));

    DataSet<Tuple2<String, Integer>> worldCounts = worlds.map(x -> Tuple2.of(x, 1))
        .groupBy(x -> x.f1())
        .agg(x -> x.f2(),(x, y) -> x + y);

    worldCounts.collect()
        .forEach(x -> System.out.println(x.f1() + "," + x.f2()));
```
* fromCollection
```
    MppContext mppContext = MppContext.getOrCreate();
    //set partition = 4
    DataSet<String> dataSet = mppContext.fromCollection(Arrays.asList("1", "2", "3") ,4);

    dataSet.map(k -> Integer.parseInt(k))
    .map(k -> k + 1);
    .foreach(x -> {
        System.out.println(x);
    });
```

### 内容:
包括和不限于以下内容概念:

operator(算子):
* map,flatMap,filter 等transform算子
* foreach,count,collect 等action算子(触发器)

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
* Cluster

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
