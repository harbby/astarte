# mppwhater
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
        .reduce((x, y) ->
            Tuple2.of(x.f1(), x.f2() + y.f2())
        );

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

思考问题:
1. 处理1000G的数据需要1000G内存吗?(答: 不一定,某些可能只需32M内存)
2. 数据量增长和所需内存资源成线性吗?(答: 不一定是线性)
3. 那么Apache Spark等分布式内存MPP计算到底解决什么问题?
(通过可水平扩展: 充分并很好的利用了集群其他机器的cpu资源,内存等资源，通常会大幅降低大规模数据量的处理时间)
4. 为什么数据倾斜非常可怕?

答: 刚才讲了MPP架构最大的优点是可以调动集群的其他节点一起协作工作。如果你数据倾斜的非常夸张，最坏情况
可能所有的数据和任务都在一个节点上计算.那么虽然你申请了很多资源，但其实他们都处于闲置状态(1核有难8核围观)
实际资源利用率非常低。
5. spark等分布式计算框架调优通常需要关注什么？

理论上:以申请的集群资源利用率为核心目标，按需平衡好cpu,内存,网络io等三种资源，并优化代码降低不必要的资源浪费。

6. 大数据类系统需要注意什么?
把系统吞吐量作为重要指标。任何大数据系统都应该考虑吞吐量规格。水平扩容是否对吞吐量有帮助等。
通常资源不变的情况下提升资源利用率可以提高吞吐能力。

6. 采用MPP进行大规模数据量计算时通常容易遇到什么瓶颈?
(答: 网络>内存>cpu,
某些情况下数据源磁盘io可能也会瓶颈，io瓶颈通常和存储模型和Source Connector质量有关.
)

7. 不同算子对内存有什么样的影响?(请思考: map算子,cache算子,shuffle算子)
8. 不同算子对网络有什么样的影响?(请思考: map算子,cache算子,shuffle算子)
9. 什么操作最复杂?
答: shuffle。shuffle操作是网络io复杂度主要贡献者，也是内存复杂度主要贡献者者。
shuffle操作就像魔法，神奇又麻烦
传统的瓶颈调优通常降低时间复杂度(cpu)或空间复杂度(内存)，而分布式系统通常更优先考虑io复杂度
举个例子: spark中cache算子通常来说是为了大幅降低io复杂度(cpu也会降),但也牺牲了空间复杂度
10. 如何调优shuffle
答: 通过Combiner降低io量, 合理增加并行度提高速度, 选择合适的策略平衡数据均匀性避免数据倾斜,
避免无意义的shuffle减少shuffle操作
11. 什么是流水线设计

流水线设计是硬件设计中非常成熟理论和技术。通常会极大的提高系统吞吐量。请查阅