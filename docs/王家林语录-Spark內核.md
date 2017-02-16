#### 王家林每日 Spark 语录 0001
* 腾讯的 Spark 集群已经达到 8000 台的规模,是目前已知最大 的 Spark 集群,每天运行超过 1 万各种作业。单个 Spark Job 最大分别是阿里巴巴和 Databricks——1PB。

#### 王家林每日 Spark 语录 0002
* Spark 基于 RDD 近乎完美的实现了分布式内存的抽象,且能够基于位置感知性调度、自动容错、负载均衡和高度的可扩展性,Spark 中允许用户在执行多 个查询时显式的将工作集缓存起来(Persists/ Cache) 以供后续查询重用,这极大的提高了查询的速度。

#### 王家林每日 Spark 语录 0003
* Spark 一体化多元化的解决方案极大的减少了开发和维护的人力成本和部署平台的物力成本,并在性能方面有极大的优势,特别适合于迭代计算 (Iterator),例如机器学习和和图计算;同时 Spark 对 Scala 和 Python 交互式 shell 的支持也极大的方便了通过shell直接来使用Spark集群来验证解决问题的方法,这对于原型开发至关重要,对数据分析人员有着无法拒绝的吸引力!

#### 王家林每日 Spark 语录 0004
* Spark 中 RDD 采用高度受限的分布式共享内存,且新的 RDD 的产生只能够通过其它 RDD 上的批量操作来创建,依赖于以 RDD 的 Lineage 为核心的容错处理, 在迭代计算方面比 Hadoop 快 20 多倍,同时还可以在 5~7 秒内交互式的查询 TB 级别的数据集。

#### 王家林每日Spark语录0005(2015.10.28于深圳)
* SparkRDD是被分区的,对于RDD来说,每个分区都会被一个计算任务处理,并决定并行计算的粒度;RDD 的每次转换操作都会生成新的 RDD,在生成 RDD 时候,一般可以指定分区的数量 (Level of parallelism),如果不指定分区数量,当 RDD 从集合创建时候,则默认为该程序所分配到的资源的 CPU 核数,如果是从 HDFS 文件创建, 默认为文件的 Block 数。

#### 王家林每日 Spark 语录 0006(2015.10.29 于深圳)
* 基于 RDD 的整个计算过程都是发生在 Worker 中的 Executor 中的。RDD 支持三种类型的操作:Transformation、Action 以及 Persist 和 CheckPoint 为代表的控制类型的操作,RDD 一般会从外部数据源读取数据,经过多次 RDD 的 Transformation(中间为了容错和提高效率,有可能使用 Persist 和 CheckPoint),最终通过 Action 类型的操作一般会把结果写回外部存储系统。

#### 王家林每日 Spark 语录 0007(2015.10.30 于北京)
* RDD 的所有 Transformation 操作都是 Lazy 级别的,实际上这些 Transformation 级别操作的 RDD 在发生 Action 操作之前只是仅仅被记录会作用在基础数据集上而已,只有当 Driver 需要返回结果的时候,这些 Transformation 类型的 RDD 才会真正作用数据集,基于这样设计的调度模式和运行模式让 Spark 更加有效率的运行。

#### 王家林每日大数据语录 Spark 篇 0008(2015.10.31 于北京)
* 持久化(包含 Memory、Disk、 Tachyon 等类型)是 Spark 构建迭代算法和快速交互式查询的关键,当通过 persist 对一个 RDD 持久化后,每一个节点都将把计算的分片结果保存在内存或者磁盘或者 Tachyon 上,并且对此数据集或者衍生出来的数据集进行的其它 Action 级别的炒作都可以重用当前 RDD 的计算结果,这時后续的操作通常会快 10 到 100 倍。

#### 王家林每日大数据语录 Spark 篇 0009(2015.11.1 于北京)
* Spark 的 CheckPoint 是在计算完成之后重新建立一个 Job 来进行计算的,用户可以通过调用 RDD.checkpoint()来指定 RDD 需要 checkpoint 的机制;为了避免重复计算,建议先对 RDD 进行 persist 操作,这样可以 保证 checkpoint 更加快速的完成。

#### 王家林每日大数据语录 Spark 篇 0010(2015.11.2 于深圳)
* SparkContext 是用户程序和 Spark 交互的接口,它会负责连接到 Spark 集群,并且根据系统默认配置和用户设置来申请计算资源,完成 RDD 的创建等工作。

#### 王家林每日大数据语录 Spark 篇 0011(2015.11.2 于深圳)
* RDD 的 saveAsTextFile 方法会首先生成一个 MapPartitionsRDD,该 RDD 通过雕工 PairRDDFunctions 的 saveAsHadoopDataset 方法向 HDFS 等输出 RDD 数据的内容,并在在最后调用 SparkContext 的 runJob 来真正的向 Spark 集群提交计算任务。

#### 王家林每日大数据语录 Spark 篇 0012(2015.11.2 于深圳)
* 可以从两个方面来理解 RDD 之间的依赖关系,一方面是 RDD 的 parent RDD(s)是什么,另一方面是依赖于 parent RDD(s) 哪些 Partions(s); 根据依赖于 parent RDD(s)哪些 Partions(s)的不同情况,Spark 讲 Dependency 分为宽依赖和窄依赖两种。

#### 王家林每日大数据语录 Spark 篇 0013(2015.11.3 于广州)
* RDD 有 narrow dependency 和 widen dependency 两种不同的类型的依赖,其中的 narrow dependency 指的是每一个 parent RDD 的 Partition 最多被 child RDD 的一个 Partition 所使用 (1對1的關係) ,而 widen dependency 指的是多个 child RDDs 的 Partition 会依赖于同一个 parent RDD 的 Partition (1對多的關係)。

#### 王家林每日大数据语录 Spark 篇 0014(2015.11.4 于南宁)
* 对于 Spark 中的 join 操作,如 果每个 partition 仅仅和特定的 partition 进行 join 那么就是窄依赖;对于需要 parent RDD 所有 partition 进行 join 的操作,即需要 shuffle,此时就是宽依赖。

#### 王家林每日大数据语录 Spark 篇 0015(2015.11.5 于南宁)
* Spark 中宽依赖指的是生成的 RDD 的每一个 partition 都依赖于父 RDD(s) 所有 partition,宽依赖典型的操作有 groupByKey, sortByKey 等,宽依赖意味着 shuffle 操作,这是 Spark 划分 stage 的边界的依据,Spark 中宽依赖支持两种 Shuffle Manager,即 HashShuffleManager 和 SortShuffleManager,前者是基于Hash的Shuffle机制,后者是基于排序的Shuffle机制。

#### 王家林每日大数据语录 Spark 篇 0016(2015.11.6 于南宁)
* RDD 在创建子 RDD 的时候,会 通过 Dependency 来定义他们之间的关系,通过 Dependency,子 RDD 可以获得 parent RDD(s) 和 parent RDD(s)的 Partition(s).

#### 王家林每日大数据语录 Spark 篇 0017(2015.11.6 于南宁)
* 在 Spark 的 Stage 内部的每个 Partition 都会被分配一个计算任务 Task,这些 Task 是并行执行的; Stage 之间的依赖关系变成了一个大粒度的 DAG,Stage 只有在它没有 parent Stage 或者 parent Stage 都已经执行完成后才可以执行,也就是说 DAG 中的 Stage 是从前往后顺序执行的。

#### 王家林每日大数据语录 Spark 篇 0018(2015.11.7 于南宁)
* 在 Spark 的 reduceByKey 操作时会触发 Shuffle 的过程,在 Shuffle 之前,会有本地的聚合过程产生 MapPartitionsRDD, 接着具体 Shuffle 会产生 ShuffledRDD,之后做全局的聚合生成结果 MapPartitionsRDD. 

* #### 王家林每日大数据语录 Spark 篇 0019(2015.11.10 于重庆)
Spark 中的 Task 分为 ShuffleMapTask 和 ResultTask 两种类型,在 Spark 中 DAG 的最后一个 Stage 内部的任务都是 ResultTask,其余所有的 Stage(s)的内部都是 ShuffleMapTask,生成的 Task 会被 Driver 发送到已经启动的 Executor 中执行具体的计算任务,执行的实现是在 TaskRunner.run 方法 中完成的。

#### 王家林每日大数据语录 Spark 篇 0020(2015.11.11 于重庆)
* Spark 中生成的不同的 RDD 中有的和用户逻辑显示的对应,例如 map 操作会生成 MapPartitionsRDD,而又的 RDD 则是 Spark 框架帮助我们隐式生成的,例如 reduceByKey 操作时候的 ShuffledRDD.

#### 王家林每日大数据语录 Spark 篇 0021(2015.11.18 于珠海)
* Spark RDD 实现基于 Lineage 的容错机制,基于 RDD 的各项 transformation 构成了 compute chain,在部分计算结果丢失的时候可以根据 Lineage 重新计算恢复。在窄依赖中,在子 RDD 的分区丢失要重算父 RDD 分区时,父 RDD 相应分区的所有数据都是子 RDD 分区的数据,并不存在冗余计算;在宽依赖情况下,丢失一个子 RDD 分区重算的每个父 RDD 的每个分区的所有数据并不是都给丢失的子 RDD 分区用的,会有一部分数据相当于对应的是未丢失的子 RDD 分区中需要的数据,这样就会产生冗余计算开销和巨大的性能浪费。

#### 王家林每日大数据语录 Spark 篇 0022(2015.11.18 于珠海)
* Spark Checkpoint 通过将 RDD 写入 Disk 做检查点,是 Spark lineage 容错的辅助,lineage 过长会造成容错成本过高, 这时候在中间阶段做检查点容错,如果之后有节点出现问题而丢失分区,从做检查点的 RDD 开始重做Lineage,就会减少开销。Checkpoint主要适用于以下两种情况:
1. DAG 中的 Lineage 过长,如果重算时会开销太大,例如在 PageRank、ALS 等;
2. 尤其适合于在宽依赖上做 Checkpoint,这个时候就可以避免应为 Lineage 重新计算而带来的冗余计算。

#### 王家林每日大数据语录 Spark 篇 0023(2015.11.18 于珠海)
* Spark 的调度器分为高层调度器 DAGScheduler 和底层调度器 TaskScheduler,其中 TaskScheduler 是一个抽象类,其最为重要的实现是 TaskSchedulerImpl,其中 Local、Standalone、Mesos 底层调度器的实现都是 TaskSchedulerImpl, 而 Yarn Cluster 和 Yarn Client 的 TaskScheduler 的实现分别是 YarnClusterScheduler 和 YarnClientClusterScheduler 都是继承自 TaskSchedulerImpl。

#### 王家林每日大数据语录 Spark 篇 0024(2015.11.24 于上海)
* Spark 的任务调度模块的三个核心累分别是 DAGScheduler、TaskScheduler 和 SchedulerBackend,其中 SchedulerBackend 的作用是向当前等待分配的计算资源 Task 分配计算资源 Executor,并且在分配的 Executor 上启动该 Task,最终完成计算的调度过程。

#### 王家林每日大数据语录 Spark 篇 0025(2015.11.24 于上海)
* Spark 的 MapOutputTrackerMaster 是运行在 Driver 端管理 Job 中 ShuffleMapTask 输出的,这样整个 Job 中的下一个 Stage 就可以通过 MapOutputTrackerMaster 来获取上一个依赖的 Stage 内部 Task 处理完数据后所保存的数据的位置信息,进而获取所依赖 Stage 的数据。

#### 王家林每日大数据语录 Spark 篇 0026(2015.11.24 于上海)
* Spark 的任务调度具体实现有 FIFO 和 FAIR 两种实现,FIFO 方式的 Pool 中是 TaskSetManager,而 FAIR 的 Pool 里面包含 了一组由 Pool 构建的调度树,这棵树的叶子节点是 TaskSetManager.

#### 王家林每日大数据语录 Spark 篇 0027(2015.11.24 于上海)
* Task 在 Executor 执行完成后 会想 Driver 发送 StatusUpdate 消息来通知 Driver 任务的状态,此时 Driver 端会首先调用 TaskScheduler 的具体是闲着 statusUpdate 来处理结果信息。

#### 王家林每日大数据语录 Spark 篇 0028(2015.11.25 于上海)
* TaskSetManager 在 Task 执行 失败时根据不同的失败原因采取不同的处理方式,如果是 Task 结果序列化失败,就会说明 Task 的执行有问题,会直接导致 TaskSetManager 的失败,如果是其它的情况, TaskSetManager 会重试任务的执行,默认重试的最大次数是 4 次,当然,用户也可以通过修改 spark.task.maxFailures 来设置这个最大重试次数。

#### 王家林每日大数据语录 Spark 篇 0029(2015.11.25 于上海)
* 对于 Spark 的 Standalone 模 式而言,在创建 SparkContext 的时候会通过 Master 为用户提交的计算分配计算资源 Executors,在 DAGScheduler 将计算任务通过 TaskSet 的方式提交给 TaskScheduler 的时候 就会在已经分配好的计算资源启动计算的 Tasks。

#### 王家林每日大数据语录 Spark 篇 0030(2015.11.25 于上海)
* Task 是 Spark 集群运行的基 本单位,一个 Task 负责处理 RDD 的一个 Partition,Spark 中的 ShuffleMapTask 会根据 Task 的 partitioner 将计算结果放到不同的 bucket 中,而 ResultTask 会将计算结果发送回 Driver。

#### 王家林每日大数据语录 Spark 篇 0031(2015.11.25 于上海)
* 一个 Job 包含多个 Stage(至 少一个 Stage),Stage 内部是一组完全相同的 Task 构成,这些 Task 只是处理的数据不同; 一个 Stage 的开始就是从外部存储或者 Shuffle 结果中读取数据,一个 Stage 的结束是由于 要发送 Shuffle 或者生成最终的计算结果。

#### 王家林每日大数据语录 Spark 篇 0032(2015.12.1 于北京)
* Spark 中的 Task 会被发送到为当前程序启动的Executors中,由Executor在线程池中使用线程完成Task的计算,执行的 过程调用 TaskRunner 的 run 方法。

#### 王家林每日大数据语录Spark篇0033(2015.12.2于北京)
* Spark的Cluster Manager支持 Standalone、YARN、Mesos、EC2、Local 等五种模式,Standalone 模式极大的丰富和扩展了 Spark 的应用场景、同时也极大的降低了部署和使用 Spark 的难度。

#### 王家林每日大数据语录 Spark 篇 0034(2015.12.11 于广州)
* Spark on Mesos 粗粒度的资 源调度模式是指每个 Executor 获得资源后就长期持有,直到应用程序退出才会释放资源。 这种方式的优点是减少了资源调度的时间开销,缺点是可能造成资源浪费,尤其在有些计算 出现长尾的情况下。

#### 王家林每日大数据语录 Spark 篇 0035(2015.12.11 于广州)
* Spark on Mesos 细粒度的资 源调度模式是指根据任务的实际需要动态申请资源,任务完成后就会将申请的资源归还给系 统,这种方式可以有效的避免资源的浪费,但是也带来的调度时间的开销,尤其是在任务运 行时间非常短但是计算任务数量有非常多的情况下对性能的影响会非常大。

#### 王家林每日大数据语录 Spark 篇 0036(2015.12.11 于广州)
* Spark 部署模式采用的是典型 的 Master-Slave 架构,Master 负责整个集群的资源调度和 Application 的管理,Worker 接受 Master 的资源分配命令来启动具体的 Executor,通过 Executor 中线程池来并行的完 成 Task 的计算。


#### 王家林每日大数据语录 Spark 篇 0037(2015.12.12 于广州)
* Spark Master 保存了整个集 群中所有 Worker、Application、Driver 的元数据信息,具体的元数据的持久化方式可能是 ZooKeeper、FileSystem 或者其它用户自定义的方式。默认情况下不保存元数据,Master 在启动之后会立即接管集群的管理工作。

#### 王家林每日大数据语录 Spark 篇 0038(2015.12.13 于上海)
* Spark Master 实现基于 ZooKeeper 的 HA 的时候,整个集群的元数据会持久化到 ZooKeeper 中,在 Master 故障后 ZooKeeper 会在 Standby 的 Master 中选举出新的 Master,新选举出来的 Master 在启动后会 从 ZooKeeper 中获取元数据信息并基于这些元数据信息恢复集群的状态,从而让新的 Master 提供资源管理和分配服务。

#### 王家林每日大数据语录 Spark 篇 0039(2015.12.14 于上海)
* Worker 会记录当前机器节点 使用的 CPU Cores、Memory 等信息,但在 Worker 给 Master 发送心跳的时并不会携带这些信 息,Worker 向 Master 发送心跳只是为了说明 Worker 还在活着,而 Master 在让 Worker 分 配资源的时候会记录资源 Worker 的资源的使用,并且在 Worker 的资源被回收的时候也会想 Master 发送消息。

#### 王家林每日大数据语录 Spark 篇 0040(2015.12.14 于上海)
* Worker 在向 Master 注册的时 候如果在指定的时间收不到 Master 的响应会重新发送注册信息进行重试,目前重试次数是 16 次,其中前 6 次的重试时间间隔是 5-15 秒,后面 10 次重试的时间间隔是 30-90 秒。

#### 王家林每日大数据语录 Spark 篇 0041(2015.12.14 于上海)
* Spark 对错误的处理提供了高 度的弹性,对于集群而言,机器故障、网络故障时常态,Spark 基于 RDD 的调度和容错系统 和 Hadoop 相比能够以更小的代价和更快的速度从错误中恢复从而持续提供数据处理服务。

#### 王家林每日大数据语录 Spark 篇 0042(2015.12.15 于上海)
* 生产环境下 Spark Master 一 般选择使用 ZooKeeper 做 HA,如果当前管理集群的 Master 失败,ZooKeeper 会在 Standby 模式下的 Master 中选出新的集群管理者,新选举出的 Master 会从 ZooKeeper 中读取集群的 Worker、Driver Client、Application 等元数据信息进行恢复并和 Worker、Driver Client、 Application 沟通,最后 Master 会变成 ACTIVE 的状态,开始对外提供服务。

#### 王家林每日大数据语录 Spark 篇 0043(2015.12.15 于上海)
* Worker 在退出的时候会通过 ExecutorRunner 杀死 Executor 并且会将运行在当前 Worker 下的 Driver Client 删除掉, 最终 AppClient 端的 SparkDeploySchedulerBackend 会收到 Master 发过来的 StatusUpdate 信息来处理 Executor 丢失的信息,Task 会被重新分配。

#### 王家林每日大数据语录 Spark 篇 0044(2015.12.16 于上海)
* Spark 无论在目前的五种资源调度框架(Standalone、Mesos、Yarn、Local、EC2)下都是通过 Executor 中的线程池负责 Task 的最终计算。

#### 王家林每日大数据语录 Spark 篇 0045(2015.12.16 于上海)
* Worker 在接受到 Master 发过 来的启动 Executor 的命令后会创建 ExecutorRunner,借助 ExecutorRunner 的 fetchAndRunExecutor 会最终创建出 ExecutorBackend 进程。

#### 王家林每日大数据语录 Spark 篇 0046(2015.12.25 于上海)
* Spark 采用 Curator 来封装和简化对 ZooKeeper 的使用,Spark 使用 Curator 后自动管理链接,当 Client 中断后会自动 进行重新链接,同时 Curator 框架提供更多使用友好的 API 来使用 ZooKeeper。

#### 王家林每日大数据语录 Spark 篇 0047(2015.12.28 于深圳)
* Spark Context 在实例化的时 候会向Master注册当前的Application,如果注册成功,Master会通过schedule方法为当 前的 Application 分配 Executor,最终在通过 Executor 并发的执行 Job 的 Tasks

#### 王家林每日大数据语录 Spark 篇 0048(2015.12.29 于深圳)
* SparkDeploySchedulerBackend 在创建的时候会实现 AppClientListener 接口,进而来感知 connected、disconnected、dead、 executorAdded、executorRemoved 等事件并进行相关事件的处理。

#### 王家林每日大数据语录 Spark 篇 0049(2015.12.29 于深圳)
* Spark 的 Master 在为 Application 分配计算的资源的 Executor 时候有两种分配策略:一种是将当前 Application 的计算尽可能的分配到不同的计算节点上,这也是 Spark 默认的方式;另外一种是给 Application 分配尽量少的节点,这种情况比较适合于 CPU 计算密集而内存使用比较少的 Application。

#### 王家林每日大数据语录 Spark 篇 0050(2015.12.30 于深圳)
* Spark 的 Task 的执行之前会 从序列化的 Task 中获取 Task 执行时依赖的文件和文件下载的位置信息,然后使用 updateDependencis下载依赖的Files、JARs等,随后会创建TaskContext,进而运行Task。

#### 王家林每日大数据语录 Spark 篇 0051(2015.1.13 于深圳)
* TaskRunner 会将 Task 的执行 状态汇报给 SchedulerBackend 的 DriverActor,而 DriverActor 会把信息转发给 TaskScheduler 的 statusUpdate 进行处理。

#### 王家林每日大数据语录 Spark 篇 0052(2015.1.13 于深圳)
* Executor 的内存被其内部所有 的任务所共享,Executor 所能够支持的 Task 的并行处理的数量取决于其所持有的 CPU Cores 的数量,另外我们可以通过 spark.executor.memory 来设置 Executor 可以最多使用多大的 内存,而要评估实际运行中内存的消耗则需要了解每个 Task 处理数据的规模和计算过程中 所需要的临时内存空间,一个比较简单的方式是通过 RDD Cache,进而从 BlockManager 日 志中看出每个 Cache 分区的大小的估计值。

#### 王家林每日大数据语录 Spark 篇 0053(2015.1.13 于深圳)
* 如果 Spark 的内存特别紧张, 可以考虑采用更多的分区来增加任务的并行度的方式来减少每个 Task 所需要处理数据的大 小,当然作为代价之一就是要进行更多批次任务计算。

#### 王家林每日大数据语录 Spark 篇 0054(2015.1.18 于深圳)
* 在 Spark 中,RDD 实现用户的业务逻辑,Storage 实现用户的数据管理;Storage 在本身架构也是基于 Master-Slaves 结构的,在用户实际编程的过程中可以通过RDD的persist来持久化数据,而数据的具体持久化是通过 Storage 模块来完成的。

#### 王家林每日大数据语录 Spark 篇 0055(2016.1.18 于深圳)
* BlockManagerMaster 会持有整个 Application 的 Block 的位置、Block 所占用的存储空间等元数据信息,在 Spark 的 Driver 的 DAGScheduler 中就是通过这些信息来确认数据运行的本地性的!

#### 王家林每日大数据语录 Spark 篇 0056(2016.1.18 于深圳)
* 所有的 Executor 中的 BlockManager 的在启动的时候会调用 initialize 来向 BlockManagerMasterActor 注册自己,注册的信息包括 blockManagerId,当前 Node 可用的最大内存数据以及当前 BlockManagerSlaveActor 的引用等。

#### 王家林每日大数据语录 Spark 篇 0057(2016.1.19 于深圳)
* DiskBlockManager 管理和维 护逻辑上 Block 和存储 Disk 上的物理 Block 的映射,这些物理 Block 文件会通过 hash 的方 式 spark.local.dir 或者通过 SPARK_LOCAL_DIRS 设置的目录中。

#### 王家林每日大数据语录 Spark 篇 0058(2016.1.19 于深圳)
* 每个 Executor 中都会有一个 BlockManagerSlaveActor 实例用来接受来自 BlockManagerMasterActor 的命令并做一些清 理工作且响应 Master 获取 Block 的状态的请求。

#### 王家林每日大数据语录 Spark 篇 0059(2016.1.19 于深圳)
* Driver 在创建 SparkContext 实例的时候会初始化名称为 HeartbeatReceiver 的 Actor 实例,Executor 启动时候会获得该实例的引用句柄并且通过该句柄来维持 Executor 和 Driver 之间的心跳。

#### 王家林每日大数据语录 Spark 篇 0060(2016.1.19 于深圳)
* 缓存是 Spark 构建迭代算法和 快速交互式查询的关键,用户可以调用persist或者cache来标记一个RDD需要持久化,当 触发一个 Action 后才将 RDD 缓存起来以供后续计算使用。如果缓存丢失,可以根据原来的 计算过程进行重新计算。

#### 王家林每日大数据语录 Spark 篇 0061(2016.1.19 于深圳)
* Spark 的计算尽量不要落到磁 盘上,因为一般情况下重新计算一个 Partition 的速度可能和从硬盘上读数据差不多,另外 磁盘操作会面临出错和写硬盘的开销,所以也就导致了失败重算比读磁盘持久化的数据好很 多,当然在计算逻辑特别复杂或者是从一个超大规模的数据集中筛选出一小部分数据的情形 使用磁盘才可能是一个比较理想的选择。

#### 王家林每日大数据语录 Spark 篇 0062(2016.1.19 于深圳)
* 使用多个 Replication 的方式 可以提供故障的快速恢复能力,此时备份的数据可以让 Application 直接使用副本而不是重 新去计算丢失的 Partition 的数据。

#### 王家林每日大数据语录 Spark 篇 0063(2016.1.19 于深圳)
* 使用 Spark on Tachyon 方式 别适合于集群中有大量内存以及有很多计算任务的情景,通过 Tachyon 可以使得多个 Executor 共享一个内存池、显著的减少 GC 的开销,并且在 Executor 异常退出的时候缓存在内存中的数据也不会丢失!!!ode 可用的最大内存数据以及当前 BlockManagerSlaveActor 的引用等。

#### 王家林每日大数据语录 Spark 篇 0064(2016.1.20 于深圳)
* 之所以需要 Shuffle 是因为分布在不同的计算节点的数据可能具有某类共同特征需要 Aggregate 到一个节点上进行计算。 Shuffle 的过程必须持久化中间结果,否则的话一旦数据丢失,就需要重新计算全部依赖的 RDD.

#### 王家林每日大数据语录 Spark 篇 0065(2016.1.21 于深圳)
* ShuffleMapTask 运算结果的时候一方面是要在 Executor 端对其进行处理,另外一方面是在 Task 运行结束的时候在 Driver 端要对其进行 Shuffle Write 输出处理,这样就非常方便下一个 Stage 中的 Task 获得属于 自己的 Shuffle 数据。

#### 王家林每日大数据语录 Spark 篇 0066(2016.1.21 于深圳)
* TaskRunner 将 Task 的执行状 态通过 ExecutorBackend 的 statusUpdate 汇报给 Driver 后,Driver 会转发给 TaskSchedulerImpl 的 statusUpdate 去处理,statusUpdate 方法会根据 Task 的结果的不同 运行状态进行不同的处理。

#### 王家林每日大数据语录 Spark 篇 0067(2016.1.21 于深圳)
* ShuffleMapTask 处理后的结构 实际上是 MapStatus,在 DAGScheduler 的 handleTaskCompletion 中会通过 MapOutputTrackerMaster 进行 registerMapOutputs,这样下一个 Stage 的 Task 就可以找到 上一个 Stage 的数据了。

#### 王家林每日大数据语录 Spark 篇 0068(2016.1.21 于深圳)
* 在 Spark 中无论 Hash Based Shuffle 还是 Sort Based Shuffle,内置的 Shuffle Reader 都是 HashShuffleReader。

#### 王家林每日大数据语录 Spark 篇 0069(2016.1.21 于深圳)
* 在 Spark 中可以考虑在 Worker 节点上使用固态硬盘以及把 Worker 的 Shuffle 结构保存到 RAM Disk 的方式来极大的提高性 能。

#### 王家林每日大数据语录 Spark 篇 0070(2016.1.22 于深圳)
* Spark Stage 内部是一组计算 逻辑完全相同但处理数据不同的分布式并行运行的 Task 构成 ,Stage 内部的计算都以 Pipeline 的方式进行,不同的 Stage 之间是产生 Shuffle 的唯一方式。

#### 王家林每日大数据语录 Spark 篇 0071(2016.1.22 于深圳)
* Spark 集群在默认情况每台 host 上只有一个 Worker,而每个 Worker 默认只会为当前应用程序分配一个 Executor 来执行 Task,但实际上通过配置 spark-env.sh 可以让每台 host 上有若干的 Worker,而每个 Worker 下面又可以有若干个 Executor。

#### 王家林每日大数据语录 Spark 篇 0072(2016.1.22 于深圳)
* 默认情况下 Spark 的 Executor 会尽可能占用当前机器上尽量多的 Core,这样带来的一个好处就是可以最大化的提高计算 的并行度,减少一个 Job 中任务运行的批次,但带来的一个风险就是如果每个 Task 占用内 存比较大,就需要频繁的 spill over 或者有更多的 OOM 的风险。

#### 王家林每日大数据语录 Spark 篇 0073(2016.1.23 于深圳)
* 处理 Spark Job 的过程中如果 出现特别多的小文件,这时候就可以通过 coalesce 来减少 Partition 的数量,进而减少并 行运算的 Task 的数量来减少过多任务的开辟,从而提升硬件的使用效率

#### 王家林每日大数据语录 Spark 篇 0074(2016.1.23 于深圳)
* 处理 Spark Job 时候如果发现 某些 Task 运行的特别慢,这个时候应该考虑增加任务的并行度,减少每个 Partition 的数 据量来提高执行效率。

#### 王家林每日大数据语录 Spark 篇 0075(2016.1.23 于深圳)
* 处理 Spark Job 时候如果发现 某些 Task 运行的特别慢另外一个处理办法是增加并行的 Executor 的个数,这样每个 Executor 分配的计算资源就变少了,可以提升硬件的整体使用效率。

#### 王家林每日大数据语录 Spark 篇 0076(2016.1.23 于深圳)
* 处理 Spark Job 时候如果发现 比较容易内存溢出,一个比较有效的办法就是增加 Task 的并行度,这样每个 Task 处理的 Partition 的数据量就变少了,减少了 OOM 的可能性。

#### 王家林每日大数据语录 Spark 篇 0077(2016.1.23 于深圳)
* 处理 Spark Job 时候如果发现 比较容易内存溢出,另外一个比较有效的办法是减少并行的 Executor 数量,这样每个 Executor 就可以分配到更多的内存,进而增加每个 Task 使用的内存数量,降低 OOM 的风险。

#### 王家林每日大数据语录 Spark 篇 0078(2016.1.23 于深圳)
* 提升 Spark 硬件尤其是 CPU 使 用率的一个方式就是增加Executor的并行度,但是如果Executor过多的话,直接分配在每 个 Executor 的内存就大大减少,在内存的操作就减少,基于磁盘的操作就越来越多,导致 性能越来越差。

#### 王家林每日大数据语录 Spark 篇 0079(2016.1.26 于深圳)
* 适当设置 Partition 分片数是 非常重要的,过少的 Partition 分片数可能会因为每个 Partition 数据量太大而导致 OOM 以及频繁的 GC,而过多的 Partition 分片数据可能会因为每个 Partition 数据量太小而导 致执行效率低下。

#### 王家林每日大数据语录 Spark 篇 0080(2016.1.26 于深圳)
* 如果 Spark 中 CPU 的使用率不 够高,可以考虑为当前的程序分配更多的 Executor,或者增加更多的 Worker 实例来充分的 使用多核的潜能。

#### 王家林每日大数据语录 Scala 篇 0081(2016.4.11 于深圳)
* Scala 函数式编程的精髓在于尽可能的推迟副作用,通过 Scala 提供的手段来延迟程序的执行,这也符合函数编程中把副作用推到越晚越好的理念,而 Spark 正是这一理念的完美应用!


