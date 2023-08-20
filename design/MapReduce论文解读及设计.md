TODO：

分离数据流和控制流：

# 基础功能：

## 全流程

1. Client向Master提交作业申请，Master只保留一些作业相关的元数据，并不会直接保存要处理的数据（针对场景就是大数据量场景，数据从Master中专浪费时间也容易造成单点瓶颈。）。Master返回Client执行作业的Worker信息，以及一些任务切分信息。
2. Master向Worker发送作业执行信息。
3. Client与Worker做数据交互、上传任务数据并执行。Worker直接从本地磁盘读取。
4. Map任务执行，并将结果输出到本地磁盘。
5. Master通知Worker执行Reduce任务，从Map的结果集中读取数据
6. 执行Reduce任务，并输出结果。



> 第一个demo版本不考虑Master节点无法全量加载数据的情况，假定client的数据能全量/至少元数据全量在Master节点上。

> 将本地磁盘类比GFS，Master与Worker共用的本地磁盘。

## Master数据结构



## 容错

## 任务粒度

## 备份任务

# 优化

分区函数

预聚合

输入输出格式

额外附加输出

本地调试

状态信息同步展示（client可观测）



# 模块划分：

控制流程、数据流程、故障处理/容错、任务状态管理、Worker工作量/机器资源管理、

# 角色划分：

至少分为三个角色，Client、Master、Worker

功能划分：

- Client：
  - JobSubmit：提交作业申请
  - TaskSubmit：提交作业数据
  - 显示作业结果
- Master：
  - ResourceManager-资源管理/分配：决定每个client提交的作业由那些worker执行。
  - WorkerStatusCollector-任务状态管理：资源管理、容错相关
  - 故障处理/容错：
  - TaskManager-作业执行计划管理：任务颗粒度，map分成多少个、reduce分成多少个
    - backup task
  - 数据准确性校验：
- Worker：
  - 故障处理/容错：
  - WorkerStatusManager-：
  - MapExecutor-Map任务执行：
  - ReduceExecutor-Reduce任务执行：
  - MapReader-Map数据读取：
  - MapWriter-Map数据输出：
  - ReduceReader-Reduce数据读取：
  - ReduceWriter-Reduce数据输出：

# 流程图

总流程

客户端作业及任务提交

Master资源管理/分配

服务端作业分配（Master/Worker）

服务端节点/任务状态收集（Master/Worker）

Worker map任务执行

Worker reduce任务执行

# A&Q：

1. 提交作业的并发度、速度控制等应该隶属哪一块功能？如何控制？
   1. 并发这些参数应该属于用户的可调节参数，只是服务器根据自身资源及安全或者其他考虑，会对用户设置的并发数有一个区间限制。针对用户单独设置的并发度可考虑使用或者不适用，以及修改后使用。
2. Master切分了任务后，把任务信息分别发送给Client和Worker，如何保障两者在交互过程中是符合作业预期？数据切片准确，输出地址准确。如何校验任务作业符合预期？
3. （Master同一时间处理的多个Client的作业申请）Master向Worker发送任务计划时是定时批量下发，还是及时消息传递？是只发送Worker必要的任务计划，还是广播全量的任务计划？针对不同的作业提交，是单个作业为粒度下发，还是定时下发所有作业的任务计划？
   1. 定时批量下发
   2. 只下发Worker必要的任务信息。
   3. 所有作业统一下发。
4. 所有作业的任务计划统一下发是否会对单个Worker节点产生资源瓶颈？是否会对单个worker造成性能瓶颈。
   1. 这个下发不是任务分配，所以不会存在单个worker节点因为倾斜/分配不均匀带来的性能问题。不论是任务计划瞬间到达Worker还是逐步到达Worker，任务所需要执行的工作量总体是不变的，从总量来说也不会存在差异。
   2. 这个问题可以类比短期大流量请求，在Worker节点使用队列。
5. Master中的状态信息在全量下发过程中会频繁的更新元数据信息。
   1. 元数据/状态信息应该随心跳一样，及时更新，保证Master可以纵览全局。
6. 为什么Reduce节点需要拉去所有Worker节点的数据？
   1. Master在对Reduce任务切分时以什么指标来切分/分配任务？Map任务通过数据块进行切分，按照数据大小对任务切分。
   2. 按照中间键值对的Key对Reduce任务进行切分。之前在做Map任务时，同一个Key可能分布在不同的Worker节点上。Reduce任务需要读取所有的Map结果才可以得到单一Key的全部数据，在一个Worker节点上。
7. Reduce任务在获取自己处理节点数据时是读取所有中间键值对到本地，还是只读取要处理的中间键值对到本地？
   1. Reduce任务肯定要遍历全量数据，可以只读取自己要处理的Key的数据到本地。
8. Map的输出结果如何存储？是所有的结果存在一个文件中？还是根据Reduce任务分配Worker节点进行分别存储？还是每一类Key存到一个文件中（分区函数）？
9. shuffle是处理数据倾斜，没有shuffle阶段不影响程序执行结果的正确性。
10. 什么时候决定哪个worker处理哪个Reduce任务/负责哪个中间Key？
    1. 目标：如何在不同数据分布的时候，保证各个节点的工作负载相对均衡。
11. Map的中间结果是存放在内存中还是存放在磁盘当中？
12. Map任务的结果如何通知Master？是定时反馈快照信息给Master，定时返回信息是随心跳返回还是单独返回？还是任务完成后通过回调通知master？
    1. 定时任务上报实现会相对简单。
13. Master与Worker之间的心跳是Master主动询问Worker还是Worker定时反馈Master？
    1. Worker定时上报心跳信息，避免Master向Worker询问近况的开销，每次询问的信息没有变化可以直接省略。
14. Worker返回给Master的信息包含哪些内容？
    1. Map任务结果存放地址，不返回中间键值对数据。
15. Reduce节点的选取与Map节点是否有相同的节点，相同节点可以减少读取数据时的跨网络传输。
16. Worker节点资源分配的粒度，是Job作业层级，还是Map、Reduce任务层级？