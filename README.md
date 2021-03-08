

## 背景
Pika是一个可持久化的大容量redis存储服务，兼容string、hash、list、zset、set的绝大部分接口(兼容详情)，解决redis由于存储数据量巨大而导致内存不够用的容量瓶颈。用户可以不修改任何代码从redis迁移到pika服务。具有良好的兼容性和稳定性，被360公司内部使用超过3000+实例，github社区超过3.8K star。由于单机pika容量受限于单块硬盘容量的大小，360公司业务和社区对分布式pika集群的需求越来越强烈，因此我们推出了原生分布式pika集群,发布pika版本v3.4。与pika+codis集群方案相比，pika集群不需要额外部署codis-proxy模块，同时由于codis对pika创建和管理slot操作的支持并不友好，需要运维人员大量介入。
## 架构
#### 集群部署结构
![部署结构](https://raw.githubusercontent.com/kernelai/image/main/%E9%83%A8%E7%BD%B2%E7%BB%93%E6%9E%84.png)
以3个pika节点的集群为例，集群部署结构如上图所示：
1. 部署Etcd集群作为pika manager的元信息存储。
2. 3台物理机上分别部署pika manager，并配置好Etcd的服务端口。Pika manager会向etcd注册，并争抢成为leader。集群中有且只有一个pika manager能够成为leader并向etcd中写入集群数据。
3. 3台物理机上分别部署pika节点，然后把pika节点的信息添加到pika manager中。
4. 为了负载均衡，把pika的服务端口注册到LVS中。

#### 数据分布
![数据分布](https://raw.githubusercontent.com/kernelai/image/main/pika%E9%9B%86%E7%BE%A4%E6%95%B0%E6%8D%AE%E5%AD%98%E5%82%A8%E6%9E%B6%E6%9E%84.png)
为了对数据按照业务进行隔离，Pika集群引入table的概念，不同的业务数据存储在不同的table中。业务数据按照key的hash值存储到对应的slot上面。每一个slot会有多个副本，从而形成一个replication group。replication group中的所有slot副本具有相同的slot ID，其中一个slot副本是leader，其他副本为follower。为了保证数据的一致性，只有leader提供读写服务。可以使用pika manager可以对slot进行调度迁移，使数据和读写压力均匀的分散到整个pika集群中，从而保证了整个集群资源的充分利用并且可以根据业务压力和存储容量的需要进行水平扩容和缩容。

pika使用rocksdb作为存储引擎，每个slot会创建对应的rocksdb。pika中的每个slot都支持读写redis 5种数据结构。因此数据迁移的时候会特别方便，只需迁移pika中的slot即可。但同时也存在资源占用过多的问题。目前的pika在创建slot的时候会默认创建5个rocksdb，分别来存储5种数据结构。在table中含有大量slot或者创建大量table的时候会使单个pika节点含有多个slot，进而创建过多的rocksdb实例，占用了过多系统资源。在后续版本中一方面会支持创建slot的时候根据业务需要创建一种或几种数据结构，另一方面会持续对pika中的blackwidow接口层进行优化，减少对rocksdb的使用。

#### 数据处理
![数据处理](https://raw.githubusercontent.com/kernelai/image/main/pika%E8%8A%82%E7%82%B9%E6%95%B0%E6%8D%AE%E5%A4%84%E7%90%86.png)
 1. 当pika节点接收到用户请求时，解析层处理解析redis协议，并把解析好的结果交给router层进行判断。
 2. router根据key的hash结果找到key对应的slot，并判断slot是否在本地节点上。
 3. 如果key所在的slot在其他节点，则根据请求创建一个task放入队列中，并把请求转发给peer节点来处理。当task接收到请求的处理结果后把请求返回给客户端。
 4. 如果key所在的slot属于本地节点，就直接本地处理请求并返回给客户端。
 5. 对于需要本地处理的写请求，先通过replication manager模块写binlog，异步复制到其他slot副本。process layer根据一致性的要求，写入leader slot。其中blackwidow是对rocksdb的接口封装。

我们把proxy内嵌的pika中，不需要单独部署。与redis cluster相比，客户端不需要感知proxy的存在，只需像使用单机一样使用集群。可以把pika节点的服务端口挂载到LVS中，实现压力在整个集群的负载均衡。
#### 日志复制
pika中replication manager模块负责日志的主从同步。为了兼容redis，pika支持非一致日志复制，leader slot直接在db中写入数据而无需等待从follower slot的ack应答。同时也支持raft一致性协议方式的日志复制，需要满足收到大多数副本的ack才写入db。
###### 非一致日志复制
![非一致性日志复制](https://raw.githubusercontent.com/kernelai/image/main/%E9%9D%9E%E4%B8%80%E8%87%B4%E6%80%A7%E6%97%B6%E5%BA%8F%E5%9B%BE.png)
在非一致场景下处理流程如下：
1. 处理线程接收到客户端的请求，直接加锁后写入binlog和并操作db。
2. 处理线程返回客户端response。
3. 辅助线程发送BinlogSync同步请求给follower slot，同步日志。
4. follower slot返回BinlogSyncAck报告同步情况。
###### 一致性日志复制
![一致性日志复制](https://raw.githubusercontent.com/kernelai/image/main/%E4%B8%80%E8%87%B4%E6%80%A7%E5%A4%8D%E5%88%B6%E6%97%B6%E5%BA%8F%E5%9B%BE.png)
在一致性日志复制场景下：
1. 处理线程把客户端请求写入binlog文件
2. 通过发送BinlogSync请求向从库同步
3. 从库返回BinlogSyncAck报告同步状况
4. 检查从库应答满足大多数后将相应的请求写入db
5. 将response返回客户端

#### 集群元数据处理
我们在codis-dashboard的基础上二次开发了pika manager（简称PM），作为整个集群的全局控制节点，用来部署和调度管理集群。PM里保存了整个集群的元数据及路由信息。
* 增加了集群创建多表的功能，方便业务根据表的不同来实现业务数据隔离。
* 支持创建表时指定slot数目和副本数目，方便运维根据业务的规模和故障容忍度创建table。
* 从逻辑上把group的概念改为replication group，使得原来的进程级别的数据和日志复制转变为slot级别的复制。
* 支持创建table时创建密码来隔离业务的使用。客户端只需要执行auth和select语句就可以认证并对指定的table进行操作。
* 支持slot迁移，方便根据业务需求进行扩容和缩容。
* 集成哨兵模块，PM会不断的向集群中的pika节点发送心跳，监测存活状态。当PM发现leader slot down时，会自动提升binlog偏移最大的slave slot为leader。
* 存储后端支持元数据写入etcd，保证元数据的高可用。
* pika manager通过不断向etcd争抢锁来成为leader，来实现pika manager的高可用。

#### 后记
pika原生集群的推出解决了单机pika受限于磁盘容量的限制，可以按照业务的需求进行水平扩容。但仍然有一些缺陷，如基于raft的内部自动选主功能的缺失，基于range的数据分布，及监控信息的展板等功能。后续版本我们会一一解决这些问题。









