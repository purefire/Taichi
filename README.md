# Taichi
A big data analysis engine based on bitmap, targeting very little resource (disk, cpu, memory) and fast calculating, and long term storage.

Generally, big data analysis for internet business require UV(number of identical individuals), PV(total visits), retention(the rate of the individaul remain compared) analysis etc. The common solution is to use database/data storage which is heavy and costy.
A quick and lightweight solution is to use bitmap to do such analysis and store history data. The Taichi project is targetting such a solution that:
1.	PC level resource (like 8-core cpu and 16G memory, with 1T hard disk storage) can hold ten millions of data per business lines, and ten thousands of business lines; 
2.	and all these data may store many years without disk extension
3.	all data and analysis result are 100% accurate
4.	Fast – single operation may cost less than 1ms; a calcation of thousand business lines may cost less than 5s; support non-blocking operations for all mess calculation.
5.	Support group operation and complex formula
6.	Support HA

As a design result, Taichi may NOT support individual detail other than UV, like you may search if one client do visit on specific date in a specific action, but cannot store/find how long does he spend on that action;

Bitmap is based on 32-bit integer, though Taichi does support bigger ids. However it will cost much more if the id is over N*2^31 , which N is > 16. An alternative solution in Taichi is keep a mapping between the real id and stored id, which total count of stored-id is smaller than N*2^31 . For example, mobile device is usually 64-bit or even 128-bit long, but actually the existing and potential customers may be much smaller than 2^31 .

Again, Taichi is targetting small and fast data analyis for UV, PV, retention and other mess operations based on that. Developer may need common calution if other analysis required like dividual detail.

### Infrastructure
The project is using vert.x framework.
The project is using activemq, kafka for input, mongo as meta-info storage, local file system as file storage (remove some private implementation as object storage, may develop some public implementation on s3 oss later)
Docker is introduced to enable mongo and MQs for local deployment and test - but still it is suggested that storage, including DB and file storage is to have distributed deployment (for HA and data safety)

### Local Env 
Just execute:
``` shell
 $ docker-compose up -d
```

### Debug/Run

> Just run/debug: `lv.jing.taichi.web.Bootstrap`

## Build

### Build distributions:

```shell
 $ gradle clean build -x test
``` 

### Unzip distribution tarball:

```shell
 $ tar xvzf truck-web/build/distributions/truck-web-0.0.1-SNAPSHOT.tar -C YOUR_UNZIP_DIR
```

### Start web app in your unzip path:

```shell
 $ ./bin/truck-web -h
 usage: truck-web
  -c,--config <arg>   config path. (default: config.yml in classpath)
  -h,--help           print usage.
  -p,--port <arg>     listen port. (default: 8888)
 $ ./bin/truck-web -c "classpath:config.prod.yml" -p 8080
```
### Special thanks
Special thanks to @jeffsky who introduce and setup the vert.x framework to this project

# 太极项目
精确统计DAU、留存，精确计算分析不同业务线用户行为是互联网服务的基本分析方式之一；Bitmap是统计最准确、速度最快、资源占用最小的方式。
太极项目期望能在bitmap的基础上构架轻量级的数据分析和存储工具。其目标是：
> 功能需求
- [x]运行在PC机级别的服务器上（比如8core，16G内存和1T磁盘）
- [x]十万到十亿级别用户
- [x]可以在小容量的条件下保存三年或以上的数据；保障数据完整性
- [x]100%精确性
- [x]支持各种输入和输入，支持表达式计算，支持更多的比较、导入导出操作
- [x]支持更广泛类型（String)，包括deviceid（imei，mac地址，IDFA，IDFV等）
- [x]支持接口或者MQ/Kafka输入，支持gPGC等输出
> 非功能需求
- [x]ns级写入，秒级操作，比如Count (ms)/AND/OR/XOR (s)
- [x]保持个体数据准确性 (本日ms/非当日冷数据s)
- [x]支持HA/分布式

虽然一般的bitmap实现都是基于32位的，太极项目支持超过32位整数的数据。但一般地，建议使用百亿级的内的数据；对二十亿以内，即16×32位整数最大值(2^31)以内的数据支持较好。更大的数据会造成计算和存储性能的极大下降。
（Roaring已经支持64位整数，对此的支持正在测试中，预计近期上线）

更大的数据一般建议使用内置的映射功能。实际上，业务的用户不太可能超过2^31,即21亿。

其他更详细的数据存储分析，比如找到单个用户实际在某一个页面里花费的时长，则需要使用更通用的解决方案。

### 项目结构
本项目使用vert.x构架。本地docker需要安装mongo等；但在生产实践中，强烈建议使用分布式部署，并将mongo、文件储存等分开，以确保信息安全。

### 本地环境 
执行
``` shell
 $ docker-compose up -d
```

### 运行和调试
> Just run/debug: `lv.jing.taichi.web.Bootstrap`

## 构建
### Build distributions:
```shell
 $ gradle clean build -x test
``` 

### 解压:
```shell
 $ tar xvzf web/build/distributions/web-0.0.1-SNAPSHOT.tar -C YOUR_UNZIP_DIR
```

### 运行:
```shell
 $ ./bin/truck-web -h
 usage: truck-web
  -c,--config <arg>   config path. (default: config.yml in classpath)
  -h,--help           print usage.
  -p,--port <arg>     listen port. (default: 8888)
 $ ./bin/truck-web -c "classpath:config.prod.yml" -p 8080
```
