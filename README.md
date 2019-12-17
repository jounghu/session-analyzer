## Sessoion 分析

    电商用户行为分析

#### 同一个Session相同行为进行聚合



#### 访问时长和访问步长通过自定义accumulator进行累加

1. 根据session的stepLength计算出来，stepLength在1-3,4-6 ... 之间的session数量

2. 根据session的累计costTime计算出来，costTime在1-30s,1m-2m ... 之间的session数量

然后存放入MySQL.


#### 实现随机抽取session算法

根据session所有的占比，然后随机抽取对应比例session打印出来。

