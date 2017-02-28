# storm-examples

Storm worker consumer data from kafka and compute.
Every five minute workers send the results to hbase, opentsdb and falcon-transfer which is a json-rpc server


Related Artical:
http://zhaif.us/index.php/2016/04/30/strom2/


这是一个简单的storm应用，其从kafka读取数据，以5min为一个时间窗口，统计数据。然后将计算结果以两种维度分别存入hbase和opentsdb。
同时，支持将数据打入open-falcon的transfer模块，用以告警。

代码详细的业务逻辑介绍请参考这篇文章：
http://zhaif.us/index.php/2016/04/30/strom2/

ps ：整个代码逻辑比较简单，包含kafka、storm、hbase、opentsdb的一些基本代码，是我刚接触storm时写的一些尝试性代码。槽点较多，请包涵~


Email：
zhai3516@163.com
