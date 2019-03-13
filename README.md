### **题目要求**</br>
100GB url 文件，使用 1GB 内存计算出出现次数 top100 的 url 和出现的次数。</br>
### **整体思路**</br>
采用Top-K问题的常见思路，把大文件转换为小文件再进一步处理。首先采用Hash(Url)%100的方法，把Url分成100小份，如果生成的文件大小仍然大于内存大小，则使用另一种哈希方法继续划分数据。</br>

接着在内存中维护一个大小为100的小根堆遍历小文件，将每个小文件依次加载到内存，使用Map<string,int>获得每个Url在文件中出现的次数，把每个小文件的HashMap放入小根堆中，每次放入都调整堆。
最后堆中所剩就是重复出现次数的Top100。</br>
![image](https://github.com/XuQianJin-Stars/UrlTop100/blob/master/pics/Top100Url.png)

### **造数据和划分数据**</br>
使用Java生成数据，数据格式为 xxx.xxx/xxxxx/xxxxx…。一条Url大约40B，则100GB的Url数据共100*1024*1024*1024/40 约27亿条数据。</br>
划分数据也使用Java。</br>
内存占用分析/保证内存小于1G</br>
划分阶段，将数据写入到文件后，及时释放内存。
在读每个小文件并计算Top100阶段，程序每次读入一个文件，同时一致在系统中维护一个大小为100的堆，内存使用不超过题目规定。</br>
### **拆分数据使用生产者-消费者模式（多线程读/写）**</br>

鉴于上述方案的劣势，我们提出使用生产者-消费者模式来实现，同时为了提高读效率，使用多线程读/写。

## 设计思路

1、采用生产者-消费者模式，对读写任务可控，从而读内存使用可控，防止出现omm；

2、使用多线程读/写，提高效率；

3、借助内存文件映射MappedByteBuffer，分段多线程读取文件；

## 示意图

 ![img](https://github.com/XuQianJin-Stars/UrlTop100/blob/master/pics/849051-20170521110059322-1730309386.png)

## 类图

![img](https://github.com/XuQianJin-Stars/UrlTop100/blob/master/pics/849051-20170521110108869-958193079.png)

Master、*Task、FileSpiltter —— 和之前一样的职责；只是不同的实现方式；

*Pool —— 读/写线程池，使用ThreadPoolExcutor实现，使用有界队列、有界线程池；

TaskAllocater —— *Task任务初始化，填充*Pool；

Queue —— 生产者/消费者共享Blocking任务队列，有界，大小可配置；

FileLine —— 包装一行文件内容，这里的一行为csv文件内容的一行，出现\n字节时任务换行；

## 时序图

![img](https://github.com/XuQianJin-Stars/UrlTop100/blob/master/pics/849051-20170521110608838-981702885.png)

## 优劣势

优势

1、内存使用可控，避免OMM问题；

2、读文件效率提高，整个文件拆分时延降低；

劣势

1、文件拆分逻辑和任务控制逻辑复杂，代码复杂度高；

2、文件内容的有序性无法保证；FileWriteTask从queue里获取FileLine是随机的，无法保证文件内容写入的有序性，这里的有序性是指相对于源文件的行位置；

3、文件拆分后子文件大小的均匀性无法保证。

## 性能调优

生产者/消费者方式的实现，使得任务控制和文件拆分逻辑复杂，最初版本性能比‘单线程读-多线程写’的方案还要查，后来通过调优得到了比较满意的结果。

总结下来：需要针对几个关键性参数进行调节，以求得到最佳性能，这几个关键性的参数包括：FileReadTaskNum（生产者数量，源文件读取任务数量）、FileWriteTaskNum（消费者数量，子文件写入任务数量）、queueSize（任务队列大小）。

下面简单罗列下在测试机的调优过程：

### **测试环境**</br>
使用本人笔记本，
```
OS: windows 10 64bit
cpu: 4core, 主频：2.4GHZ
mem：16G
jdk version：Java HotSpot(TM) 64-Bit ，1.8.0_101
```
1. 使用 GenBigFile.java生成数据
```
java -Xmx1024m -Xms1024m -XX:+UseG1GC -XX:+PrintGCDetails 
 -cp UrlTop100-1.0-SNAPSHOT.jar 
 com.forwardxu.makedata.GenBigFile D:\\soucecode\\data\\s0\\data.txt 10 600
```
2. 使用AppMain.java根据内容按照hash进行数据切分文件
```
java -Xmx1024m -Xms1024m -XX:+UseG1GC -XX:+PrintGCDetails  
-cp UrlTop100-1.0-SNAPSHOT.jar 
com.forwardxu.filespilt.AppMain D:\\soucecode\\data\\s0 data.txt 10 PRODUCERCONSUMER 24 8 10240
```
3. 使用FileSpiltToWC.java对上一步产生的文件一个个进行top100的处理并写入结果文件
```
java -Xmx1024m -Xms1024m -XX:+UseG1GC -XX:+PrintGCDetails  
-cp UrlTop100-1.0-SNAPSHOT.jar 
com.forwardxu.spiltfiletowc.FileSpiltToWC D:\\soucecode\\data\\s0 D:\soucecode\\data\\s1
```
4. 使用FileMerge.java对上一步产生的所有文件合并后进行top100的处理并写入结果文件
```
java -Xmx1024m -Xms1024m -XX:+UseG1GC -XX:+PrintGCDetails  
-cp UrlTop100-1.0-SNAPSHOT.jar 
com.forwardxu.filemerge.FileMerge D:\\soucecode\\data\\s1 D:\soucecode\\data\\s2 data.txt
```

### **参考说明**
测试案例中两种方案中一种拆分阶段逻辑参考:[https://github.com/daoqidelv/filespilt-demo](https://github.com/daoqidelv/filespilt-demo)

