# Cassandra_Fast_FullTableScan

You can use this utility to scan your table in cassandra database at a very very fast speed.<br><br>
All you need to do is provide <br>
<ul>
<li>Table identifier</li>
<li>Cluster node IP</li>
<li>Queue (LinkedBlockingQueue)
<li>Configurable options ( <i>values in brackets are default values</i> ) </li>
    <ul>
    <li>Username ( <i>nill</i> )</li>
    <li>Password ( <i>nill</i> )</li>
    <li>Consistency Level ( <i>LOCAL_ONE</i> )</li>
    <li>Number of threads ( <i>16</i> )</li>
    <li>DC Aware Option ( <i>all nodes considered</i> )</li>
    <li>ColumnNames to be dumped ( <i>*</i> )</li>
    <li>Fetch Size per page ( <i>5000</i> )</li>
    </ul>
    
</ul>
<h2>How fast are we talking about?</h2>
<table>
<tr><td>~  229 million rows</td><td>128 threads</td><td>134((18) + 116)</td><td>1DC, 6 nodes in DC, RF:3</td></tr>
<tr><td>~  765 million rows</td><td>128 threads</td><td>487((16) + 471)</td><td>3DCs, 3 nodes in concerened DC, RF:1</td></tr>
</table>
<sub>
the time indicated in column is <b>TotalTime((SetupTime) + EffectiveTime)</b>.
<br>TotalTime = total time taken by script to dump data.
<br>SetupTime = time taken to instantiate cluster, create sessions, etc.
<br>Thus, EffectiveTime to dump the data id TotalTime-SetupTime
<br>All the indicated numbers denote seconds.
</sub>



<h2>Explanation</h2>
<h3>Traditional Cassandra Scan</h3>
<img src="https://github.com/siddv29/cfs/blob/master/images/TraditionalCassandrScan.png"/>
<h3>CFS</h3>
<img src="https://github.com/siddv29/cfs/blob/master/images/CFS.png"/>
<img src="https://github.com/siddv29/cfs/blob/master/images/tokenRangeSplit.png"/>


<h3>Sample Program</h3>

```
package com.cassandra.utility.trial;

import com.cassandra.utility.method1.CassandraFastFullTableScan;
import com.cassandra.utility.method1.Options;
import com.cassandra.utility.method1.RowTerminal;
import com.datastax.driver.core.Row;
import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String... args) throws Exception{
        LinkedBlockingQueue<Row> queue =new LinkedBlockingQueue<Row>();
        
        CassandraFastFullTableScan cfs = 
                new CassandraFastFullTableScan("mykeyspace.table_name",
                        "10.41.55.111",queue,
                        new Options().setUsername("cassandra").setPassword("cassandra"),
                        new PrintStream(new File("/tmp/cfs_round_1.log")));
                        
        CountDownLatch countDownLatch = cfs.start();
        
        new NotifyWhenCFSFinished(countDownLatch).start();
        
        Row row;
        int counter=0;
        while(! ((row = queue.take()) instanceof RowTerminal)){
            System.out.println(++counter+":"+row);
            /*
              you can use row.getString("column1") and so on
            */
        }
    }

    static class NotifyWhenCFSFinished extends Thread{
        CountDownLatch latch;

        public NotifyWhenCFSFinished(CountDownLatch latch) {
            this.latch = latch;
        }

        public void run(){
            System.out.println("Waiting for CFS to complete");
            try{
                latch.await();
            }catch (Exception e1){
                //ignore
            }
            System.out.println("CFS completed");
        }

    }

}
```
