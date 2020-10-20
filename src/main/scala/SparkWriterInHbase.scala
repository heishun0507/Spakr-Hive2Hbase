import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkWriterInHbase {

  def main(args: Array[String]): Unit = {
    //创建sparkConf
    val sparksession: SparkSession = SparkSession
      .builder()
      //设置hive数据库地址
      .config("spark.sql.warehouse.dir", "hdfs://192.168.52.100:8020/user/hive/warehouse")
      .master("local[*]")
      .appName("hive2hbase")
      //开启HQL
      .enableHiveSupport()
      .getOrCreate()

    //    val frame: DataFrame = sparksession.sql("select * from sion.sc")
    //    val dataRdd: RDD[(Integer, (String, String, Integer))] = frame.rdd.flatMap(row => {
    //      val rowkey = row.getAs[Integer]("sid".toLowerCase)
    //      Array(
    //        (rowkey, ("info", "sid", row.getAs[Integer]("sid".toLowerCase))),
    //        (rowkey, ("info", "cid", row.getAs[Integer]("cid".toLowerCase))),
    //        (rowkey, ("info", "score", row.getAs[Integer]("score".toLowerCase)))
    //
    //      )
    //    })
    //    dataRdd.foreach(println(_))

    saveAsHadoopDatasetDef(sparksession)
    //    saveAsNewAPIHadoopDatasetDef(sparksession)


  }

  def saveAsHadoopDatasetDef(sparksession: SparkSession): Unit = {
    val sc: SparkContext = sparksession.sparkContext
    //创建HBaseConfiguration连接zookeeper
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "192.168.52.100,192.168.52.110")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //Hbase表名
    val tablename = "student"

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    //设置输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //设置输出的表名
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    //模拟数据
    //    val indataRDD = sc.makeRDD(Array("3,Rongcheng,M,26", "4,Guanhua,M,27"))
    //  }
    //    val rdd: RDD[(ImmutableBytesWritable, Put)] = indataRDD.map(_.split(',')).map { arr => {
    //      val put = new Put(Bytes.toBytes(arr(0))) //行健的值rowkey
    //      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1))) //info:name列的值
    //      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(arr(2))) //info:gender列的值
    //      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(3))) //info:age列的值
    //      //封装数据
    //      (new ImmutableBytesWritable, put)
    //    }
    //    }
    val HiveDF: DataFrame = sparksession.sql("select * from sion.sc")
    //    +---------+---------+-----------+--+
    //    | sc.sid  | sc.cid  | sc.score  |
    //      +---------+---------+-----------+--+
    //    | 1       | 1       | 80        |
    //     | 2       | 1       | 70        |
    //     | 3       | 1       | 80        |
    //     | 4       | 1       | 50        |
    //     | 6       | 3       | 34        |
    //     | 7       | 2       | 89        |

    //提取表数据
    val dataRdd: RDD[(String, String, String, String)] = HiveDF.rdd.flatMap(row => {

      val rowkey = Integer.toString(row.getAs[Integer]("sid".toLowerCase))
      Array(
        (rowkey, "info", "sid", Integer.toString(row.getAs[Integer]("sid".toLowerCase))),
        (rowkey, "info", "cid", Integer.toString(row.getAs[Integer]("cid".toLowerCase))),
        (rowkey, "info", "score", Integer.toString(row.getAs[Integer]("score".toLowerCase)))

      )
    })
    //封装成Hfile格式
    val rdd: RDD[(ImmutableBytesWritable, Put)] = dataRdd.map { arr => {
      val put = new Put(Bytes.toBytes(arr._1)) //行健的值rowkey
      put.addColumn(Bytes.toBytes(arr._2), Bytes.toBytes(arr._3), Bytes.toBytes(arr._4)) //info:name列的值
      //封装数据
      (new ImmutableBytesWritable, put)
    }
    }
    //执行保存
    rdd.saveAsHadoopDataset(jobConf)
    sc.stop()
  }

  def saveAsNewAPIHadoopDatasetDef(sparksession: SparkSession): Unit = {
    val sc: SparkContext = sparksession.sparkContext
    val tablename = "student"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "192.168.52.100,192.168.52.110")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("1,shuxue,N,15", "2,yuwen,N,16"))
    val rdd = indataRDD.map(_.split(',')).map { arr => {

      val put = new Put(Bytes.toBytes(arr(0))) //行健的值rowkey
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1))) //info:name列的值
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(arr(2))) //info:gender列的值
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(3))) //info:age列的值
      (new ImmutableBytesWritable, put)
    }
    }

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    sc.stop()
  }


}
