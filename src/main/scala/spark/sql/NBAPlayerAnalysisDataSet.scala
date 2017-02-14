package spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


/**
  * 版权：DT大数据梦工厂所有
  * 时间：2017年1月26日；
  * NBA篮球运动员大数据分析决策支持系统：
  *   基于NBA球员历史数据1970~2017年各种表现，全方位分析球员的技能，构建最强NBA篮球团队做数据分析支撑系统
  * 曾经非常火爆的梦幻篮球是基于现实中的篮球比赛数据根据对手的情况制定游戏的先发阵容和比赛结果（也就是说比赛结果是由实际结果来决定），
  * 游戏中可以管理球员，例如说调整比赛的阵容，其中也包括裁员、签入和交易等
  *
  * 而这里的大数据分析系统可以被认为是游戏背后的数据分析系统。
  * 具体的数据关键的数据项如下所示：
  *   3P：3分命中；
  *   3PA：3分出手；
  *   3P%：3分命中率；
  *   2P：2分命中；
  *   2PA：2分出手；
  *   2P%：2分命中率；
  *   TRB：篮板球；
  *   STL：抢断；
  *   AST：助攻；
  *   BLT: 盖帽；
  *   FT: 罚球命中；
  *   TOV: 失误；
  *
  *
  *   基于球员的历史数据，如何对球员进行评价？也就是如何进行科学的指标计算，一个比较流行的算法是Z-score：其基本的计算过程是
  *     基于球员的得分减去平均值后来除以标准差，举个简单的例子，某个球员在2016年的平均篮板数是7.1，而所有球员在2016年的平均篮板数是4.5
  *     而标准差是1.3，那么该球员Z-score得分为：2
  *
  *   在计算球员的表现指标中可以计算FT%、BLK、AST、FG%等；
  *
  *
  *   具体如何通过Spark技术来实现呢？
  *   第一步：数据预处理：例如去掉不必要的标题等信息；
  *   第二步：数据的缓存：为加速后面的数据处理打下基础；
  *   第三步：基础数据项计算：方差、均值、最大值、最小值、出现次数等等；
  *   第四步：计算Z-score，一般会进行广播，可以提升效率；
  *   第五步：基于前面四步的基础可以借助Spark SQL进行多维度NBA篮球运动员数据分析，可以使用SQL语句，也可以使用DataSet（我们在这里可能会
  *     优先选择使用SQL，为什么呢？其实原因非常简单，复杂的算法级别的计算已经在前面四步完成了且广播给了集群，我们在SQL中可以直接使用）
  *   第六步：把数据放在Redis或者DB中；
  *
  *
  *   Tips：
  *     1，这里的一个非常重要的实现技巧是通过RDD计算出来一些核心基础数据并广播出去，后面的业务基于SQL去实现，既简单又可以灵活的应对业务变化需求，希望
  *       大家能够有所启发；
  *     2，使用缓存和广播以及调整并行度等来提升效率；
  *
  */

object NBAPlayerAnalysisDataSet {


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    var masterUrl = "local"

    val conf = new SparkConf().setMaster(masterUrl).setAppName("NBAPlayerAnalysisDataSet")

    if (args.length > 0) masterUrl = args(0) //

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    var dataPath = "src/main/resources/nbaBasketball/" // 数据存放的目录
    val dataTmp = "src/main/resources/tmp/"

//    FileSystem.get(new Configuration()).delete(new Path(dataTmp), true)

//    for (year <- 1970 to 2016){
//
//      val statsPerYear = sc.textFile(s"${dataPath}/leagues_NBA_${year}*")
//      statsPerYear.filter(_.contains(",")).map(line => (year, line)).saveAsTextFile(s"${dataTmp}/nbaStatsPerYear/${year}")
//
//    }

    val NBAStats = sc.textFile(s"${dataTmp}/nbaStatsPerYear/*/*")
    val filteredData = NBAStats.filter(line => !line.contains("FG%")).filter(line => line.contains(",")) // draft ETL
      .map(line => line.replace(",,",",0,"))

    filteredData.persist(StorageLevel.MEMORY_AND_DISK) //因为这样可以更好的使用内存且不让数据丢失

    val itemStats = "FG,FGA,FG%,3P,3PA,3P%,2P,2PA,2P%,eFG%,FT,FTA,FT%,ORB,DRB,TRB,AST,STL,BLK,TOV,PF,PTS".split(",")
    itemStats.foreach(println)


    def computeNormalize(value: Double, max: Double, min: Double) = {
      value / math.max(math.abs(max), math.abs(min))
    }

    case class NBAPlayerData(year: Int, name: String, position: String, age: Int, team: String, gp: Int, gs: Int, mp: Double, stats: Array[Double], statsZ: Array[Double] = Array[Double](), valueZ: Double = 0, statsN: Array[Double] = Array[Double](), valueN: Double = 0, experience: Double = 0)


    def rawData2NBAPlayerData(line: String, bStats: Map[String, Double] = Map.empty,
                              zStats: Map[String, Double] = Map.empty) = {

      val content = line.replace(",,",",0,")
      val value = content.substring(1, content.length - 1).split(",")

      val year = value(0).toInt
      val name = value(2)
      val position = value(3)
      val age = value(4).toInt
      val team = value(5)
      val gp = value(6).toInt
      val gs = value(7).toInt
      val mp = value(8).toDouble
      val stats:  Array[Double]= value .slice(9,31).map(score => score.toDouble)
      val statsZ: Array[Double] = Array.empty
      val valueZ: Double = Double.NaN
      val statsN: Array[Double] = Array.empty
      val valueN: Double = Double.NaN
      val experience: Double = Double.NaN

      if (bStats.isEmpty)

      NBAPlayerData(year, name, position, age, team, gp, gs, mp, stats, statsZ, valueZ, statsN, valueN, experience)

    }

    val basicData = computeStats(filteredData, itemStats).collect()

    val basicDataBroadcast = sc.broadcast(basicData)

    spark.stop()

  }

  def computeStats(filteredData: RDD[String], itemStats: Array[String]): RDD[String]  = {

    filteredData
  }

}
