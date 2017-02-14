package spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable


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

    /////////////////////////////////////////////////////////

    Logger.getLogger("org").setLevel(Level.WARN)

    var masterUrl = "local"
    var dataPath = "src/main/resources/bballStat/" //数据存放的目录
    val conf = new SparkConf().setMaster(masterUrl).setAppName("NBAPlayerAnalysisDataSet")

    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    /////////////////////////////////////////////////////////

    for (year <- 1970 to 2016){

      val statsPerYear = sc.textFile(s"${dataPath}/leagues_NBA_${year}*")
      statsPerYear.filter(_.contains(",")).map(line => (year, line)).saveAsTextFile(dataPath)

    }

    spark.stop()

  }
}
