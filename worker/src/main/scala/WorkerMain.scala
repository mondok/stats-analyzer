import org.apache.spark.{SparkConf, SparkContext}

import org.elasticsearch.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics

object WorkerMain {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("StatsWorker2")
      .setMaster("spark://localhost:7077")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "localhost")
      .set("es.port", "9200")

    val context = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(context)
    val csvFiles = Array("AwardsManagers", "Batting", "FieldingOF", "Managers",
      "Pitching", "SeriesPost", "AwardsPlayers", "BattingPost",
      "FieldingPost", "ManagersHalf", "PitchingPost", "Teams",
      "AllstarFull", "AwardsShareManagers", "CollegePlaying", "HallOfFame",
      "Master", "Salaries", "TeamsFranchises", "Appearances",
      "AwardsSharePlayers", "Fielding", "HomeGames", "Parks", "Schools",
      "TeamsHalf")


    csvFiles.foreach(file => {
      val filePath = s"../2015_stats/$file.csv"
      println(s"Indexing $filePath")
      val df = sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .load(filePath).toDF()
      val index = s"spark/${file.toLowerCase}"
      df.saveToEs(index)
    })


    val master = sqlContext.read.format("es").load("spark/master")
    val college = sqlContext.read.format("es").load("spark/collegeplaying")
    val batting = sqlContext.read.format("es").load("spark/batting")
    master.registerTempTable("master")
    batting.registerTempTable("batting")
    college.registerTempTable("collegeplaying")
    val res = college.as("c").join(master.as("m"), "playerID")
      .where("m.nameGiven = 'Timothy John'").select("c.schoolID").show(10)

    val players1 = batting.as("b").join(master.as("m"))
      .where("m.birthCity = 'Denver'")
      .select("b.AB")

    val players2 = batting.as("b").join(master.as("m"))
      .where("m.birthCity = 'Pittsburgh'")
      .select("b.AB")

    println("next")


    val correlation: Double = Statistics.corr(players1.map(r => r.getAs[Double]("atbats")),
      players2.map(r => r.getAs[Double]("atbats")), "pearson")

    println(correlation)

    context.stop()
  }
}

