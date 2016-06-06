import org.apache.spark.{SparkConf, SparkContext}

import org.elasticsearch.spark.sql._

object WorkerMain {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("StatsWorker")
      .setMaster("local[2]")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "localhost")
      .set("es.port", "9500")

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
      val df = sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .load(filePath).toDF()
      val index = s"spark/${file.toLowerCase}"
      df.saveToEs(index)
    })

    val master = sqlContext.read.format("es").load("spark/master")
    val college = sqlContext.read.format("es").load("spark/collegeplaying")

    val res = college.as("c").join(master.as("m"), "playerID")
      .where("m.nameGiven = 'Timothy John'").select("c.schoolID").show(10)

  }
}

