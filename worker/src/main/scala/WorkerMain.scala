import org.apache.spark.{SparkConf, SparkContext}

import org.elasticsearch.spark.sql._
object WorkerMain {
  case class Player(name:String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Worker")
      .setMaster("local[2]")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "localhost")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")

    val context = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(context)

    val masterDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load("../2015_stats//Master.csv").toDF()

    val collegeDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load("../2015_stats//CollegePlaying.csv").toDF()

    masterDF.saveToEs("spark/master")
    collegeDF.saveToEs("spark/college")

    val master = sqlContext.read.format("es").load("spark/master")
    val college = sqlContext.read.format("es").load("spark/college")

    val res = college.as("c").join(master.as("m"), "playerID")
      .where("m.nameGiven = 'Timothy John'").select("c.schoolID").show(10)

  }
}

