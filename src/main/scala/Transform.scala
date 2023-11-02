import org.apache.spark.sql.SparkSession

object Transform extends App {

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._

  // Function to process the Teams data and flatten the required fields
  def processTeamData(df: DataFrame): DataFrame = {
    val df_team_filtered = df
      .withColumn("data_flat", explode(col("data")))
      .select(
        col("data_flat.id").as("game_id"),
        col("data_flat.home_team.full_name").as("hometeam_name"),
        col("data_flat.home_team.id").as("hometeam_id"),
        col("data_flat.home_team_score").as("hometeam_score"),
        col("data_flat.visitor_team.full_name").as("visitteam_name"),
        col("data_flat.visitor_team.id").as("visitteam_id"),
        col("data_flat.visitor_team_score").as("visitteam_score")
      )
    df_team_filtered
  }

  // function to process the stats data and retrieve the desired columns
  def processStatsData(df: DataFrame): DataFrame = {
    val df_stats_filtered = df.withColumn("data_flat", explode(col("data")))
                          .select(
                           col("data_flat.game.id").as("game_id"),
                           col("data_flat.player.id").as("player_id"),
                           col("data_flat.pts").as("pts"),
                           col("data_flat.reb").as("reb"))
        df_stats_filtered
    }
  
  // Building the spark Session
  val spark = SparkSession
    .builder()
    .appName("Transform")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  
  // Loading the teams data 
  val df_AH = spark.read.json("teams-seasons/Atlanta_Hawks_games_2021_2022.json")
  val df_LA = spark.read.json("teams-seasons/LA_Clippers_games_2021_2022.json")
  val df_MB = spark.read.json("teams-seasons/Milwaukee_Bucks_games_2021_2022.json")
  val df_PH = spark.read.json("teams-seasons/Phoenix_Suns_games_2021_2022.json")

  //loading stats data

  val df_stats = spark.read.json("/home/m-salem/etl_project/stats/stats.json")


  // processing the data frames
  val df_AH_filtered = processTeamData(df_AH)
  val df_LA_filtered = processTeamData(df_LA)
  val df_MB_filtered = processTeamData(df_MB)
  val df_PH_filtered = processTeamData(df_PH)

  // Union the all data of Teams
  val df_Games = df_AH_filtered.union(df_LA_filtered).union(df_MB_filtered).union(df_PH_filtered)

  
  val df_stats_filtered = processStatsData(df_stats) 


  // join the two data frames 
  val joinedDF = df_stats_filtered.join(df_Games, Seq("game_id"))

  joinedDF.write.mode("overwrite").option("header", true).csv("csv_file")
  //joinedDF.show(100, 70)
}