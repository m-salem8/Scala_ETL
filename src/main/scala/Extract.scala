import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer  // <- Import this for ListBuffer (will be used for teams list append)

object Extract {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().appName("NBA Data").master("local[*]").getOrCreate()

    //To handle Sql
    import spark.implicits._

    // instantiate httpclient
    val httpClient = HttpClients.createDefault()
    //List to be appended with all gameids of the 4 teams required for season 2021-2022
    val allGameIds = new ListBuffer[Long]()


    // ##################################### PART (1) - Extract games ###################################
    // Fetch NBA teams data
    val httpGetTeams = new HttpGet("https://www.balldontlie.io/api/v1/teams")
    val httpResponseTeams = httpClient.execute(httpGetTeams)
    val entityTeams = httpResponseTeams.getEntity
    val responseStringTeams = EntityUtils.toString(entityTeams, "UTF-8")

    // reading the response and make and covert it to datasets
    val teamsDF = spark.read.json(Seq(responseStringTeams).toDS)

    //List of teams in question
    val Teams_List = List("Phoenix Suns", "Atlanta Hawks", "LA Clippers", "Milwaukee Bucks")

    //filter the data based on the Teams_list
    val filteredTeamsDF = teamsDF
      .withColumn("team", explode($"data"))
      .select("team.id", "team.full_name")
      .filter($"team.full_name".isin(Teams_List: _*))

    filteredTeamsDF.show()

    //foreach team in the list, find the team_id, set the currrent page=1, total pages to be a max integer
    Teams_List.foreach { teamName =>
      val teamId = filteredTeamsDF.filter($"full_name" === teamName).select("id").collect()(0)(0)
      var allGamesDF: Option[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] = None
      var currentPage = 1
      var totalPages = Int.MaxValue


      //While loop to iterate over each page, Query parameters are current page, team_id, season = 2021-2022
      while (currentPage <= totalPages) {
        val httpGetGames = new HttpGet(s"https://www.balldontlie.io/api/v1/games?page=$currentPage&seasons[]=2021&seasons[]=2022&team_ids[]=$teamId")
        val httpResponseGames = httpClient.execute(httpGetGames)
        val entityGames = httpResponseGames.getEntity
        val responseStringGames = EntityUtils.toString(entityGames, "UTF-8")
        //convert the response to a dataset
        val gamesDF = spark.read.json(Seq(responseStringGames).toDS())
        //findding the total pages from the meta section
        totalPages = gamesDF.select($"meta.total_pages").collect()(0)(0).asInstanceOf[Long].toInt

        //filter the data if if the home_team or visitor team = team_id
        val filteredGamesDF = gamesDF
          .withColumn("game", explode($"data"))
          .filter($"game.home_team.id" === teamId || $"game.visitor_team.id" === teamId)

        // append the game_id list
        val gameIds = filteredGamesDF.select("game.id").as[Long].collect()
        allGameIds ++= gameIds

        //in case of data to be appended from a next page, union it. Otherwise None

        allGamesDF = allGamesDF match {
          case Some(df) => Some(df.union(filteredGamesDF))
          case None => Some(filteredGamesDF)
        }
        
        // increase the page by 1
        currentPage += 1
      }
      // write the file locally for each team
      allGamesDF.get.write.json(s"teams-seasons/${teamName.replace(" ", "_")}_games_2021_2022.json")
    }
    
    //println(allGameIds.toList)

    
    // ##################################### PART (2) - Extract stats ###################################
    val season = "2021-2022"
    // avoid over loading the API, the games_id is devided into batches each of 3 ids, then wait for 5 seconds
    val groupSize = 3 // Size of each group of game IDs
    val sleepTime = 5000 // Time to sleep in milliseconds
    val games_id = allGameIds.toArray // Convert the ListBuffer to an Array

    for (i <- 0 until games_id.length by groupSize) {
      val slice = games_id.slice(i, i + groupSize)
      // slice he game_ids and convert it to String
      val games_id_str = slice.map(id => s"game_ids[]=$id").mkString("&")

      //get the response basd on the season, and game_id
      val request = new HttpGet(s"https://www.balldontlie.io/api/v1/stats?seasons[]=$season&per_page=100&$games_id_str")
      val response = httpClient.execute(request)
      val entity = response.getEntity
      val content = EntityUtils.toString(entity)
      //convert to dataset
      val df: DataFrame = spark.read.json(Seq(content).toDS())

      // save the json file in append mode
      df.write.mode("append").json(s"stats/stats.json")
      // print statement of sleep to ensure the functionality
      if (i + groupSize < games_id.length) {
        println(s"Sleeping for $sleepTime milliseconds.")
        // sleep for 5 seconds
        Thread.sleep(sleepTime)
      }
    }

    httpClient.close()
    spark.stop()
  }
}