package rplaces

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.time.Instant


case class PaintEvent(
    timestamp: Instant, 
    id: Types.UserId, 
    pixel_color_index: Types.ColorIndex, 
    coordinate_x: Types.CoordX, 
    coordinate_y: Types.CoordY, 
    end_x: Option[Types.CoordX], 
    end_y: Option[Types.CoordY],
)

object RPlacesQuery {

    //
    // 1. Getting started
    //
    val spark : SparkSession = SparkSession.builder
      .master("local[*]") 
      .appName("rPlaces Query") 
      .getOrCreate()

    val sc : SparkContext = spark.sparkContext

    //
    // 2. Read-in r/places Data
    //

    def loadPaintsRDD(): RDD[PaintEvent] = RPlacesData.readDataset(spark, "data/paint_event_500x500+750+750.parquet")

    // Change to INFO or DEBUG if need more information
    sc.setLogLevel("WARN")

    def main(args: Array[String]): Unit = {
        val paintsRDD = loadPaintsRDD()
        val q = new RPlacesQuery(spark, sc)
        val canvas = Canvas(2000, 2000)

        val beforeWhitening = timed("Last colored event", q.lastColoredEvent(paintsRDD))
        println("\n********** Last colored event **********")
        println(beforeWhitening)
        println("\n****************************************")

        println(beforeWhitening.toEpochMilli)

        val colors = Colors.idMapping.keys.toList

        val colorRanked = timed("naive ranking", q.rankColors(colors, paintsRDD))
        println("\n********** Ranked Colors **********")
        println(colorRanked)
        println("\n****************************************")


        val colorIndex = q.makeColorIndex(paintsRDD)
        val colorRankedUsingIndex = timed("ranking using inverted index", q.rankColorsUsingIndex(colorIndex))
        println("\n********** Ranked Colors Using Index **********")
        println(colorRankedUsingIndex)
        println("\n************************************************")


        val colorRankedReduceByKey = timed("ranking using reduceByKey", q.rankColorsReduceByKey(paintsRDD))
        println("\n********** Ranked Colors Using Reduce By Key **********")
        println(colorRankedReduceByKey)
        println("\n*******************************************************")


        // val mostActiveUsers = timed("Most active users", q.nMostActiveUsers(paintsRDD))
        // println(mostActiveUsers)
        val mostActiveUsers = timed("Most active users", q.nMostActiveUsers(paintsRDD))
        println("\n********** Most Active Users **********")
        println(mostActiveUsers)
        println("\n****************************************")

        val mostActiveUsersBusiestTile = timed("Most active users busiest tile", q.usersBusiestTile(paintsRDD, mostActiveUsers.map(_._1), canvas))
        println("\n********** Most Active Users Busiest Tile **********")
        println(mostActiveUsersBusiestTile)
        println("\n****************************************************")



        // Render image qui ne marche pas ?
        println("\n********** Colors at time **********")
        val atT = q.colorsAtTime(beforeWhitening, paintsRDD)
        println(atT.take(10).toList)
        timed("render color before the whitening", canvas.render("color_render_before_whitening", canvas.rddToMatrix(atT, 31), Colors.id2rgb))
        println("\n*************************************")

        val similarity = timed("color similarity between history and the whitening", q.colorSimilarityBetweenHistoryAndGivenTime(beforeWhitening, paintsRDD).cache())
        println("\n********** Colors similarity **********")
       
        println(similarity.take(10).toList)
        timed("render color similarity between history and the whitening", canvas.render("similarity_render", canvas.rddToMatrix(similarity, 0.0), Colors.ratio2grayscale))
        println("\n*************************************")

        println(timing)

        spark.close()
    }

    val timing = new StringBuffer("Recap:\n")
    def timed[T](label: String, code: => T): T = {
        val start = System.currentTimeMillis()
        val result = code
        val stop = System.currentTimeMillis()
        val s = s"Processing $label took ${stop - start} ms.\n"
        timing.append(s)
        print(result)
        // print(s)
        result
    }
}


class RPlacesQuery(spark: SparkSession, sc: SparkContext) {
    import Types._

    //
    // 3. Find the last paint event before the whitening
    //

    def lastColoredEvent(rdd: RDD[PaintEvent]): Instant = {
        val nonWhiteEvents = rdd.filter(event => event.pixel_color_index != 1)

        val latestEvent = nonWhiteEvents.reduce((event1, event2) =>
            if (event1.timestamp.isAfter(event2.timestamp)) event1 else event2
        )

        latestEvent.timestamp
    }

    
    //
    // 4. Compute a ranking of colors
    //

   def occurencesOfColor(color: ColorIndex, rdd: RDD[PaintEvent]): Long = {
        rdd.filter(event => event.pixel_color_index == color).count()
    }

    def rankColors(colors: List[ColorIndex], rdd: RDD[PaintEvent]): List[(ColorIndex, Long)] = {
        colors.map(color => (color, occurencesOfColor(color, rdd)))
        .sortBy(-_._2) 
    }

    def makeColorIndex(rdd: RDD[PaintEvent]): RDD[(ColorIndex, Iterable[PaintEvent])] = {
        rdd.map(event => (event.pixel_color_index, event)).groupByKey()
    }

    def rankColorsUsingIndex(index: RDD[(ColorIndex, Iterable[PaintEvent])]): List[(ColorIndex, Long)] = {
        index.mapValues(events => events.size.toLong)
            .sortBy(-_._2)
            .collect()
            .toList
    }

    def rankColorsReduceByKey(rdd: RDD[PaintEvent]): List[(ColorIndex, Long)] = {
        rdd.map(event => (event.pixel_color_index, 1L))
            .reduceByKey(_ + _)
            .sortBy(-_._2)
            .collect()
            .toList
    }


    //
    // 5. Find for the n users who have placed the most pixels, their busiest tile
    //

    def nMostActiveUsers(rdd: RDD[PaintEvent], n: Int = 10): List[(UserId, Long)] = {
        rdd.map(event => (event.id, 1L))
            .reduceByKey(_ + _)
            .sortBy(-_._2)
            .take(n)
            .toList
    }



    // Ici ça prend en compte tous les users sauf le numero 1 ? bizarre
    def usersBusiestTile(rdd: RDD[PaintEvent], activeUsers: List[UserId], canvas: Canvas): List[(UserId, (TileId, Long))] = {
        val userTileCount = rdd
            .filter(event => activeUsers.contains(event.id)) 
            .flatMap(event => {
            val tileId = canvas.tileAt(event.coordinate_x, event.coordinate_y)
            List((event.id, (tileId, 1L)))
            })

        val userTilesGrouped = userTileCount.groupByKey()

        val userBusiestTile = userTilesGrouped.mapValues(tileCounts =>
            tileCounts.groupBy(_._1)
            .mapValues(_.map(_._2).sum) 
            .maxBy(_._2) 
        )

        userBusiestTile.collect().toList
    }





    //
    // 6. Find the percentage of painting event at each coordinate which have the same color as just before the whitening
    //
    def colorsAtTime(t: Instant, rdd: RDD[PaintEvent]): RDD[((CoordX, CoordY), ColorIndex)] = {
        val eventsBeforeTime = rdd.filter(_.timestamp.compareTo(t) <= 0)
        val groupedByCoordinates = eventsBeforeTime.map(event => ((event.coordinate_x, event.coordinate_y), event))
        val latestEventsByCoordinate = groupedByCoordinates.reduceByKey((event1, event2) =>
            if (event1.timestamp.compareTo(event2.timestamp) >= 0) event1 else event2
        )
        val colorsAtGivenTime = latestEventsByCoordinate.mapValues(_.pixel_color_index)
        colorsAtGivenTime
    }


    def colorSimilarityBetweenHistoryAndGivenTime(t: Instant, rdd: RDD[PaintEvent]): RDD[((CoordX, CoordY), Double)] = {
        val colorsAtGivenTime = colorsAtTime(t, rdd)

        val eventsGroupedByCoordColor = rdd.filter(_.timestamp.compareTo(t) < 0).map(event => ((event.coordinate_x, event.coordinate_y, event.pixel_color_index), 1L))
          .reduceByKey(_ + _)

        val eventsGroupedByCoord = eventsGroupedByCoordColor.map { case ((x, y, _), count) => ((x, y), count) }
          .reduceByKey(_ + _)

        val joined = eventsGroupedByCoordColor.map { case ((x, y, color), count) => ((x, y), (color, count)) }
          .join(eventsGroupedByCoord)
          .map { case ((x, y), ((color, count), total)) => ((x, y), (color, count.toDouble / total)) }

        val result = joined.join(colorsAtGivenTime)
          .map { case ((x, y), ((color, ratio), colorAtTime)) => ((x, y), if (color == colorAtTime) ratio else 0.0) }
        
        result.sortBy { case ((x, y), ratio) => (-ratio, x, y) }
    }





}
