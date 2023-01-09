package com.github.Areteos
package twitter_analysis


import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql._

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import javax.ws.rs.core.UriBuilder


object TwitterAnalysis {
	val textColumnName = "text"
	val wordColumnName = "words"
	val authorColumnName = "author"
	val idColumnName = "id"
	val engagementColumnName = "engagement"
	val intermediateFeatureColumnName = "raw features"
	val featureColumnName = "features"

	val exampleRawTweets = """{"data":[{"id":"1612118401392025600","author_id":"2244151426","public_metrics":{"retweet_count":56,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":0},"text":"RT @mamafloelgueta: Hola!! Yo nuevamente apelando a su empatía. Ayúdame xfa! @ClinicoVM a la cabeza Orfali, quién es el dueño; manda a sus…","edit_history_tweet_ids":["1612118401392025600"]},{"id":"1612118371683926016","author_id":"617853906","public_metrics":{"retweet_count":0,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":0},"text":"#twitter #BMPRT #PointmanRT #Retweet #RT #tweet #marketing #media #affiliatemarketing #digitalmarketing #socialmedia #business #social #twitter #help #support #donate #StJude https://t.co/3tf9lgAZ3H","edit_history_tweet_ids":["1612118371683926016"]},{"id":"1612118300422737923","author_id":"3334589205","public_metrics":{"retweet_count":0,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":2},"text":"#blog #donate #charity #teddies #KnittingPattern #Knitting #toys #orphans #Children #preschool #nursery #poverty #Food #help #malawi #Scotland #Trauma https://t.co/Eq9Fbi1t1E https://t.co/EbHyyZF3Tv","edit_history_tweet_ids":["1612118300422737923"]},{"id":"1612118266398269442","author_id":"1542898798250409986","public_metrics":{"retweet_count":0,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":0},"text":"Do you need perfect work done for you?\n#class kicking my ass\n#write this #essay\n#pay assignment\n#pay essay.,...\n#Law\n#research paper\n#essay due\n#Maths\n#Homework\n#Summer classes\n#Paper due\n#Anatomy labs\n#lab reports\n#physics\n#Economics \n#Help\n#Dm for all your academic help.","edit_history_tweet_ids":["1612118266398269442"]},{"id":"1612118227848671232","author_id":"1369048265325309954","public_metrics":{"retweet_count":20,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":0},"text":"RT @jkm11106: Donate $1 or more to our rural #Texas public school #preschool class &amp; my after school #prek group.  Drop ur list\uD83D\uDC47&amp; I’ll #rt.…","edit_history_tweet_ids":["1612118227848671232"]},{"id":"1612118070494990336","author_id":"3236688725","public_metrics":{"retweet_count":0,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":4},"text":"Şəhərdə (28 tərəfdə) şirin nəsə məsləhətli bir şey yeməyə hara var. Təcili məsləhət lazımdı. Hava soyuqdu. Dostumla donuruq \n#rt #help","edit_history_tweet_ids":["1612118070494990336"]},{"id":"1612118066845949954","author_id":"906018574619131905","public_metrics":{"retweet_count":20,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":0},"text":"RT @jkm11106: Donate $1 or more to our rural #Texas public school #preschool class &amp; my after school #prek group.  Drop ur list\uD83D\uDC47&amp; I’ll #rt.…","edit_history_tweet_ids":["1612118066845949954"]},{"id":"1612118049351499781","author_id":"736631570841362432","public_metrics":{"retweet_count":95,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":0},"text":"RT @SafferTheGaffer: ‼️A CRY FOR HELP‼️\n\nWe’re desperately appealing for a foster home for a young, male bull breed dog. Ideally somewhere…","edit_history_tweet_ids":["1612118049351499781"]},{"id":"1612117807272976387","author_id":"1599961873432166404","public_metrics":{"retweet_count":0,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":1},"text":"All creative people want to do the unexpected, help out Scooter School supplies ⬇️⬇️⬇️\n @Walmart  #LevelUp #school #teacher #prototype #lifestyle #help #Scooter #helpinghand #schoolsupplies \nhttps://t.co/z8OshlUK0x","edit_history_tweet_ids":["1612117807272976387"]},{"id":"1612117792223887361","author_id":"1553983615285039105","public_metrics":{"retweet_count":5,"reply_count":0,"like_count":0,"quote_count":0,"impression_count":0},"text":"RT @Fi_za92: Winter package distribution in deserving and needy people of district khyber by @MSfoundationPK \nMay Allah accept your efforts…","edit_history_tweet_ids":["1612117792223887361"]}],"meta":{"newest_id":"1612118401392025600","oldest_id":"1612117792223887361","result_count":10,"next_token":"b26v89c19zqg8o3fqk406o4wzcpnju24bln078w9osk8t"}}"""


	val spark: SparkSession = SparkSession.builder()
		.appName("Twitter Keyword Analysis")
		.master("local[4]")
		.getOrCreate()

	import spark.implicits._

	// TODO further expand the amount of tweets downloadable
	def retrieveAndProcessTweets(query: String, number: Int, nextToken: String = ""): Dataset[Row] = {
		if (number <= 100)
			processTweets(retrieveTweets(query, number, nextToken))._1
		else {
			val (tweets, newNextToken) = processTweets(retrieveTweets(query, 100, nextToken))
			if (newNextToken == "")
				tweets
			else
				tweets.union(retrieveAndProcessTweets(query, number - 100, newNextToken))
		}
	}

	def retrieveTweets(query: String, number: Int = 100, nextToken: String = ""): String = {
		val client = HttpClient.newHttpClient
		val uriBuilder = UriBuilder.fromPath("https://api.twitter.com/2/tweets/search/recent")
			.queryParam("query", query)
			.queryParam("tweet.fields", "author_id,text,public_metrics")
			.queryParam("max_results", Math.min(100, Math.max(number, 10)))
		if (nextToken != "") uriBuilder.queryParam("next_token", nextToken)
		val uri = uriBuilder.build()

		val request = HttpRequest.newBuilder
			.uri(uri)
			.timeout(Duration.ofSeconds(3))
			.header("Authorization", "Bearer AAAAAAAAAAAAAAAAAAAAABKekwEAAAAAu0Dracqjo4IlJQiR3dZUGR9soUI%3DSLf1g7hZ9wzcQgoiTs8ZsDSWU7HrsD0vjJYsQ8A3fueIwb6OkA")
			.build()

		val response = client.send(request, HttpResponse.BodyHandlers.ofString)
		val responseBody = response.body()
		responseBody
	}

	def processTweets(rawJson: String): (DataFrame, String) = {
		val rawFrame = spark.read.json(spark.createDataset(Seq(rawJson)))
		val basicData = rawFrame.select("data")
			.withColumn("data", explode(col("data")))
			.select("data.*").drop("edit_history_tweet_ids")


		(basicData.map(row => {
			val authorId = row.getString(0)
			val id = row.getString(1)
			val publicMetrics = row.getStruct(2)
			val text = row.getString(3)

			val engagement = calculateEngagement(publicMetrics)
			val preprocessedText = preprocessTweetText(text)

			(authorId, id, engagement, preprocessedText)
		})
			.withColumnRenamed("_1", authorColumnName)
			.withColumnRenamed("_2", idColumnName)
			.withColumnRenamed("_3", engagementColumnName)
			.withColumnRenamed("_4", textColumnName),
		try {
			rawFrame.select("meta.next_token").head().getString(0)
		} catch {
			case _: AnalysisException =>  // This indicates the end of the pages of search twitter is willing to give
				""
		})
	}


	def preprocessTweetText(text: String): Array[String] = {
		text.replaceAll("[\\p{Punct}&&[^#]]", "").toLowerCase().split("[ \n]").filter(s => s != "")
	}

	def calculateEngagement(row: Row): Double = {
		(for (i <- 0 until row.size) yield row.getLong(i)).sum
	}

	def extractKeywords(dataFrame: DataFrame): DataFrame = {
		val countVectorizer = new CountVectorizer()
			.setInputCol(textColumnName)
			.setOutputCol(intermediateFeatureColumnName)
		val idf = new IDF()
			.setInputCol(countVectorizer.getOutputCol)
			.setOutputCol(featureColumnName)
		val randomForest = new RandomForestRegressor()
			.setFeaturesCol(featureColumnName)
			.setLabelCol(engagementColumnName)

		val vectorizationModel = countVectorizer.fit(dataFrame)
		val frequencyVectorised = vectorizationModel.transform(dataFrame)
		val idfModel = idf.fit(frequencyVectorised)
		val tfidfData = idfModel.transform(frequencyVectorised)
		val randomForestModel = randomForest.fit(tfidfData)

		(for (i <- 0 until randomForestModel.featureImportances.size)
			yield {
				(vectorizationModel.vocabulary(i), randomForestModel.featureImportances(i))
			}).toDF("keyword", "importance").filter("importance > 0").sort($"importance".desc)
	}

	def main(args: Array[String]): Unit = {
		if (args.length != 2) sys.error("ERROR Must supply exactly 2 arguments")

		val query = args(0)
		val numTweets = args(1).toInt

		val preProcessedTweets = retrieveAndProcessTweets(query, numTweets)

		println(preProcessedTweets.count())
		preProcessedTweets.show(200)
		preProcessedTweets.printSchema()
		val keywords = extractKeywords(preProcessedTweets)
		keywords.show(200)
		keywords.write.mode(SaveMode.Overwrite).json(s"keyword strengths for query ${query.replaceAll("[\\p{Punct}&&[^#]]", "")}.json")

	}
}
