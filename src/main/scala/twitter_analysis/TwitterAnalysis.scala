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
