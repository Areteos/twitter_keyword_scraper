# twitter_keyword_scraper
A basic Scala app using Spark to scrape the twitter API for keyword relevance.

## Building
Can be staged or built using sbt-native-packager, with all the options that entails!

## Use
This applet is intended for use via CLI, taking in exactly 2 arguments: a query string, and a number of tweets to consider. The app will retrieve tweets using the given query and number, and attempt to ascertain the relative importance of all potential keywords present in the tweets. This (in theory!) allows a user to know the best set of keywords for gaining exposure within a certain infospace or hashtag on twitter.

## Operation
The query is used to search the twitter API for up to the given number of tweets. The number is handled relatively robustly: anything smaller than twitter's minimum number of tweets retrieved (10) will be rounded up, while the app will pay attention to twitter's signs that the search API will be cut off. Unfortunately, with only basic API access, that cutoff comes very quickly, usually around 350 tweets. Finding ways around this is a major development goal to come.

The app sanitises text of the tweets retrieved tweets, removing punctuation and lowercasing the text, then tokenises, filters out empty strings (""), and `CountVectorise`s. TFIDF is then applied to the vectorised word data to produce weights for each word. Meanwhile, a single "engagement" metric is calculated for each tweet from the various available public metrics (likes, retweets, etc). At present, this calculation a simple summation, but a more complex function could be implemented fairly easily.

With this feature and label data, a random forest model is finally trained on the processed data. The `featureImportance` from the random forest model is then used in conjunction with the `CountVectorizer`'s `vocabulary` to construct a new `DataFrame` matching the various keywords to their respective importances. This is then filtered for 0-importance keywords, sorted in descending order of importance, displayed in the terminal, and saved to disk in JSON Lines format. 

## Further work needed
Firstly, more data is needed. A few hundred tweets is not enough to conclusively establish a keyword trend. Filtering URLs out of the tweet text would be helpful. Author IDs should be included in the random forest's feature set to control for more and less popular users. Finally, it's very possible that the feature importance of the random forest is not the best direct metric for keyword strength.
