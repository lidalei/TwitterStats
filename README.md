# Twitter-Stats
The HBase project at Technical University of Madrid. The goal is to support various top N queries on trending Twitter topics.

# Schema of table twitterStats
* Key - timestamp
* Column family - each row has several languages, such as en, es and it
* Column - each column family has topHashTag1, topHashTag2, topHashTag3, topHashTag1Freq, topHashTag2Freq, topHashTag3Freq

# Queries methodology
* Given language and time interval ([startTimesatmp, endTimestamp)), performing range scan with column family (language) and using a StreamTopK object to store and retrieve top N words
* Given a list of languages and time interval, performing range scan with column families (languages) and using a set of StreamTopK objects to store and retrieve top N words for each language
* Given time interval, performing range scan without column family and using a StreamTopK object to store and retrieve top N words for all languages
* The key point here is to traverse scan result using keySet() to get column families (all languages) instead of predefined languages

# Test
The program was tested in hbase standalone mode and hbase distributed mode with hbase ZooKeeper.
