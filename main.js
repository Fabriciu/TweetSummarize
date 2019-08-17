'use strict';

let config = require('./config');

let MongoClient = require('mongodb').MongoClient;
let mongoUrl = "mongodb+srv://"+config.mongoUser+":"+config.mongoPass+"@"+config.mongoCluster;

MongoClient.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true }, function(err, db) {
	if(err) throw err;
	var dbo = db.db("tweeterData")
	dbo.collection("lastTweets").find().sort({user_followers_count:-1}).toArray(function(err, result) {
		if(err) throw err;
		for(var tweet of result) {
			dbo.collection("mostFollowedUsers").updateOne({user_name: tweet.user_name}, 
				{ $setOnInsert: 
					{
						user_id: tweet.user_id, 
						user_name: tweet.user_name,
						user_screen_name: tweet.user_screen_name,
						user_followers_count: tweet.user_followers_count,
						hashtag: tweet.hashtag
					}
				}, {upsert: true}, function(err, data) {
				if(err)
					console.log(err)
			})
		}
		db.close()
	})
})





