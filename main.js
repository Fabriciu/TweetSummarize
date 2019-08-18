'use strict';

let config = require('./config');

let MongoClient = require('mongodb').MongoClient;
let mongoUrl = "mongodb+srv://"+config.mongoUser+":"+config.mongoPass+"@"+config.mongoCluster;

MongoClient.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true }, function(err, db) {
	if(err) throw err;
	var dbo = db.db("tweeterData")
	
	dbo.collection("lastTweets").aggregate([
	{
		'$group': { 
			'_id': { 
				user_id: '$user_id',
				user_name: '$user_name',
				user_screen_name: '$user_screen_name',
				user_followers_count: '$user_followers_count'
			}
		}
	},
	{
		'$sort': {
			'_id.user_followers_count': -1
		}
	},
	{
		'$project': {
			'_id': 0,
			'user_id': '$_id.user_id',
			'user_name': '$_id.user_name',
			'user_screen_name': '$_id.user_screen_name',
			'user_followers_count': '$_id.user_followers_count'
		}
	}]).toArray(function(err, results){
		if(err) throw err
			console.log(results)
		dbo.collection("mostFollowedUsers").insertMany(results, function(err, result) {
			if(err) throw err
		})

	});

	
	dbo.collection("lastTweets").aggregate([
	{
		'$group': {
			'_id': { $hour: '$created_at' },
			count: { $sum: 1 }
		}
	},
	{
		'$sort': {
			'_id': 1
		}
	},
	{
		'$project': {
			'_id': 0,
			'dayHour': '$_id',
			'tweets': '$count'
		}
	}]).toArray(function(err, results){
		if(err) throw err
			console.log(results)
		dbo.collection("tweetsPerHour").insertMany(results, function(err, result) {
			if(err) throw err

		})

	});
	
	dbo.collection("lastTweets").aggregate([
	{
		'$group': {
			'_id': { hashtag: '$hashtag', lang: '$lang', location: '$user_location' },
			count: { $sum: 1 },
		}
	},
	{
		'$sort': {
			'_id.hashtag': 1,
			'count': -1
		}
	},
	{
		'$project': {
			'_id': 0,
			'hashtag': '$_id.hashtag',
			'tweets': '$count',
			'lang': '$_id.lang',
			'user_location': '$_id.location'
		}
	}]).toArray(function(err, results){
		if(err) throw err
			console.log(results)
		dbo.collection("tweetsPerLangAndLocation").insertMany(results, function(err, result) {
			if(err) throw err

		})

	});
	db.close()
})





