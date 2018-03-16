/**
 * Perform the following steps:
 *
 *     1) Get list of blog tweet templates
 *     2) Get history of blog tweets
 *     3) Choose random template, weighted so least tweeted is most likely
 *     4) Get Twitter credentials
 *     5) Tweet template + link to blog post
 *     6) Update tweet history to reflect this new tweet
 *     7) Use SNS to send e-mail notification
 *
 */

var AWS = require("aws-sdk");
AWS.config.update({region: 'us-east-1'});
var sns = new AWS.SNS();
var Twit = require("twit");
var docClient = new AWS.DynamoDB.DocumentClient();
var blogServer = "https://ian-says.com/";
var snsTopic = "arn:aws:sns:us-east-1:632386139671:blog-tweeted";

/**
 * Get tweet templates from DynamoDB
 */
function getTemplates(soFar) {
    var templateTable = {
//        Limit: 2,
        TableName: "blog-tweet-templates"
    };

    // If we've done nothing yet, make the first DB call
    if (!soFar) {
        return docClient.scan(templateTable).promise()
                .then(getTemplates([]));
    }

    /**
     * This is the function to handle the result of the promise from getting
     * the templates.
     */
    return function (data) {

        // Combine new data with existing data.
        // Also add "tweeted" count of zero - will be populated from history
        // table, later.
        var allItems = soFar;
        data.Items.forEach(function (item) {
            item.tweeted = 0;
            allItems[item["template-id"]] = item;
        });

        // Continue scanning if we have more items
        if (typeof data.LastEvaluatedKey != "undefined") {
            templateTable.ExclusiveStartKey = data.LastEvaluatedKey;
            return docClient.scan(templateTable).promise()
                    .then(getTemplates(allItems));
        }

        // If there are no more items, return our data as a resolved promise,
        // so it can be chained
        return Promise.resolve(allItems);
    };
}


/**
 * Get our tweet history from DynamoDB
 */
function getHistory(soFar) {
    var historyTable = {
//        Limit: 2,
        TableName: "blog-tweet-history"
    };

    // If "soFar" is null, we should make an initial call to DynamoDB.
    if (soFar === null) {
        return function (templates) {
            return docClient.scan(historyTable).promise()
                    .then(getHistory(templates));
        };
    }

    // If we get here, we are handling the response from a tweet history
    // DynamoDB scan
    return function (data) {

        // Enrich templates with history information
        var enriched = soFar;
        data.Items.forEach(function (item) {
            enriched[item["template-id"]].tweeted += 1;
        });

        // Continue scanning if we have more items
        if (typeof data.LastEvaluatedKey != "undefined") {
            historyTable.ExclusiveStartKey = data.LastEvaluatedKey;
            return docClient.scan(historyTable).promise()
                    .then(getHistory(enriched));
        }

        // If there are no more items, select a random tweet and return it
        var selectedTweet = randomTweet(enriched);
        if (selectedTweet !== null) {
            return Promise.resolve(selectedTweet);
        }

        return Promise.reject(new Error("Could not find an active tweet template"));
    };
}


/**
 * We want to weight the selection of tweets,  so the one which
 * has been tweeted more times has less chance of being selected
 */
function randomTweet(tweets) {

    // Find the total of all the tweeted counts
    var total = tweets.map(function (tweet) {
        return tweet.tweeted;
    }).reduce(function (sum, val) {
        return sum + val;
    }, 0);

    // Edge case for empty table (total = 0), set it to 1
    if (total === 0) {
        total = 1;
    }

    // Assign weights based on occurrences
    var weights = tweets.map(function (tweet) {
        if (!tweet.enabled) {
            return null;
        }

        return {
            "template": tweet["template-id"],
            "weight": (total - tweet.tweeted) / total
        };
    }).filter(x => {return x;});

    // Weighting calculation doesn't work if there's only one active template
    // (because total and tweet.tweeted are equal, so weight is zero)
    if (weights.length === 1) {
        return tweets[weights[0].template];
    }

    // Build up an array based on weighting
    var selection = [];
    weights.forEach(function (tweet) {
        var n;
        for (n = 0; n < Math.floor(100 * tweet.weight); n++) {
            selection.push(tweet.template);
        }
    });

    // Edge case - we have no active templates
    if (selection.length === 0) {
        return null;
    }

    return tweets[selection[Math.floor(Math.random() * selection.length)]];
}


/**
 * Get our Twitter credentials
 */
function getTwitterCredentials(tweet) {
    var credentialsTable = {
        TableName: "blog-tweet-keys",
        Key: {
            user: "ianf"
        }
    };

    return docClient.get(credentialsTable).promise()
            .then(processTwitterCredentials(tweet));
}


/**
 * Process the response from a Twitter credential query
 */
function processTwitterCredentials(tweet) {
    return function (data) {
        return Promise.resolve({
            tweet: tweet,
            credentials: data.Item
        });
    };
}


/**
 * We want the past 5 tweets to all have been manual, if we are going to auto-post
 */
function checkForNaturalTweets(data) {
    return function(tweets) {
        var naturalPosts = tweets.data
                                .map(x => x.source)
                                .filter(x => x.includes("Twitter for iPhone"));
        data.okayToPost = tweets.data.length === naturalPosts.length;
        return data;
    };
}


/**
 * Check recent tweets (to avoid spamming your timeline
 */
function checkRecentTweets(data) {
    var twitter = new Twit({
        consumer_key: data.credentials["consumer-key"],
        consumer_secret: data.credentials["consumer-secret"],
        access_token: data.credentials["access-token"],
        access_token_secret: data.credentials["access-secret"]
    });

    return twitter.get("statuses/user_timeline", {count: 5})
            .then(checkForNaturalTweets(data));
}


/**
 * Send a tweet
 */
function sendTweet(data) {
    var twitter = new Twit({
        consumer_key: data.credentials["consumer-key"],
        consumer_secret: data.credentials["consumer-secret"],
        access_token: data.credentials["access-token"],
        access_token_secret: data.credentials["access-secret"]
    });

    return twitter.post("statuses/update", { status: data.tweet.message + " " + blogServer + "articles/" + data.tweet.slug + "/" })
            .then(processSentTweet(data.tweet));
}


/**
 * Mock sending a tweet, for testing
 */
function mockSendTweet(data) {
    return Promise.resolve({
        data: {
            id_str: (Math.floor(Math.random() * 999999999) + 1000000000).toString(),
            text: "This is a mock tweet",
            created_at: "Sometime",
            user: {
                screen_name: "mockuser",
                id_str: "12345"
            }
        }
    }).then(processSentTweet(data.tweet));
}


/**
 * Handle result of a tweet - adds in template details
 */
function processSentTweet(template) {
    return function(data) {
        data.template = template;

        // Check for failure
        if (data.resp.statusCode !== 200) {
            var errorPayload = {
                response: {
                    status: data.resp.statusCode,
                    message: data.resp.statusMessage,
                    date: data.resp.headers.date
                },
                errors: data.data.errors,
                template: template
            };
            updateErrors(errorPayload);
            errorToSns(errorPayload);
            return Promise.reject(errorPayload);
        }

        return Promise.resolve(data);
    };
}


/**
 * Update the tweet errors table in DynamoDB
 */
function updateErrors(data) {
    var params = {
        TableName: "blog-tweet-errors",
        Item: {
            date: data.response.date,
            status: "" + data.response.status + " " + data.response.message,
            "template-id": data.template["template-id"],
            slug: data.template.slug,
            errors: data.errors
                        .map(err => {return "" + err.code + " " + err.message;})
                        .join("; ")
        }
    };

    // Deep into errors here, so use a callback
    docClient.put(params, function (err, data) {
        if (err) {
            console.error(err);
        }
    });
}


/**
 * Send an e-mail on error in tweeting
 */
function errorToSns(data) {
    var params = {
        Message: JSON.stringify(data),
        Subject: "ERROR tweeting about blog: " + data.template.slug,
        TopicArn: snsTopic
    };

    // Use a callback, because we're buried in errors already
    sns.publish(params, function (err, data) {
        if (err) {
            console.error(err);
        }
    });
}


/**
 * Update the history table in DynamoDB with details of our tweet
 */
function updateHistory(tweet) {
    var params = {
        TableName: "blog-tweet-history",
        Item: {
            "template-id": tweet.template["template-id"],
            "tweet-id": tweet.data.id_str,
            message: tweet.data.text,
            slug: tweet.template.slug,
            user: tweet.data.user.screen_name,
            "user-id": tweet.data.user.id_str,
            date: tweet.data.created_at
        }
    };

    // So, "put" doesn't allow us to return the created item, so we pass
    // it on from our params
    return docClient.put(params).promise()
            .then(function () { return Promise.resolve(params.Item); });
}


/**
 * Add an entry to an SNS queue, to trigger an e-mail
 */
function sendToSns(data) {
    var params = {
        Message: JSON.stringify(data),
        Subject: "Tweeted about blog: " + data.slug,
        TopicArn: snsTopic
    };
    return sns.publish(params).promise()
            .then(confirmSnsPublication(data));
}


/**
 * Add a "didn't post" message to SNS
 */
function sendSkippedToSns(data) {
    var params = {
        Message: "Skipped automated blog tweet because there are not enough natural tweets since the last automated tweet",
        Subject: "Skipped automated blog tweet",
        TopicArn: snsTopic
    };
    return sns.publish(params).promise()
            .then(confirmSnsPublication(data));
}


/**
 * Validate that the SNS publish succeeded
 */
function confirmSnsPublication(tweet) {
    return function (data) {
        console.log("TWEET", tweet);
        console.log("DATA", data);
        return Promise.resolve(tweet);
    };
}


// What we expose from the module
exports.handler = (event, context, callback) => {

    function errorHandler(err) {
        console.error(err);
        callback("Error tweeting about blog");
    }

    function successMessage(data) {
        console.info(data);
        callback(null, "Tweeted about blog: " + data.slug);
    }

    getTemplates()
        .then(getHistory(null))
        .then(getTwitterCredentials)
        .then(checkRecentTweets)
        .then(data => {

            if (data.okayToPost) {
//                mockSendTweet(data)
                return sendTweet(data)
                        .then(updateHistory)
                        .then(sendToSns)
                        .then(successMessage);
            }

            return sendSkippedToSns(data);
        })
        .catch(errorHandler);
};
