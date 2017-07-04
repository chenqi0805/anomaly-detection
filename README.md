# Table of Contents
1. [Challenge Summary](README.md#challenge-summary)
2. [Details of Implementation](README.md#details-of-implementation)
3. [Anomalous Purchases](README.md#anomalous-purchases)
4. [Sample Data](README.md#sample-data)
5. [Writing Clean, Scalable, and Well-tested Code](README.md#writing-clean-scalable-and-well-tested-code)
6. [Repo Directory Structure](README.md#repo-directory-structure)
7. [Testing your Directory Structure and Output Format](README.md#testing-your-directory-structure-and-output-format)


# Challenge Summary

Imagine you're at an e-commerce company, Market-ter, that also has a social network. In addition to shopping, users can see which items their friends are buying, and are influenced by the purchases within their network. 

Your challenge is to build a real-time platform to analyze purchases within a social network of users, and detect any behavior that is far from the average within that social network.

### Example

A Product Manager at Market-ter approaches you with a new idea to encourage users to spend more money, without serving them pesky ads for items. The Product Manager shows you the following diagram and says: 

<img src="https://github.com/InsightDataScience/anomaly_detection/raw/master/images/social-network.png" width="150">

"If User A makes a large purchase, we should flag them to make sure User B and User C are influenced by it. We could highlight these large purchases in their "feed". We could also send an email to User D, recommending that they become friends with User A. They won't find these emails annoying because they share the mutual friend, User C.

But we can't send our users too many emails, so we should only do this with really high purchases that are considered "anomalies" - those that are 3 standard deviations above the average within their social network. These emails will ensure that our top spenders are the most connected and influential!"

Despite the excitement, you realize the Product Manager hasn't fully thought out two specific aspects of the problem:

1. Social networks change their purchasing behavior over time, so we shouldn't average over the full history of transactions. **How many transactions should we include in the average?**

2. Users only accept "nearby" recommendations. Recommendations might work for a "friend of a friend" (`Degree = 2`), but would a "friend of a friend of a friend" (`Degree = 3`) still work? **How many "degrees" should a social network include?**

Since the Product Manager doesn't know these factors yet, your platform must be flexible enough to easily adjust these parameters. Also, it will take in a lot of data, so it has to efficiently scale with the size of the input.

# Details of Implementation

## Parameters

For this challenge, you'll need two flexible parameters 

`D`: the number of degrees that defines a user's social network.

`T`: the number of consecutive purchases made by a user's social network (not including the user's own purchases)

A purchase amount is anomalous if it's more than 3 standard deviations from the mean of the last `T` purchases in the user's `D`th degree social network. As an expression, an anomalous amount is anything greater than `mean + (3 * sd)` where `sd` stands for standard deviation. 

### Number of degrees in social network (`D`)
 
`D` should not be hardcoded, and will be at least `1`.

A value of `1` means you should only consider the friends of the user. A value of `2` means the social network extends to friends and "friends of friends".

For example, if `D = 1`, User A's social network would only consist of User B and User C but not User D.

If `D = 2`, User A's social network would consist of User B, User C, and User D.

### Tracked number of purchases in the user's network (`T`)

`T` also shouldn't be hardcoded, and will be at least `2`.

The latest purchase is the one with the highest timestamp. If 2 purchases have the same timestamp, the one listed first would be considered the earlier one.

If a user's social network has less than 2 purchases, we don't have enough historical information, so no purchases should be considered anomalous at that point. 

If a user's social network has made 2 or more purchases, but less than `T`, we should still proceed with the calucations to determine if the purchases are anomalous.


### Input Data
The purchases and social network events have already been collected in two logs (in the log_input directory), which we can replay to mimic the data stream.

The first file, `batch_log.json`, contains past data that should be used to build the initial state of the entire user network, as well as the purchase history of the users.

Data in the second file, `stream_log.json`, should be used to determine whether a purchase is anomalous. If a purchase is flagged as anomalous, it should be logged in the `flagged_purchases.json` file. As events come in, both the social network and the purchase history of users should get updated.

The first line of `batch_log.json` contains a JSON object with the parameters:  degree (`D`) and number of tracked purchases (`T`) to consider for the calculation.

The rest of the events in both `batch_log.json` and `stream_log.json` fall into the following 3 categories:

 * `purchase` - includes a `timestamp`, user `id` and the `amount` paid. 
 * `befriend` - two users becoming friends (all friendships are considered bi-directional)
 * `unfriend` - two users ending their friendship
 
For example, the top of `batch_log.json` could be:

    {"D":"3", "T":"50"}
    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "16.83"}
    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "59.28"}
    {"event_type":"befriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "2"}
    {"event_type":"befriend", "timestamp":"2017-06-13 11:33:01", "id1": "3", "id2": "1"}
    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:01", "id": "1", "amount": "11.20"}
    {"event_type":"unfriend", "timestamp":"2017-06-13 11:33:01", "id1": "1", "id2": "3"}

While an event in `stream_log.json` could be:

    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:02", "id": "2", "amount": "1601.83"}

### Output Data
We write all the flagged purchases to a file, named `flagged_purchases.json`, with the extra fields of `mean` and `sd` (the order of both the events and the json fields should remain the same as in `stream_log.json`). Please report the values of `mean` and `sd` truncated to two decimal points (e.g. `3.46732` -> `3.46`). If there is no flagged event `flagged_purchases.json` should be empty, but present.

Flagged events are still valid, and can contribute to the baseline for the social network.

An example output in `flagged_purchases.json` could be:

    {"event_type":"purchase", "timestamp":"2017-06-13 11:33:02", "id": "2", "amount": "1601.83", mean": "29.10", "sd": "21.46"}

## Plan of attack

The dependencies are as follows:

1. sys: to read system inputs of data files
2. json: to read .json files
3. collections: python collections of data structures
4. heapq: heap data structure
5. math: math library

The following two libraries are used for the optional feature:

6. time
7. datetime

The following function is used in optional feature;

def time_convert(): convert the timestamp string into float in seconds.

The customers’ network can be built into a single class. Once we need to initialize a network by an initial batch of data, we just need to create an instance of the network
class. In the constructor of the class, we have

1. time_label: label the time order of purchasing events
2. connections: all current friends of a customer. We use a dictionary with customer_ids as keys and set of friends as values. As customer_ids should be unique, we could use a set to store friend_ids of a customer_id.
3. purchases: a dictionary with customer_ids as keys and corresponding purchase histories as values. For our purposes, we only need to keep up to T latest purchases of each customer_id. So we use a queue data structure in order to add new purchases and remove old purchases efficiently in O(1) time.
4. flagged_purchases: anomalous purchases in the whole network.
5, D: the number of degrees that defines a user's social network.
6. T: the number of consecutive purchases made by a user's social network (not including the user's own purchases).

The methods of the network class includes:

1. read: read a log without any anomaly detection. This method is used in initialization of the network.
2. read_with_anomaly: read a log with anomaly detection. This method is used in reading streams of logs. Once an anomaly is detected, it is added to the flagged_purchases.
3. befriend: two users become friends, i.e. appear in each other’s connections.
4. unfriend: two users cancel the connection, i.e. disappear in each other’s connections.
5. purchase: a customer_id makes a purchase with a particular amount. In order to keep track of the time order, purchases[customer_id] is a queue of tuples (time_label, amount), which facilitate the removal of the T+1th latest purchase when new logs come in. time label will be used in get mean and standard deviation of a user’s Dth social network.
6. getDfriends: a method to get a customer_id’s Dth degree network. This method returns the Dth degree connections in a set called connections. The implementation is by breadth-first search in the social network graph, which is a standard way of finding the shortest path between two nodes(users in the network).
7. getstatistic: a method to compute the mean and standard deviation of the last T purchases in customer_id’s Dth degree network. First we pull up customer_id’s Dth degree network. For each connection, we check his/her purchase histories and see if it can be added to the Tth latest purchases. Here we use a heap to store T latest purchases and then we compute the mean and the standard deviation of these purchases.
8. get_rank(optional): a method to get the spending ranking of all the customers in a certain time period. The input should be the ending time and the duration. In the implementation, we pull up the purchase histories of all the customers and add up the total spending of each customer in the time window and then output the ranked list of customer_id.

## Implementation

Data are read from json files into lists line by line. An instance of the network class is created and initialized by the batch data. Then for the main purpose, we implement read_stream method to record anomalous logs. Last, we write these logs into ‘flagged_purchases.json’.

## Optional Features

We can create a spending ranking among the customers in a certain time period in the network to send out discounts for vip users. This feature is meaningful when T is large enough to cover all the purchases in the period.

## Repo Directory Structure

The directory structure for your repo should look like this:

    ├── README.md 
    ├── run.sh
    ├── src
    │   └── process_log.py
    ├── log_input
    │   └── batch_log.json
    │   └── stream_log.json
    ├── log_output
    |   └── flagged_purchases.json
    ├── insight_testsuite
        └── run_tests.sh
        └── tests
            └── test_1
            |   ├── log_input
            |   │   └── batch_log.json
            |   │   └── stream_log.json
            |   |__ log_output
            |   │   └── flagged_purchases.json
            ├── your-own-test
                ├── log_input
                │   └── your-own-log.txt
                |__ log_output
                    └── flagged_purchases.json