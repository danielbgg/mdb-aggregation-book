// You want to query a collection of people 
// to find the three youngest people 
// who have a job in engineering, 
// sorted by the youngest person first.

// Select the database to use.
use('BOOK-AGGREGATIONS');

// Match engineers only
var matchStage = {
    "$match": {
        "vocation": "ENGINEER",
    }
};

// Sort by youngest person first
var sortStage = {
    "$sort": {
        "dateofbirth": -1,
    }
};

// Only include the first 3 youngest people
var limitStage = {
    "$limit": 3,
};

var unsetStage = {
    "$unset": [
        "_id",
        "vocation",
        "address"
    ]
};

var pipeline = [
    matchStage,
    sortStage,
    limitStage,
    unsetStage,
];

db.persons.aggregate(pipeline);

//db.persons.explain("executionStats").aggregate(pipeline);
