// You want to generate a report to show what each shop customer purchased in 2020. 
// You will group the individual order records by customer, 
// capturing each customer's first purchase date, 
// the number of orders they made, the total value of all their orders 
// and a list of their order items sorted by date.


// Select the database to use.
use('BOOK-AGGREGATIONS');

// purchases in 2020
var matchStage = {
    "$match": {
        "orderdate": {
            $gte: new Date('2020-01-01'), 
            $lte: new Date('2020-12-31')
        },
    }
};

// Sort by order date
var sortStage = {
    "$sort": {
        "orderdate": 1,
    }
};

// group by customer id
// capturing first purchase date
// number of orders
// total value of all their orders
// list of their order items sorted by date
var groupStage = {
    "$group": {
        _id: "$customer_id",
        first_purchase_date: { $min: "$orderdate"},
        total_orders: { $count: {} },
        total_value: { $sum: "$value" },
        orders: { $push: { "orderdate": "$orderdate", "value": "$value"} }
    }
};

// sort by each customer's first purchase date
var sortStage2 = {
    "$sort": {
        "first_purchase_date": 1,
    }
};

// set customer's id as their email
var setStage = {
    "$set": {
        "customer_id": "$_id"
    }
}

// omit id field
var unsetStage = {
    "$unset": ["_id"]
}

var pipeline = [
    matchStage,
    sortStage,
    groupStage,
    sortStage2,
    setStage,
    unsetStage
];

db.orders.aggregate(pipeline);

//db.orders.explain("executionStats").aggregate(pipeline);
