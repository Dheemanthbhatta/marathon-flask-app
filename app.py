from flask import Flask, render_template, request, jsonify
from pymongo import MongoClient
from bson import json_util
import json

app = Flask(__name__)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['marathon_db']

# Collections
runners = db['runners']
sponsors = db['sponsors']
refreshments = db['refreshments']


# ---------------------------------------------------------
# INITIAL SAMPLE DATA (AUTO INSERT)
# ---------------------------------------------------------

def init_data():
    if runners.count_documents({}) == 0:

        sample_runners = [
            {"bib_number": "NU25MCA11", "name": "Amit Kumar", "category": "Full Marathon", "city": "Mumbai",
             "start_time": "06:00", "completion_time": 235, "finished": True, "medal": True, "certificate": True,
             "checkpoint_times": [60, 120, 180, 235]},

            {"bib_number": "NU25MCA12", "name": "Priya Singh", "category": "Half Marathon", "city": "Delhi",
             "start_time": "07:00", "completion_time": 110, "finished": True, "medal": True, "certificate": True,
             "checkpoint_times": [30, 60, 90, 110]},

            {"bib_number": "NU25MCA13", "name": "Rahul Sharma", "category": "10K", "city": "Bangalore",
             "start_time": "08:00", "completion_time": 45, "finished": True, "medal": True, "certificate": False,
             "checkpoint_times": [15, 30, 45]},

            {"bib_number": "NU25MCA14", "name": "Sneha Patel", "category": "5K", "city": "Pune",
             "start_time": "08:30", "completion_time": 28, "finished": True, "medal": True, "certificate": False,
             "checkpoint_times": [10, 20, 28]},

            {"bib_number": "NU25MCA15", "name": "Vikram Reddy", "category": "Full Marathon",
             "city": "Hyderabad", "start_time": "06:00", "completion_time": 250,
             "finished": True, "medal": True, "certificate": True,
             "checkpoint_times": [65, 130, 195, 250]},

            {"bib_number": "NU25MCA16", "name": "Anjali Gupta", "category": "Half Marathon", "city": "Mumbai",
             "start_time": "07:00", "completion_time": 0,
             "finished": False, "medal": False, "certificate": False,
             "checkpoint_times": [35, 70]},

            {"bib_number": "NU25MCA17", "name": "Suresh Nair", "category": "10K", "city": "Chennai",
             "start_time": "08:00", "completion_time": 42,
             "finished": True, "medal": True, "certificate": False,
             "checkpoint_times": [14, 28, 42]},

            {"bib_number": "NU25MCA18", "name": "Deepa Joshi", "category": "Full Marathon", "city": "Delhi",
             "start_time": "06:00", "completion_time": 240,
             "finished": True, "medal": True, "certificate": True,
             "checkpoint_times": [62, 125, 188, 240]},

            {"bib_number": "NU25MCA19", "name": "Karthik Iyer", "category": "Half Marathon",
             "city": "Bangalore", "start_time": "07:00", "completion_time": 105,
             "finished": True, "medal": True, "certificate": True,
             "checkpoint_times": [28, 58, 88, 105]},

            {"bib_number": "NU25MCA20", "name": "Meera Das", "category": "5K", "city": "Kolkata",
             "start_time": "08:30", "completion_time": 30,
             "finished": True, "medal": True, "certificate": False,
             "checkpoint_times": [11, 21, 30]},
        ]

        # Multi category participants
        sample_runners.extend([
            {"bib_number": "NU25MCA26", "name": "Amit Kumar", "category": "Half Marathon",
             "city": "Mumbai", "start_time": "07:00", "completion_time": 108,
             "finished": True, "medal": True, "certificate": True,
             "checkpoint_times": [29, 59, 89, 108]},

            {"bib_number": "NU25MCA27", "name": "Priya Singh", "category": "10K",
             "city": "Delhi", "start_time": "08:00", "completion_time": 44,
             "finished": True, "medal": True, "certificate": False,
             "checkpoint_times": [15, 30, 44]}
        ])

        runners.insert_many(sample_runners)

        sample_sponsors = [
            {"name": "Nike", "categories": ["Full Marathon", "Half Marathon"], "amount": 500000},
            {"name": "Adidas", "categories": ["10K", "5K"], "amount": 300000},
            {"name": "Puma", "categories": ["Full Marathon", "Half Marathon", "10K"], "amount": 450000},
            {"name": "Reebok", "categories": ["5K"], "amount": 150000},
        ]

        sponsors.insert_many(sample_sponsors)

        sample_refreshments = [
            {"stall_name": "Water Station A", "location": "Km 5", "visitors": 75},
            {"stall_name": "Energy Drink Hub", "location": "Km 10", "visitors": 120},
            {"stall_name": "Fruit Corner", "location": "Km 15", "visitors": 45},
            {"stall_name": "Electrolyte Zone", "location": "Km 20", "visitors": 90},
            {"stall_name": "Final Refresh", "location": "Finish Line", "visitors": 180},
        ]

        refreshments.insert_many(sample_refreshments)


init_data()

# ---------------------------------------------------------
# ✅ QUERIES (1 to 12 - Corrected order)
# ---------------------------------------------------------

def query_1():
    return list(runners.find(
        {"category": "Full Marathon", "finished": True},
        {"_id": 0}
    ))


def query_2():
    pipeline = [
        {"$match": {"finished": True}},
        {"$sort": {"completion_time": 1}},
        {"$group": {
            "_id": "$category",
            "fastest_runner": {"$first": "$name"},
            "bib_number": {"$first": "$bib_number"},
            "completion_time": {"$first": "$completion_time"}
        }}
    ]
    return list(runners.aggregate(pipeline))


def query_3():
    return list(runners.find({"finished": False}, {"_id": 0}))


def query_4():
    pipeline = [
        {"$match": {"finished": True}},
        {"$group": {
            "_id": "$category",
            "average_time_minutes": {"$avg": "$completion_time"},
            "total_finishers": {"$sum": 1}
        }}
    ]
    return list(runners.aggregate(pipeline))


def query_5():
    return list(runners.find(
        {"medal": True, "certificate": True},
        {"_id": 0}
    ))


def query_6():
    return list(sponsors.find(
        {"$expr": {"$gt": [{"$size": "$categories"}, 1]}},
        {"_id": 0}
    ))


def query_7():
    pipeline = [
        {"$group": {
            "_id": "$name",
            "categories": {"$addToSet": "$category"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]
    return list(runners.aggregate(pipeline))


def query_8():
    return list(runners.find(
        {"category": "Half Marathon", "finished": True},
        {"_id": 0}
    ).sort("completion_time", 1).limit(3))


def query_9():
    return list(refreshments.find(
        {"visitors": {"$gt": 50}},
        {"_id": 0}
    ))


def query_10():
    pipeline = [
        {"$group": {
            "_id": "$category",
            "total_participants": {"$sum": 1}
        }},
        {"$match": {"total_participants": {"$gt": 200}}}
    ]
    return list(runners.aggregate(pipeline))


def query_11():
    pipeline = [
        {"$match": {"finished": True}},
        {"$group": {
            "_id": "$category",
            "avg_time": {"$avg": "$completion_time"},
            "runners": {"$push": {"name": "$name", "time": "$completion_time", "bib": "$bib_number"}}}
         },
        {"$unwind": "$runners"},
        {"$match": {"$expr": {"$lt": ["$runners.time", "$avg_time"]}}},
        {"$project": {
            "_id": 0,
            "category": "$_id",
            "name": "$runners.name",
            "bib_number": "$runners.bib",
            "completion_time": "$runners.time",
            "category_average": "$avg_time"
        }}
    ]
    return list(runners.aggregate(pipeline))


def query_12():
    pipeline = [
        {"$group": {"_id": "$city", "total_participants": {"$sum": 1}}},
        {"$sort": {"total_participants": -1}}
    ]
    return list(runners.aggregate(pipeline))


# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/query/<int:query_num>')
def execute_query(query_num):

    queries = {
        1: query_1, 2: query_2, 3: query_3,
        4: query_4, 5: query_5, 6: query_6,
        7: query_7, 8: query_8, 9: query_9,
        10: query_10, 11: query_11, 12: query_12
    }

    if query_num in queries:
        result = queries[query_num]()
        return jsonify(json.loads(json_util.dumps(result)))

    return jsonify({"error": "Invalid Query Number"}), 400


@app.route('/all_runners')
def all_runners():
    return jsonify(json.loads(json_util.dumps(list(runners.find({}, {"_id": 0})))))


@app.route('/insert_runner', methods=['POST'])
def insert_runner():
    data = request.json
    runners.insert_one(data)
    return jsonify({"message": "Runner inserted successfully!"})


@app.route('/update_runner', methods=['POST'])
def update_runner():
    data = request.json
    bib = data.pop('bib_number')
    runners.update_one({"bib_number": bib}, {"$set": data})
    return jsonify({"message": "Runner updated successfully!"})


@app.route('/delete_runner', methods=['POST'])
def delete_runner():
    bib = request.json.get('bib_number')
    runners.delete_one({"bib_number": bib})
    return jsonify({"message": "Runner deleted successfully!"})


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    print("✅ Marathon Event System Running...")
    print("🌐 Access: http://localhost:5000")
    print("📦 Database: marathon_db")
    app.run(debug=True)