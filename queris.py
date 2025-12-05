#!/usr/bin/env python3
from pymongo import MongoClient
import json
import sys
from bson import json_util

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['marathon_db']
runners = db['runners']
sponsors = db['sponsors']
refreshments = db['refreshments']


def print_results(title, results):
    """Pretty print query results"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

    if not results:
        print("No results found.")
        return

    print(json.dumps(json.loads(json_util.dumps(results)), indent=2))
    print(f"\nTotal Results: {len(results)}")
    print("="*80 + "\n")


# ---------------------------------------------------------
# 1. List all runners who completed the Full Marathon
# ---------------------------------------------------------
def query_1():
    result = list(runners.find(
        {"category": "Full Marathon", "finished": True},
        {"_id": 0}
    ))
    print_results("Q1: Full Marathon Completers", result)


# ---------------------------------------------------------
# 2. Find the fastest runner in each category
# ---------------------------------------------------------
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
    result = list(runners.aggregate(pipeline))
    print_results("Q2: Fastest Runner in Each Category", result)


# ---------------------------------------------------------
# 3. Show runners who did not finish the race
# ---------------------------------------------------------
def query_3():
    result = list(runners.find(
        {"finished": False},
        {"_id": 0}
    ))
    print_results("Q3: Runners Who Did Not Finish", result)


# ---------------------------------------------------------
# 4. Calculate the average completion time per category
# ---------------------------------------------------------
def query_4():
    pipeline = [
        {"$match": {"finished": True}},
        {"$group": {
            "_id": "$category",
            "average_time_minutes": {"$avg": "$completion_time"},
            "total_finishers": {"$sum": 1}
        }}
    ]
    result = list(runners.aggregate(pipeline))
    print_results("Q4: Average Completion Time Per Category", result)


# ---------------------------------------------------------
# 5. Identify runners who received both medals and certificates
# ---------------------------------------------------------
def query_5():
    result = list(runners.find(
        {"medal": True, "certificate": True},
        {"_id": 0}
    ))
    print_results("Q5: Runners With Both Medals and Certificates", result)


# ---------------------------------------------------------
# 6. List sponsors who supported more than one category
# ---------------------------------------------------------
def query_6():
    result = list(sponsors.find(
        {"$expr": {"$gt": [{"$size": "$categories"}, 1]}},
        {"_id": 0}
    ))
    print_results("Q6: Sponsors Supporting Multiple Categories", result)


# ---------------------------------------------------------
# 7. Retrieve runners who participated in multiple categories
# ---------------------------------------------------------
def query_7():
    pipeline = [
        {"$group": {
            "_id": "$name",
            "categories": {"$addToSet": "$category"},
            "bib_numbers": {"$addToSet": "$bib_number"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]
    result = list(runners.aggregate(pipeline))
    print_results("Q7: Runners in Multiple Categories", result)


# ---------------------------------------------------------
# 8. Find the top 3 finishers in the Half Marathon
# ---------------------------------------------------------
def query_8():
    result = list(
        runners.find(
            {"category": "Half Marathon", "finished": True},
            {"_id": 0}
        )
        .sort("completion_time", 1)
        .limit(3)
    )
    print_results("Q8: Top 3 Half Marathon Finishers", result)


# ---------------------------------------------------------
# 9. Show refreshment stalls visited by more than 50 runners
# ---------------------------------------------------------
def query_9():
    result = list(refreshments.find(
        {"visitors": {"$gt": 50}},
        {"_id": 0}
    ))
    print_results("Q9: Popular Refreshment Stalls (50+ Visitors)", result)


# ---------------------------------------------------------
# 10. Identify categories where more than 200 runners participated
# ---------------------------------------------------------
def query_10():
    pipeline = [
        {"$group": {
            "_id": "$category",
            "total_participants": {"$sum": 1}
        }},
        {"$match": {"total_participants": {"$gt": 200}}}
    ]
    result = list(runners.aggregate(pipeline))
    print_results("Q10: Categories with More Than 200 Runners", result)


# ---------------------------------------------------------
# 11. Find runners whose times were better than the average in their category
# ---------------------------------------------------------
def query_11():
    pipeline = [
        {"$match": {"finished": True}},
        {"$group": {
            "_id": "$category",
            "avg_time": {"$avg": "$completion_time"},
            "runners": {
                "$push": {
                    "name": "$name",
                    "time": "$completion_time",
                    "bib": "$bib_number"
                }
            }
        }},
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
    result = list(runners.aggregate(pipeline))
    print_results("Q11: Runners Better Than Category Average", result)


# ---------------------------------------------------------
# 12. Retrieve cities with the most marathon participants
# ---------------------------------------------------------
def query_12():
    pipeline = [
        {"$group": {
            "_id": "$city",
            "total_participants": {"$sum": 1}
        }},
        {"$sort": {"total_participants": -1}}
    ]
    result = list(runners.aggregate(pipeline))
    print_results("Q12: Cities With Most Marathon Participants", result)


# ---------------------------------------------------------
# MENU
# ---------------------------------------------------------
def list_all_queries():
    print("\n" + "=" * 80)
    print("  AVAILABLE QUERIES")
    print("=" * 80)

    queries = [
        "1. List all runners who completed the Full Marathon",
        "2. Find the fastest runner in each category",
        "3. Show runners who did not finish the race",
        "4. Calculate the average completion time per category",
        "5. Identify runners who received both medals and certificates",
        "6. List sponsors who supported more than one category",
        "7. Retrieve runners who participated in multiple categories",
        "8. Find the top 3 finishers in the Half Marathon",
        "9. Show refreshment stalls visited by more than 50 runners",
        "10. Identify categories where more than 200 runners participated",
        "11. Find runners whose times were better than the average in their category",
        "12. Retrieve cities with the most marathon participants",
        "",
        "OPERATIONS:",
        "insert - Add new runner",
        "update - Update runner time",
        "delete - Delete runner",
        "list   - Show this menu"
    ]

    for q in queries:
        print(" ", q)

    print("=" * 80 + "\n")


def main():
    if len(sys.argv) < 2:
        print("\nUsage: python queries.py <query_number|operation>")
        list_all_queries()
        return

    command = sys.argv[1].lower()

    queries = {
        '1': query_1, '2': query_2, '3': query_3,
        '4': query_4, '5': query_5, '6': query_6,
        '7': query_7, '8': query_8, '9': query_9,
        '10': query_10, '11': query_11, '12': query_12
    }

    if command in queries:
        queries[command]()
    else:
        print("\n✗ Invalid command")
        list_all_queries()


if __name__ == "__main__":
    main()