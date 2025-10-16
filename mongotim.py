import pymongo
import json

# setup mongo client and db
client = pymongo.MongoClient()
db = client['homework']

# load json data
data = json.loads(open('example_tim_data.json').read())

tim_col = db.tim
# load data into mongo if we don't already have it
if not tim_col.estimated_document_count():
    tim_col.insert_many(data)
print('tim has %d records' % tim_col.estimated_document_count())
# ensure team col is empty
team_col = db.team_col
team_col.drop()


# Define mongodb aggregation pipeline
pipeline = [
    {
        "$group": {
            "_id": "$team_num",
            "average_balls_scored": { "$avg": "$num_balls" },
            "least_balls_scored": { "$min": "$num_balls" },
            "most_balls_scored": { "$max": "$num_balls" },
            "num_matches_played": { "$sum": 1 },
            "num_climbed_matches": {
                "$sum": {
                    "$cond": [ { "$eq": ["$climbed", True] }, 1, 0 ]
                }
            }
        }
    },
    {
        "$project": {
            "_id": 1, 
            "average_balls_scored": 1,
            "least_balls_scored": 1,
            "most_balls_scored": 1,
            "num_matches_played": 1,
            "climbed_fraction": {
                "$divide": ["$num_climbed_matches", "$num_matches_played"]
            }
        }
    },
    {
        # Add the $out stage as the final step, writing to "team_col" as string
        "$out": 'team_col'
    }
]

tim_col.aggregate(pipeline)
team_col = db.team_col
for td in team_col.find():
    print(td.pop('_id'), td)
