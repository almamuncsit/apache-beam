import apache_beam as beam
import pymongo

# Create Mongo Client and Select Database
client = pymongo.MongoClient("mongodb://localhost:27017/")
database = client["test_pipeline"]


# Load Projects From Database
def load_projects():
    projects = {}
    for x in database["projects"].find({}):
        if "hash" in x:
            projects[x['hash']] = x['slug']
    return projects
    
projects = load_projects()


# Filter Each Sites. Is it a valid request or not
class StoreIntoMongoDB(beam.DoFn):
    def process(self, element):
        element['slug'] = projects[element['hash']]
        database["sites"].insert_one(element)


# Filter Each Sites. Is it a valid request or not
class FilterSites(beam.DoFn):
    def process(self, element):
        if element['hash'] in projects:
            return [element]

