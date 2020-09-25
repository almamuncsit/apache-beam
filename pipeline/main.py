import apache_beam as beam
import json
from mongo_helper import FilterSites, StoreIntoMongoDB
from apache_beam.options.pipeline_options import PipelineOptions    


# Convert JSON String to Python Dictionary 
class ConverJsonToObject(beam.DoFn):
    def process(self, element):
        return [ json.loads(element) ]


with beam.Pipeline(options=PipelineOptions()) as p:
    # Create a PCollection (sites) from json files of input directory
    sites = p | 'ReadMyFile' >> beam.io.ReadFromText('input/')

    json_sites = ( sites | "JSON to Object" >> beam.ParDo( ConverJsonToObject() ))

    filterd_sites = ( json_sites | "Filter Sites" >> beam.ParDo( FilterSites() ))

    # Inset Sites Into MongoDB
    ( filterd_sites | "Insert Into MongoDB" >> beam.ParDo( StoreIntoMongoDB() ))

    # Write to a Single JSON file
    objectToJson = (filterd_sites | 'Object to Json' >> beam.Map(lambda x: json.dumps(x)))
    (objectToJson | 'Write' >> beam.io.WriteToText("output/filterd_data", file_name_suffix=".json"))

    # Print Data
    prints = ( filterd_sites | "Print" >> beam.ParDo( lambda x: print(x['hash']) ) )

