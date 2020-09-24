import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import  PipelineOptions


class ExtractTyoeAndPrice(beam.DoFn):
    def process(self, element):
        item = element.split(',')
        return [( item[2], float(item[3]) )]


with beam.Pipeline(options=PipelineOptions()) as p:
    
    transactions = ( p | 'Read Transactions CSV' >> beam.io.ReadFromText('files/sales.csv') )

    typePrices = ( transactions | 'Product type and prices' >> beam.ParDo(ExtractTyoeAndPrice()) )

    typeGroups = ( typePrices | "Group By Type" >> beam.GroupByKey() )

    typeGroupAvg = ( typeGroups | 'Calculate Avarage' >> beam.Map( lambda x: (x[0], len(x[1]),  sum(x[1]), sum( x[1])/len(x[1]) ) ))

    printItems = ( typeGroupAvg | beam.ParDo(lambda x: print(x)) )
    
    