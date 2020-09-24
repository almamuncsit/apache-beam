import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--input',
        help='Input for the pipeline',
        default='text.txt')
    parser.add_argument(
        '--output',
        help='Output for the pipeline',
        default='count')


with beam.Pipeline(options=MyOptions()) as p:
    # Create a PCollection (lines) from a txt file
    lines = p | 'ReadMyFile' >> beam.io.ReadFromText('text.txt')
  
    # Creating PCollection From In-Memory List
    messages = (
        p
        | beam.Create([
            'To be, or not to be: that is the question: ',
            "Whether 'tis nobler in the mind to suffer ",
            'The slings and arrows of outrageous fortune, ',
            'Or to take arms against a sea of troubles, ',
        ]))


