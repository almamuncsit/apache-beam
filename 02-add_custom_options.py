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
  pass

  
