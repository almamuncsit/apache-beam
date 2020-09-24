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

# Custom Class for ParDo
class SplitEachLine(beam.DoFn):
    def process(self, element):
        print(element)
        element_arr = element.split(" ")
        if len(element_arr) > 2:
            return [ element_arr ]


# Custom Class for ParDo
class ComputeWordLengthFn(beam.DoFn):
    def process(self, element):
        print(element)
        return [len(element)]


# Print the lenghts
class PrintLenghtFn(beam.DoFn):
    def process(self, element):
        print(element)
        return [element]


with beam.Pipeline(options=MyOptions()) as p:

    lines = p | 'ReadMyFile' >> beam.io.ReadFromText('files/text.txt')
    
    split_lines = lines | "Split Each Line" >> beam.ParDo(SplitEachLine())
    
    line_lengths = split_lines | "Get Word lenght" >> beam.ParDo(ComputeWordLengthFn())
    
    print_lengths = line_lengths | "Print Word lenght" >> beam.ParDo(PrintLenghtFn())

