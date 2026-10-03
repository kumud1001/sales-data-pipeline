import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class CalculateRevenue(beam.DoFn):

    def process(self, row):
        quantity = int(row["quantity"])
        price = float(row["price"])

        row["revenue"] = quantity * price

        yield row


def run():

    options = PipelineOptions(
        runner="DirectRunner"
    )

    with beam.Pipeline(options=options) as pipeline:

        (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(
                "data/sales.csv",
                skip_header_lines=1
            )

            | "Convert CSV to Dictionary" >> beam.Map(
                lambda line: {
                    "order_id": line.split(",")[0],
                    "customer": line.split(",")[1],
                    "product": line.split(",")[2],
                    "quantity": line.split(",")[3],
                    "price": line.split(",")[4]
                }
            )

            | "Calculate Revenue" >> beam.ParDo(
                CalculateRevenue()
            )

            | "Print Results" >> beam.Map(print)
        )


if __name__ == "__main__":
    run()