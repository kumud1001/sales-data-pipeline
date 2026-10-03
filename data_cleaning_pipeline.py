import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class CleanAndCalculate(beam.DoFn):

    def process(self, line):

        try:
            order_id, customer, product, quantity, price = line.split(",")

            quantity = int(quantity)
            price = float(price)

            # Validate data
            if quantity <= 0:
                return

            if price < 0:
                return

            revenue = quantity * price

            yield {
                "order_id": order_id.strip(),
                "customer": customer.strip(),
                "product": product.strip(),
                "quantity": quantity,
                "price": price,
                "revenue": revenue
            }

        except ValueError:
            # Ignore invalid records
            return


def format_output(row):

    return ",".join([
        row["order_id"],
        row["customer"],
        row["product"],
        str(row["quantity"]),
        str(row["price"]),
        str(row["revenue"])
    ])


def run():

    options = PipelineOptions(
        runner="DirectRunner"
    )

    with beam.Pipeline(options=options) as pipeline:

        (
            pipeline
            | "Read Sales CSV" >> beam.io.ReadFromText(
                "data/sales.csv",
                skip_header_lines=1
            )

            | "Clean and Calculate" >> beam.ParDo(
                CleanAndCalculate()
            )

            | "Format Output" >> beam.Map(
                format_output
            )

            | "Write Processed CSV" >> beam.io.WriteToText(
                "data/processed_sales",
                file_name_suffix=".csv",
                header="order_id,customer,product,quantity,price,revenue"
            )
        )


if __name__ == "__main__":
    run()