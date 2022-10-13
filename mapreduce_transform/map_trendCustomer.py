#!python3

from mrjob.job import MRJob
from mrjob.step import MRStep
from mr3px.csvprotocol import CsvProtocol

import csv
import json


cols = 'id_transaction,id_customer,name_customer,date_transaction,product_kategory,product_transaction,amount_transaction'.split(',')


def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class OrderDateTotalCustomers(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, csv_readline(line)))

        if row['id_customer'] != 'id_customer':
            # Yield the date_transaction
            yield row['date_transaction'][0:7], int(row['id_customer'])
            #yield key,value

    def reducer(self, key, values):
        set_k = set(values)
        val_list = list(set_k)
        total = 0
        for row in val_list:
            total = total +1
        yield None, (key, total)

if __name__ == '__main__':
    OrderDateTotalCustomers.run()