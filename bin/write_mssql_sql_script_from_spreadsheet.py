import os
import tempfile
import sys

from EtlUtils.EtlUtil import CsvImport, ExcelImport

if __name__ == '__main__':
    base_path = 'C:\\Users\\tomku\\Downloads\\20200511'
    csv_path = "{}\\{}".format(base_path, 'Customers.csv')
    xlsx_path = "{}\\{}".format(base_path, 'Customers.xlsx')


    sys.argv.append(csv_path)

    path = sys.argv[1]

    imp = CsvImport(filename=csv_path,
                        database_name='tempdb',
                        schema_name='',
                        table_name='')
    imp.write_output_file('{}\\{}'.format(base_path, 'out_csv.sql'))

    imp = ExcelImport(filename=xlsx_path,
                    database_name='tempdb',
                    schema_name='',
                    table_name='')

    imp.write_output_files('{}'.format(base_path))



