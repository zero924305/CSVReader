using CsvHelper;
using CsvHelper.Configuration.Attributes;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace CSVReader
{
    internal class Program
    {
        public static string GetDataRecord(List<object> lccs)
        {
            List<string> queryData = new();

            foreach (System.Dynamic.ExpandoObject ex in lccs)
            {
                List<string> list = ex.Select(x => x.Value.ToString().Trim()).ToList();
                list = list.Select(x =>
                       x.Replace("\\'", "'")
                        .Replace("’", "'")
                        .Replace("\'", "'")
                        .Replace("''", "'")
                        .Replace("`", "'")
                        .Replace("'", "''")
                        .Replace("\"\"", "''")).ToList();

                var concatenatedValues = String.Join(',', list.Select(x => "'" + x + "'"));

                queryData.Add("(" + concatenatedValues + ")");
            }

            return string.Join(",\n", queryData); ;
        }

        public static string[] GetHeader(string reader)
        {
            string[] header;
            using (StreamReader streamReader = new(reader, Encoding.UTF8))
            {
                using var csvReader = new CsvReader(streamReader, CultureInfo.InvariantCulture);
                csvReader.Read();
                csvReader.ReadHeader();
                header = csvReader.HeaderRecord;
            }
            return header;
        }

        public static string GetSqlHeader(string[] header)
        {
            string queryHeader = "";
            queryHeader += "DROP TABLE IF EXISTS #temp \n";
            queryHeader += "CREATE TABLE #temp \n";
            queryHeader += "(\n [tempuniqueID] [int] IDENTITY(1,1) NOT NULL,\n";

            foreach (string x in header)
            {
                queryHeader += " [" + x + "] Varchar(MAX),\n";
            }
            queryHeader = queryHeader.Remove(queryHeader.Length - 2) + "\n)";

            return queryHeader;
        }

        public static dynamic GetsqlRecord(string reader)
        {
            using (StreamReader streamReader = new(reader, Encoding.UTF8))
            {
                using (var csvReader = new CsvReader(streamReader, CultureInfo.InvariantCulture))
                {
                    var rows = csvReader.GetRecords<dynamic>().ToList();
                    return rows;
                }
            }
        }

        private static void Main(string[] args)
        {
            var csv = @"csvlocation";

            //Get Csv Header
            string[] header = GetHeader(csv);
            string queryHeader = GetSqlHeader(header);

            //Get Csv Data
            List<object> sqlRow = GetsqlRecord(csv);
            string sqldata = GetDataRecord(sqlRow);

            //Output result
            Console.WriteLine(queryHeader);
            Console.WriteLine(sqldata);
            
        }
    }
}
