using System.Data;
using Microsoft.Data.Analysis;
using Parquet;


namespace AbstractAccessTest
{
    internal class ParqDir : IAccess
    {
        public string DirPath{ get; set; }
        public string[] AllFiles { get; set; } = new string[0];
        public int PrintCount { get; set; } = 1;
        public ParqDir() { }
        public ParqDir(string path) 
        {
            if (!Path.Exists(path))
            {
                throw new ArgumentException($"The following path you are trying to access does not exist: {path}");
            }
            else
            {
                DirPath = path;
                string[] files = Directory.GetFiles(path);
                List<string> tempFiles = new List<string>();
                for (int i = 0; i < files.Length; i++)
                {
                    if (files[i].EndsWith(".parquet"))
                    {
                        tempFiles.Add(files[i]);
                        //For testing
                        Console.WriteLine(files[i]);

                    }
                }
                AllFiles = tempFiles.ToArray();
                
            }
        }

        public string GetSeriesFromFileName(string fileName)
        {
            string[] fileInfo = fileName.Split('\\');
            string[] finalStrings = fileInfo.Last().Split('_');
            return finalStrings[0];
        }

        public string GetMonthFromFileName(string fileName)
        {
            int extensionIndex = fileName.IndexOf('.');
            if(extensionIndex != -1 && extensionIndex >= 2)
            {
                string lastTwoBeforeExtension = fileName.Substring(extensionIndex - 2, 2);
                return lastTwoBeforeExtension;
            }
            else
            {
                return "There is no month attached to this file" ;
            }
            //Saved in case need something similar with real files
            //string[] fileInfo = fileName.Split('\\');
            //string[] finalStrings = fileInfo.Last().Split('_');
            //string[] dateInfo = finalStrings.Last().Split('-');
            //string[] month = dateInfo[1].Split('.');
            //return month[0];
        }


        public async Task<Dictionary<string, DataFrame>> GetAllData(DateTime[] dateRange)
        {
            DateTime start = dateRange[0];
            DateTime end = dateRange[1];
            Dictionary<string, DataFrame> dataDict = new Dictionary<string, DataFrame>();
            foreach (string file in AllFiles)
            {
                string cabType = GetSeriesFromFileName(file);
                string month = GetMonthFromFileName(file);
                int monthCheck = int.Parse(month);

                if (monthCheck < start.Month || monthCheck > end.Month)
                {
                    continue;
                }
                else
                {
                    using (Stream currFile = File.OpenRead(file))
                    {
                        DataFrame df = new DataFrame();
                        if (dataDict.ContainsKey(cabType))
                        {
                            df = dataDict[cabType];
                        }
                        else
                        {
                            using (var parqFile = await ParquetReader.CreateAsync(currFile))
                            {
                                foreach (var field in parqFile.Schema.GetDataFields())
                                {
                                    DataFrameColumn headers = new StringDataFrameColumn(field.Name);
                                    df.Columns.Add(headers);

                                }
                            }
                        }

                        DataFrame unFiltered = await currFile.ReadParquetAsDataFrameAsync();
                        currFile.Close();
                        int dateTimeIndex = 1;
                        for (int i = 0; i < unFiltered.Columns.Count; i++)
                        {
                            if (unFiltered.Columns[i].DataType == typeof(DateTime) || unFiltered.Columns[i].DataType == typeof(DateTime?))
                            {
                                dateTimeIndex = i;
                                break;
                            }
                        }
                        var filteredRows = unFiltered.Rows.Where(x => (DateTime)x[dateTimeIndex] >= start && (DateTime)x[dateTimeIndex] <= end).ToList();
                        foreach (var row in filteredRows)
                        {
                            df.Append(row, true);
                        }

                        if (dataDict.ContainsKey(cabType))
                        {
                            dataDict[cabType] = df;
                        }
                        else
                        {
                            dataDict.Add(cabType, df);
                        }
                        
                    }
                }
            }
            foreach(KeyValuePair<string,DataFrame> kvp in dataDict)
            {
                Console.WriteLine($"Cab Type: {kvp.Key}");
                PrintDataFrameData(kvp.Value);
            }
            return dataDict;
        }

        public async void PrintDataFrameData(DataFrame df)
        {
            DateTime time = DateTime.Now;
            string timestamp = time.Year.ToString()+time.Month.ToString()+time.Day.ToString()+time.Hour.ToString()+time.Minute.ToString();
            string outputPath = $"[YOUR FILE PATHS]{timestamp}{PrintCount}.csv";
            int parallelTasks = Environment.ProcessorCount;
            PrintCount++;
            using(StreamWriter writer = new StreamWriter(outputPath))
            {
                await writer.WriteLineAsync(string.Join(",",df.Columns.Cast<DataFrameColumn>().Select(column=>column.Name)));
                var chunkedData = ChunkData(df.ToTable().AsEnumerable(), parallelTasks);
                var tasks = chunkedData.Select(chunk =>
                {
                    return Task.Run(async () =>
                    {
                        foreach (var row in chunk)
                        {
                            //Gettin an excpetion here that I am still working on...
                            await writer.WriteLineAsync(string.Join(",", row.ItemArray));
                        }
                    });
                });

                await Task.WhenAll(tasks);
            }
            Console.WriteLine("DataFrame has been printed to" + outputPath);
        }

        static IEnumerable<IEnumerable<T>> ChunkData<T>(IEnumerable<T> source, int chunkSize)
        {
            var chunk = new List<T>(chunkSize);
            foreach(var item in source)
            {
                chunk.Add(item);
                if(chunk.Count == chunkSize)
                {
                    yield return chunk;
                    chunk = new List<T>(chunkSize);
                }
            }
            if (chunk.Count > 0)
            {
                yield return chunk;
            }
        }

        Dictionary<object, object> IAccess.ChannelMetaData()
        {
            throw new NotImplementedException();
        }

        List<object> IAccess.ChannelNamesInSeries()
        {
            throw new NotImplementedException();
        }


        DataFrame IAccess.GetChannelData()
        {
            throw new NotImplementedException();
        }

        DataFrame IAccess.GetSeriesData()
        {
            throw new NotImplementedException();
        }

        Dictionary<object, object> IAccess.Metadata()
        {
            throw new NotImplementedException();
        }

        List<object> IAccess.SeriesNames()
        {
            throw new NotImplementedException();
        }
    }
}
