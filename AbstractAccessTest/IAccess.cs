//Important!!
using Microsoft.Data.Analysis;


namespace AbstractAccessTest
{
    internal interface IAccess
    {

        //Need to add comments and DataTypes
        List<object> SeriesNames();

        List<object> ChannelNamesInSeries();

        Dictionary<object, object> Metadata();

        Dictionary<object, object> ChannelMetaData();

        Task<Dictionary<string,DataFrame>> GetAllData(DateTime[] dateRange);

        DataFrame GetSeriesData();

        DataFrame GetChannelData();
    }
}
