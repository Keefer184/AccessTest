using AbstractAccessTest;

ParqDir parqDir = new ParqDir(@"C:\Users\keefe\Documents");
DateTime start = new DateTime(2023, 1, 25);
DateTime end = new DateTime(2023, 2,1);
DateTime[] dateRange = new DateTime[] { start, end };
await parqDir.GetAllData(dateRange);
