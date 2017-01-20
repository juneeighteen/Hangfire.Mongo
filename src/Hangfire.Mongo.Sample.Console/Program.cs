using System;
using System.Threading;

namespace Hangfire.Mongo.Sample.NETCore
{
    public class Program
    {
        private const int JobCount = 1000;
        private static DateTime StartAt = DateTime.MinValue;
        private static int NbrRestart = 5;
        private static int Processed = 0;

        public static void Main(string[] args)
        {
            JobStorage.Current = new MongoStorage("mongodb://localhost", "Mongo-Hangfire-Sample-Console", new MongoStorageOptions
            {
                QueuePollInterval = TimeSpan.FromSeconds(1)
            });

            //JobStorage.Current = new SqlServer.SqlServerStorage(@"Server =.\sqlexpress; Database = Hangfire.Highlighter;Integrated Security=SSPI;", new SqlServer.SqlServerStorageOptions
            //{
            //    QueuePollInterval = TimeSpan.FromSeconds(1)
            //});

            var server = new BackgroundJobServer(new BackgroundJobServerOptions { WorkerCount = 6 });
            for (int i = 0; i < NbrRestart; i++)
            {
                Enqueue();
                Console.WriteLine($"Press any key to continue...");
                Console.ReadKey(true);
            }
            Console.WriteLine($"Press any key to exit...");
        }

        private static void Enqueue()
        {
            Processed = 0;
            DateTime now = DateTime.UtcNow;
            StartAt = now;
            for (var i = 0; i < JobCount; i++)
            {
                var jobId = i;
                now = DateTime.UtcNow;

                BackgroundJob.Enqueue(() => Do(now, i));
            }

            Console.WriteLine($"{JobCount} job(s) has been enqued. They will be executed shortly!");
        }

        public static void Do(DateTime launched, int count)
        {
            if (count == 0)
                StartAt = DateTime.UtcNow;
            int done = Interlocked.Increment(ref Processed);
            if (done == JobCount)
            {
                Console.WriteLine($"total done in: {(DateTime.UtcNow - StartAt.ToUniversalTime()).TotalMilliseconds} ms");
            }
        }
    }
}
