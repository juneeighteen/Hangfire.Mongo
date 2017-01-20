using System;
using System.Threading;

namespace Hangfire.Mongo.Sample.NETCore
{
    public class Program
    {
        private const int JobCount = 1000;
        private static DateTime startAt = DateTime.MinValue;

        public static void Main(string[] args)
        {
            JobStorage.Current = new MongoStorage("mongodb://localhost", "Mongo-Hangfire-Sample-NETCore", new MongoStorageOptions
            {
                QueuePollInterval = TimeSpan.FromSeconds(1) 
            });
            DateTime now = DateTime.UtcNow;
            startAt = now;
            using (new BackgroundJobServer(new BackgroundJobServerOptions { WorkerCount = 4 }))
            {
                for (var i = 0; i < JobCount; i++)
                {
                    var jobId = i;
                    now = DateTime.UtcNow;

                    BackgroundJob.Enqueue(() => Do(now, i));
                }

                Console.WriteLine($"{JobCount} job(s) has been enqued. They will be executed shortly!");
                Console.WriteLine($"");
                Console.WriteLine($"If you close this application before they are executed, ");
                Console.WriteLine($"they will be executed the next time you run this sample.");
                Console.WriteLine($"");
                Console.WriteLine($"Press any key to exit...");

                Console.ReadKey(true);
            }

            
        }

        private static int Processed = 0;
        public static void Do(DateTime launched, int count)
        {
            if (count == 0)
                startAt = DateTime.UtcNow;
            var done = Interlocked.Increment(ref Processed);
            // Console.WriteLine($"Fire-and-forget ({count}) time: {(DateTime.UtcNow - launched.ToUniversalTime()).TotalMilliseconds} ms");
            if (done == JobCount)
            {
                Console.WriteLine($"total done in: {(DateTime.UtcNow - startAt.ToUniversalTime()).TotalMilliseconds} ms");
            }
        }
    }
}
