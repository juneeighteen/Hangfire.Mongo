using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Storage;
using MongoDB.Driver;
using MongoDB.Bson;
using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
    internal class MongoJobQueue : IPersistentJobQueue
    {
        private readonly HangfireDbContext _context;
        private readonly MongoStorageOptions _options;

        public MongoJobQueue(HangfireDbContext connection, MongoStorageOptions options)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            _context = connection;
            _options = options;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

             
                var fetchedJob = _context.Job.FindOneAndUpdate(
                    Builders<JobDto>.Filter.In(_ => _.Queue, queues) &
                    (Builders<JobDto>.Filter.Eq(_ => _.FetchedAt, null) |
                     //avoid call to getserver time
                     Builders<JobDto>.Filter.Lt(_ => _.FetchedAt, _context.GetServerTimeUtc().Add(_options.InvisibilityTimeout.Negate()))),
                    Builders<JobDto>.Update.CurrentDate(_ => _.FetchedAt),
                    new FindOneAndUpdateOptions<JobDto> { ReturnDocument = ReturnDocument.After },
                    cancellationToken);
                
                if (fetchedJob != null)
                {
                    return new MongoFetchedJob(_context, fetchedJob.Id.ToString(), fetchedJob.Queue);
                }

                cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
            }

        }

        public void Enqueue(string queue, string jobId)
        {
            _context.Job.UpdateOne(
                Builders<JobDto>.Filter.Eq(_ => _.Id, new ObjectId(jobId)),
                Builders<JobDto>.Update.Set(_ => _.Queue, queue)
                                       .Set(_ => _.FetchedAt, null));
        }
    }
}