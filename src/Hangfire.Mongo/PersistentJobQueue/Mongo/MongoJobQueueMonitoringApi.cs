using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
    internal class MongoJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly HangfireDbContext _connection;

        public MongoJobQueueMonitoringApi(HangfireDbContext connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            _connection = connection;
        }

        public IEnumerable<string> GetQueues()
        {
            return _connection.Job.AsQueryable()
                .Where(_ => _.Queue != null)
                .GroupBy(_ => _.Queue)
                .Select(g => g.Key)
                .ToList();
        }

        public IEnumerable<ObjectId> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return _connection.Job
                .Find(Builders<JobDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobDto>.Filter.Eq(_ => _.FetchedAt, null) & Builders<JobDto>.Filter.Ne(_ => _.StateId, default(ObjectId)))
                .Skip(from)
                .Limit(perPage)
                .Project(_ => _.Id)
                .ToList();
        }

        public IEnumerable<ObjectId> GetFetchedJobIds(string queue, int from, int perPage)
        {
            return _connection.Job
                .Find(Builders<JobDto>.Filter.Eq(_ => _.Queue, queue) & Builders<JobDto>.Filter.Ne(_ => _.FetchedAt, null))
                .Skip(from)
                .Limit(perPage)
                .Project(_ => _.Id)
                .ToList();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            int enqueuedCount = (int)_connection.Job.Count(Builders<JobDto>.Filter.Eq(_ => _.Queue, queue) &
                                                Builders<JobDto>.Filter.Eq(_ => _.FetchedAt, null));

            int fetchedCount = (int)_connection.Job.Count(Builders<JobDto>.Filter.Eq(_ => _.Queue, queue) &
                                                Builders<JobDto>.Filter.Ne(_ => _.FetchedAt, null));

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = enqueuedCount,
                FetchedCount = fetchedCount
            };
        }

    }
}