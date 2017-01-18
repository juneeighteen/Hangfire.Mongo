﻿using System;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using MongoDB.Driver;
using MongoDB.Bson;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Hangfire fetched job for Mongo database
    /// </summary>
    public sealed class MongoFetchedJob : IFetchedJob
    {
        private readonly HangfireDbContext _connection;

        private bool _disposed;

        private bool _removedFromQueue;

        private bool _requeued;

        /// <summary>
        /// Constructs fetched job by database connection, identifier, job ID and queue
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="jobId">Job ID</param>
        /// <param name="queue">Queue name</param>
        public MongoFetchedJob(HangfireDbContext connection, string jobId, string queue)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            _connection = connection;
            
            JobId = jobId;
            Queue = queue;
        }

        /// <summary>
        /// Job ID
        /// </summary>
        public string JobId { get; private set; }

        /// <summary>
        /// Queue name
        /// </summary>
        public string Queue { get; private set; }

        /// <summary>
        /// Removes fetched job from a queue
        /// </summary>
        public void RemoveFromQueue()
        {
            _connection.Job.UpdateOne(
                    Builders<JobDto>.Filter.Eq(_ => _.Id, new ObjectId(JobId)),
                    Builders<JobDto>.Update.Set(_ => _.Queue, null).Set(_ => _.FetchedAt, null)
            );

            _removedFromQueue = true;
        }

        /// <summary>
        /// Puts fetched job into a queue
        /// </summary>
        public void Requeue()
        {
            _connection.Job.UpdateOne(
                Builders<JobDto>.Filter.Eq(_ => _.Id, new ObjectId(JobId)),
                Builders<JobDto>.Update.Set(_ => _.FetchedAt, null)
            );

            _requeued = true;
        }

        /// <summary>
        /// Disposes the object
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }
    }
}