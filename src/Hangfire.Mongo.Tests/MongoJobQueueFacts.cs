﻿using System;
using System.Linq;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.PersistentJobQueue.Mongo;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoJobQueueFacts
    {
        private static readonly string[] DefaultQueues = { "default" };

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoJobQueue(null, new MongoStorageOptions()));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                   () => new MongoJobQueue(connection, null));

                Assert.Equal("options", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentNullException>(
                    () => queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentException>(
                    () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            // Arrange
            UseConnection(connection =>
            {
                var jobQueue = new JobDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Queue = "default"
                };

                connection.Job.InsertOne(jobQueue);

                var id = jobQueue.Id.ToString();
                var queue = CreateJobQueue(connection);

                // Act
                MongoFetchedJob payload = (MongoFetchedJob)queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal(id, payload.JobId);
                Assert.Equal("default", payload.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            // Arrange
            UseConnection(connection =>
            {
                var job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc(),
                    Queue = "default"
                };
                connection.Job.InsertOne(job);

                var queue = CreateJobQueue(connection);
                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotNull(payload);

                var fetchedAt = connection.Job.Find(Builders<JobDto>.Filter.Eq(_ => _.Id, new ObjectId(payload.JobId))).FirstOrDefault().FetchedAt;

                Assert.NotNull(fetchedAt);
                Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            // Arrange
            UseConnection(connection =>
            {
                var job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc(),
                    Queue = "default",
                    FetchedAt = connection.GetServerTimeUtc().AddDays(-1)
                };
                connection.Job.InsertOne(job);

                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            // Arrange
            UseConnection(connection =>
            {
                var job1 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc(),
                    Queue = "default"
                };
                connection.Job.InsertOne(job1);

                var job2 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc(),
                    Queue = "default"
                };
                connection.Job.InsertOne(job2);

                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                var otherJobFetchedAt = connection.Job.Find(Builders<JobDto>.Filter.Ne(_ => _.Id, new ObjectId(payload.JobId))).FirstOrDefault().FetchedAt;

                Assert.Null(otherJobFetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            UseConnection(connection =>
            {
                var job1 = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc(),
                    Queue = "critical"
                };
                connection.Job.InsertOne(job1);

                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueuesBasedOnQueuePriority()
        {
            UseConnection(connection =>
            {
                var criticalJob = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc(),
                    Queue = "critical"
                };
                connection.Job.InsertOne(criticalJob);

                var defaultJob = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = connection.GetServerTimeUtc(),
                    Queue = "default"
                };
                connection.Job.InsertOne(defaultJob);


                var queue = CreateJobQueue(connection);

                var critical = (MongoFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(critical.JobId);
                Assert.Equal("critical", critical.Queue);

                var @default = (MongoFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(@default.JobId);
                Assert.Equal("default", @default.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);
                var id = ObjectId.GenerateNewId().ToString();

                connection.Job.InsertOne(new JobDto
                {
                    Id = new ObjectId(id)
                });
               queue.Enqueue("default", id);

                var record = connection.Job.Find(new BsonDocument()).ToList().Single();
                Assert.Equal(id, record.Id.ToString());
                Assert.Equal("default", record.Queue);
                Assert.Null(record.FetchedAt);
            });
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        private static MongoJobQueue CreateJobQueue(HangfireDbContext connection)
        {
            return new MongoJobQueue(connection, new MongoStorageOptions());
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
#pragma warning restore 1591
}