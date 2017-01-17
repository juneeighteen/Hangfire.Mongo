using System;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoFetchedJobFacts
    {
        private const string JobId = "id";
        private const string Queue = "queue";


        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoFetchedJob(null, ObjectId.GenerateNewId(), JobId, Queue));

                Assert.Equal("connection", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() => new MongoFetchedJob(connection, ObjectId.GenerateNewId(), null, Queue));

                Assert.Equal("jobId", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoFetchedJob(connection, ObjectId.GenerateNewId(), JobId, null));

                Assert.Equal("queue", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            UseConnection(connection =>
            {
                var id = ObjectId.GenerateNewId();
                var fetchedJob = new MongoFetchedJob(connection, id, JobId, Queue);

                Assert.Equal(id, fetchedJob.Id);
                Assert.Equal(JobId, fetchedJob.JobId);
                Assert.Equal(Queue, fetchedJob.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, ObjectId.GenerateNewId().ToString(), "default");
                var processingJob = new MongoFetchedJob(connection, id, ObjectId.GenerateNewId().ToString(), "default");

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = connection.JobQueue.Count(new BsonDocument());
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            UseConnection(connection =>
            {
                var job1 = ObjectId.GenerateNewId().ToString();
                var job2 = ObjectId.GenerateNewId().ToString();
                // Arrange
                CreateJobQueueRecord(connection, job1, "default");
                CreateJobQueueRecord(connection, job1, "critical");
                CreateJobQueueRecord(connection, job2, "default");

                var fetchedJob = new MongoFetchedJob(connection, ObjectId.GenerateNewId(), ObjectId.GenerateNewId().ToString(), "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = connection.JobQueue.Count(new BsonDocument());
                Assert.Equal(3, count);
            });
        }

        [Fact, CleanDatabase]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, ObjectId.GenerateNewId().ToString(), "default");
                var processingJob = new MongoFetchedJob(connection, id, ObjectId.GenerateNewId().ToString(), "default");

                // Act
                processingJob.Requeue();

                // Assert
                var record = connection.JobQueue.Find(new BsonDocument()).ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, ObjectId.GenerateNewId().ToString(), "default");
                var processingJob = new MongoFetchedJob(connection, id, ObjectId.GenerateNewId().ToString(), "default");

                // Act
                processingJob.Dispose();

                // Assert
                var record = connection.JobQueue.Find(new BsonDocument()).ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        private static ObjectId CreateJobQueueRecord(HangfireDbContext connection, string jobId, string queue)
        {
            var jobQueue = new JobQueueDto
            {
                JobId = new ObjectId(jobId),
                Queue = queue,
                FetchedAt = connection.GetServerTimeUtc()
            };

            connection.JobQueue.InsertOne(jobQueue);

            return jobQueue.Id;
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