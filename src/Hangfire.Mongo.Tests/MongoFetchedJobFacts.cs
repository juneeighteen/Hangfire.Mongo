using System;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using Hangfire.Mongo.PersistentJobQueue.Mongo;

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
                    () => new MongoFetchedJob(null, JobId, Queue));

                Assert.Equal("connection", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() => new MongoFetchedJob(connection, null, Queue));

                Assert.Equal("jobId", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoFetchedJob(connection, JobId, null));

                Assert.Equal("queue", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            UseConnection(connection =>
            {
                var fetchedJob = new MongoFetchedJob(connection, JobId, Queue);

                Assert.Equal(JobId, fetchedJob.JobId);
                Assert.Equal(Queue, fetchedJob.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            UseConnection(connection =>
            {
                var processingJob = new MongoFetchedJob(connection, ObjectId.GenerateNewId().ToString(), "default");
                
                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = new MongoJobQueueMonitoringApi(connection).GetQueues().Count();
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
                var job3 = ObjectId.GenerateNewId().ToString();

                // Arrange
                CreateJobRecord(connection, job1, "default");
                CreateJobRecord(connection, job3, "critical");
                CreateJobRecord(connection, job2, "default");

                var fetchedJob = new MongoFetchedJob(connection, ObjectId.GenerateNewId().ToString(), "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = new MongoJobQueueMonitoringApi(connection).GetQueues().Count();
                Assert.Equal(2, count);
            });
        }

        [Fact, CleanDatabase]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobRecord(connection, ObjectId.GenerateNewId().ToString(), "default");
                var processingJob = new MongoFetchedJob(connection, id.ToString(), "default");

                // Act
                processingJob.Requeue();

                // Assert
                var record = connection.Job.Find(new BsonDocument()).ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            UseConnection(connection =>
            {
                // Arrange
                var id = CreateJobRecord(connection, ObjectId.GenerateNewId().ToString(), "default");
                var processingJob = new MongoFetchedJob(connection, id.ToString(),  "default");

                // Act
                processingJob.Dispose();

                // Assert
                var record = connection.Job.Find(new BsonDocument()).ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }

        private static ObjectId CreateJobRecord(HangfireDbContext connection, string jobId, string queue)
        {
            var jobQueue = new JobDto
            {
                Id = new ObjectId(jobId),
                Queue = queue,
                FetchedAt = connection.GetServerTimeUtc()
            };

            connection.Job.InsertOne(jobQueue);

            return jobQueue.Id;
        }

    }
#pragma warning restore 1591
}