using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using System.Linq;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class ExpirationManagerFacts
    {
        private readonly MongoStorage _storage;

        public ExpirationManagerFacts()
        {
            _storage = new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, connection.GetServerTimeUtc().AddMonths(-1));
                Assert.True(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, null);
                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.Now.AddMonths(1));

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        private static ObjectId CreateExpirationEntry(HangfireDbContext connection, DateTime? expireAt)
        {
            var counter = new AggregatedCounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key",
                Value = 1,
                ExpireAt = expireAt
            };
            connection.AggregatedCounter.InsertOne(counter);

            var id = counter.Id;

            return id;
        }

        private static bool IsEntryExpired(HangfireDbContext connection, ObjectId entryId)
        {
            var count = connection.AggregatedCounter.Find(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Id, entryId)).Count();
            return count == 0;
        }
    }
#pragma warning restore 1591
}