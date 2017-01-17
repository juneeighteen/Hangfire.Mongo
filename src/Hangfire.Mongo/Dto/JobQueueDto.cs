using System;
using Hangfire.Mongo.MongoUtils;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobQueueDto
    {
        [BsonId()]
        public ObjectId Id { get; set; }

        public ObjectId JobId { get; set; }

        public string Queue { get; set; }

        public DateTime? FetchedAt { get; set; }
    }
#pragma warning restore 1591
}