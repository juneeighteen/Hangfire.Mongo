using System;
using Hangfire.Mongo.MongoUtils;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobDto
    {
        public JobDto()
        {
            Parameters = new Dictionary<string, string>();
        }

        [BsonId()]
        public ObjectId Id { get; set; }

        public ObjectId StateId { get; set; }

        public string StateName { get; set; }

        public string InvocationData { get; set; }

        public string Arguments { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime? ExpireAt { get; set; }

        public IDictionary<string, string> Parameters { get; set;  }

        public string Queue { get; set; }

        public DateTime? FetchedAt { get; set; }
    }
#pragma warning restore 1591
}