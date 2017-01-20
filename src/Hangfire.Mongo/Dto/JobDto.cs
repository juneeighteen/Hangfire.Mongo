using System;
using Hangfire.Mongo.MongoUtils;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;
using Hangfire.Storage;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobDto
    {
        public JobDto()
        {
            Parameters = new Dictionary<string, string>();
            InvocationData = new InvocationData(null, null, null, null);
        }

        [BsonId()]
        public ObjectId Id { get; set; }

        public ObjectId StateId { get; set; }

        public string StateName { get; set; }

        public InvocationData InvocationData { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime? ExpireAt { get; set; }

        public IDictionary<string, string> Parameters { get; set;  }

        public string Queue { get; set; }

        public DateTime? FetchedAt { get; set; }
    }
#pragma warning restore 1591
}