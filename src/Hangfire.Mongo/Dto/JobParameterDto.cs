using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
#pragma warning disable 1591
    public class JobParameterDto
    {
        public string Name { get; set; }

        public string Value { get; set; }
    }
#pragma warning restore 1591
}