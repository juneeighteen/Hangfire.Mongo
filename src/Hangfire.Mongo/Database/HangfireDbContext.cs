using System;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization;
using Hangfire.Storage;

namespace Hangfire.Mongo.Database
{
    /// <summary>
    /// Represents Mongo database context for Hangfire
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
    public class HangfireDbContext : IDisposable
    {
        private const int RequiredSchemaVersion = 7;

        private readonly string _prefix;

        internal IMongoDatabase Database { get; }

        static HangfireDbContext()
        {
            BsonClassMap.RegisterClassMap<InvocationData>(cm =>
            {
                cm.MapCreator(p => new InvocationData(p.Type, p.Method, p.ParameterTypes, p.Arguments));
                cm.MapProperty(p => p.Arguments);
                cm.MapProperty(p => p.ParameterTypes);
                cm.MapProperty(p => p.Method);
                cm.MapProperty(p => p.Type);
            });
        }

        /// <summary>
        /// Constructs context with connection string and database name
        /// </summary>
        /// <param name="connectionString">Connection string for Mongo database</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(string connectionString, string databaseName, string prefix = "hangfire")
        {
            _prefix = prefix;

            var client = new MongoClient(connectionString);
            
            Database = client.GetDatabase(databaseName);
            InitCollection();
            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Constructs context with Mongo client settings and database name
        /// </summary>
        /// <param name="mongoClientSettings">Client settings for MongoDB</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="prefix">Collections prefix</param>
        public HangfireDbContext(MongoClientSettings mongoClientSettings, string databaseName, string prefix = "hangfire")
        {
            _prefix = prefix;

            var client = new MongoClient(mongoClientSettings);

            Database = client.GetDatabase(databaseName);
            InitCollection();
            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Constructs context with existing Mongo database connection
        /// </summary>
        /// <param name="database">Database connection</param>
        public HangfireDbContext(IMongoDatabase database)
        {
            Database = database;
            InitCollection();
            ConnectionId = Guid.NewGuid().ToString();
        }

        private IMongoCollection<T> GetCollection<T>(string suffix)
        {
            return Database.GetCollection<T>(_prefix + "." + suffix);
        }

        /// <summary>
        /// Mongo database connection identifier
        /// </summary>
        public string ConnectionId { get; private set; }

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        public virtual IMongoCollection<DistributedLockDto> DistributedLock { get; private set; }

        /// <summary>
        /// Reference to collection which contains counters
        /// </summary>
        public virtual IMongoCollection<CounterDto> Counter { get; private set; }

        /// <summary>
        /// Reference to collection which contains aggregated counters
        /// </summary>
        public virtual IMongoCollection<AggregatedCounterDto> AggregatedCounter { get; private set; }

        /// <summary>
        /// Reference to collection which contains hashes
        /// </summary>
        public virtual IMongoCollection<HashDto> Hash { get; private set; }

        /// <summary>
        /// Reference to collection which contains jobs
        /// </summary>
        public virtual IMongoCollection<JobDto> Job { get; private set; }

        /// <summary>
        /// Reference to collection which contains lists
        /// </summary>
        public virtual IMongoCollection<ListDto> List { get; private set; }

        /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        public virtual IMongoCollection<SchemaDto> Schema { get; private set; }

        /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        public virtual IMongoCollection<ServerDto> Server { get; private set; }

        /// <summary>
        /// Reference to collection which contains sets
        /// </summary>
        public virtual IMongoCollection<SetDto> Set { get; private set; }

        /// <summary>
        /// Reference to collection which contains states
        /// </summary>
        public virtual IMongoCollection<StateDto> State { get; private set; }

        /// <summary>
        /// Initializes intial collections schema for Hangfire
        /// </summary>
        public void Init()
        {
            var schema = Schema.Find(new BsonDocument()).FirstOrDefault();
            if (schema != null)
            {
                if (RequiredSchemaVersion > schema.Version)
                {
                    Schema.DeleteMany(new BsonDocument());
                    Schema.InsertOne(new SchemaDto { Version = RequiredSchemaVersion });
                }
                else if (RequiredSchemaVersion < schema.Version)
                {
                    throw new InvalidOperationException($"HangFire current database schema version {schema.Version} is newer than the configured MongoStorage schema version {RequiredSchemaVersion}. Please update to the latest HangFire.Mongo NuGet package.");
                }
            }
            else
            {
                Schema.InsertOne(new SchemaDto { Version = RequiredSchemaVersion });
            }

            CreateJobIndexes();
        }

        private void InitCollection()
        {
            DistributedLock = GetCollection<DistributedLockDto>("locks");
            Counter = GetCollection<CounterDto>("counter");
            AggregatedCounter = GetCollection<AggregatedCounterDto>("aggregate");
            Hash = GetCollection<HashDto>("hash");
            Job = GetCollection<JobDto>("job");
            List = GetCollection<ListDto>("list");
            Schema = GetCollection<SchemaDto>("schema");
            Server = GetCollection<ServerDto>("server");
            Set = GetCollection<SetDto>("set");
            State = GetCollection<StateDto>("state");
        }

        private void CreateJobIndexes()
        {
            var background = new CreateIndexOptions() { Background = true };
            // Create for jobid on state, jobParameter, jobQueue
            State.CreateDescendingIndex(p => p.JobId);
            Job.Indexes.CreateOne(Builders<JobDto>.IndexKeys.Ascending(p => p.Queue).Ascending(p => p.FetchedAt));
            //List.Indexes.CreateOne(Builders<ListDto>.IndexKeys.Ascending(p => p.Key));
            Set.Indexes.CreateOne(Builders<SetDto>.IndexKeys.Ascending(p => p.Key), background);
            Hash.Indexes.CreateOne(Builders<HashDto>.IndexKeys.Ascending(p => p.Key), background);
            Counter.Indexes.CreateOne(Builders<CounterDto>.IndexKeys.Ascending(p => p.Key), background);
            AggregatedCounter.Indexes.CreateOne(Builders<AggregatedCounterDto>.IndexKeys.Ascending(p => p.Key), background);
            CreateTTLIndexes();
        }

        private void CreateTTLIndexes()
        {
            //create ttl indexes to avoid polling 
            var indexOption = new CreateIndexOptions() { ExpireAfter = TimeSpan.Zero };
            AggregatedCounter.Indexes.CreateOne(Builders<AggregatedCounterDto>.IndexKeys.Ascending(_ => _.ExpireAt), indexOption);
            Counter.Indexes.CreateOne(Builders<CounterDto>.IndexKeys.Ascending(_ => _.ExpireAt), indexOption);
            Job.Indexes.CreateOne(Builders<JobDto>.IndexKeys.Ascending(_ => _.ExpireAt), indexOption);
            List.Indexes.CreateOne(Builders<ListDto>.IndexKeys.Ascending(_ => _.ExpireAt), indexOption);
            Set.Indexes.CreateOne(Builders<SetDto>.IndexKeys.Ascending(_ => _.ExpireAt), indexOption);
            Hash.Indexes.CreateOne(Builders<HashDto>.IndexKeys.Ascending(_ => _.ExpireAt), indexOption);
        }


        /// <summary>
        /// Disposes the object
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly",
            Justification = "Dispose should only implement finalizer if owning an unmanaged resource")]
        public void Dispose()
        {
        }
    }
}