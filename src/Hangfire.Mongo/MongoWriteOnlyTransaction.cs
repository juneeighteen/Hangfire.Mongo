using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
#pragma warning disable 1591
    public sealed class MongoWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Queue<Action<HangfireDbContext>> _commandQueue = new Queue<Action<HangfireDbContext>>();

        private readonly HangfireDbContext _connection;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public MongoWriteOnlyTransaction(HangfireDbContext connection, PersistentJobQueueProviderCollection queueProviders)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (queueProviders == null)
                throw new ArgumentNullException(nameof(queueProviders));

            _connection = connection;
            _queueProviders = queueProviders;
        }

        public override void Dispose()
        {
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(_ => _.Job.UpdateOne(Builders<JobDto>.Filter.Eq(x => x.Id, new ObjectId(jobId)),
                Builders<JobDto>.Update.Set(x => x.ExpireAt, _connection.GetServerTimeUtc().Add(expireIn))));
        }

        public override void PersistJob(string jobId)
        {
            QueueCommand(_ => _.Job.UpdateOne(Builders<JobDto>.Filter.Eq(x => x.Id, new ObjectId(jobId)),
                Builders<JobDto>.Update.Set(x => x.ExpireAt, null)));
        }

        public override void SetJobState(string jobId, IState state)
        {
            QueueCommand(x =>
            {
                StateDto stateDto = new StateDto
                {
                    JobId = new ObjectId(jobId),
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = _connection.GetServerTimeUtc(),
                    Data = state.SerializeData()
				};
                x.State.InsertOne(stateDto);

                x.Job.UpdateOne(
                    Builders<JobDto>.Filter.Eq(_ => _.Id, new ObjectId(jobId)),
                    Builders<JobDto>.Update.Set(_ => _.StateId, stateDto.Id).Set(_ => _.StateName, state.Name));
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            QueueCommand(_ => _.State.InsertOne(new StateDto
            {
                JobId = new ObjectId(jobId),
                Name = state.Name,
                Reason = state.Reason,
                CreatedAt = _connection.GetServerTimeUtc(),
                Data = state.SerializeData()
			}));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            IPersistentJobQueueProvider provider = _queueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue(_connection);

            QueueCommand(_ =>
            {
                persistentQueue.Enqueue(queue, jobId);
            });
        }

        public override void IncrementCounter(string key)
        {
            QueueCommand(_ => _.Counter.InsertOne(new CounterDto
            {
                Key = key,
                Value = +1
            }));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(_ => _.Counter.InsertOne(new CounterDto
            {
                Key = key,
                Value = +1,
                ExpireAt = _connection.GetServerTimeUtc().Add(expireIn)
            }));
        }

        public override void DecrementCounter(string key)
        {
            QueueCommand(_ => _.Counter.InsertOne(new CounterDto
            {
                Key = key,
                Value = -1
            }));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(_ => _.Counter.InsertOne(new CounterDto
            {
                Key = key,
                Value = -1,
                ExpireAt = _connection.GetServerTimeUtc().Add(expireIn)
            }));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            QueueCommand(_ => _.Set.UpdateMany(Builders<SetDto>.Filter.Eq(x => x.Key, key) & Builders<SetDto>.Filter.Eq(x => x.Value, value),
                Builders<SetDto>.Update.Set(x => x.Score, score),
                new UpdateOptions
                {
                    IsUpsert = true
                }));
        }

        public override void RemoveFromSet(string key, string value)
        {
            QueueCommand(_ => _.Set.DeleteMany(
                Builders<SetDto>.Filter.Eq(x => x.Key, key) &
                Builders<SetDto>.Filter.Eq(x => x.Value, value)));
        }

        public override void InsertToList(string key, string value)
        {
            QueueCommand(_ => _.List.InsertOne(new ListDto
            {
                Key = key,
                Value = value
            }));
        }

        public override void RemoveFromList(string key, string value)
        {
            QueueCommand(_ =>
                _.List.DeleteMany(
                    Builders<ListDto>.Filter.Eq(x => x.Key, key) &
                    Builders<ListDto>.Filter.Eq(x => x.Value, value)));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            QueueCommand(x =>
            {
                if (keepStartingFrom > keepEndingAt)
                {
                    x.List.DeleteMany(Builders<ListDto>.Filter.Eq(_ => _.Key, key));
                    return;
                }
                var items = x.List
                        .Find(new BsonDocument())
                        .SortByDescending(s => s.Id)
                        .Skip(keepStartingFrom)
                        .Limit(keepEndingAt - keepStartingFrom + 1)
                        .Project(p => new { p.Id })
                        .ToList();
                var first = items.FirstOrDefault();
                var last = items.LastOrDefault();

                FilterDefinition<ListDto> filter = null;
                if (first != null)
                    filter = Builders<ListDto>.Filter.Gt(_ => _.Id, first.Id);
                if (last != null)
                    filter = (filter | Builders<ListDto>.Filter.Lt(_ => _.Id, last.Id));
                var finalFilter = Builders<ListDto>.Filter.Eq(_ => _.Key, key);
                if (filter != null)
                    finalFilter = finalFilter & filter;

                x.List.DeleteMany(finalFilter);
            });
                  
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (keyValuePairs == null)
                throw new ArgumentNullException(nameof(keyValuePairs));

            foreach (var keyValuePair in keyValuePairs)
            {
                var pair = keyValuePair;

                QueueCommand(_ => _.Hash.UpdateMany(
                    Builders<HashDto>.Filter.Eq(x => x.Key, key) & Builders<HashDto>.Filter.Eq(x => x.Field, pair.Key),
                    Builders<HashDto>.Update.Set(x => x.Value, pair.Value),
                    new UpdateOptions
                    {
                        IsUpsert = true
                    }));
            }
        }

        public override void RemoveHash(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            QueueCommand(_ => _.Hash.DeleteMany(Builders<HashDto>.Filter.Eq(x => x.Key, key)));
        }

        public override void Commit()
        {
            foreach (var action in _commandQueue)
            {
                action.Invoke(_connection);
            }
        }

        private void QueueCommand(Action<HangfireDbContext> action)
        {
            _commandQueue.Enqueue(action);
        }



        //New methods to support Hangfire pro feature - batches.




        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(_ => _.Set.UpdateMany(Builders<SetDto>.Filter.Eq(x => x.Key, key),
                Builders<SetDto>.Update.Set(x => x.ExpireAt, _connection.GetServerTimeUtc().Add(expireIn))));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(_ => _.List.UpdateMany(Builders<ListDto>.Filter.Eq(x => x.Key, key),
                Builders<ListDto>.Update.Set(x => x.ExpireAt, _connection.GetServerTimeUtc().Add(expireIn))));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(_ => _.Hash.UpdateMany(Builders<HashDto>.Filter.Eq(x => x.Key, key),
                Builders<HashDto>.Update.Set(x => x.ExpireAt, _connection.GetServerTimeUtc().Add(expireIn))));
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(_ => _.Set.UpdateMany(Builders<SetDto>.Filter.Eq(x => x.Key, key),
                Builders<SetDto>.Update.Set(x => x.ExpireAt, null)));
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(_ => _.List.UpdateMany(Builders<ListDto>.Filter.Eq(x => x.Key, key),
                Builders<ListDto>.Update.Set(x => x.ExpireAt, null)));
        }

        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(_ => _.Hash.UpdateMany(Builders<HashDto>.Filter.Eq(x => x.Key, key),
                Builders<HashDto>.Update.Set(x => x.ExpireAt, null)));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            foreach (var item in items)
            {
                QueueCommand(_ => _.Set.UpdateMany(
                    Builders<SetDto>.Filter.Eq(x => x.Key, key) & Builders<SetDto>.Filter.In(x => x.Value, items),
                    Builders<SetDto>.Update.Set(x => x.Score, 0.0),
                    new UpdateOptions
                    {
                        IsUpsert = true
                    }));
            }
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(_ => _.Set.DeleteMany(Builders<SetDto>.Filter.Eq(x => x.Key, key)));
        }
    }
#pragma warning restore 1591
}