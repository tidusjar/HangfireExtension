using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.SQLite.Core.Entities;
using Hangfire.Storage;

namespace Hangfire.SQLite.Core
{
    internal class SQLiteStorageConnection : JobStorageConnection
    {
        private readonly SQLiteStorage _storage;

        public SQLiteStorageConnection([NotNull] SQLiteStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new SQLiteWriteOnlyTransaction(_storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new SQLiteDistributedLock();
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));

            var providers = queues
                .Select(queue => _storage.QueueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(string.Format(
                    "Multiple provider instances registered for queues: {0}. You should choose only one type of persistent queues per server instance.",
                    string.Join(", ", queues)));
            }

            var persistentQueue = providers[0].GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var createJobSql = string.Format(@"
insert into [{0}.Job] (InvocationData, Arguments, CreatedAt, ExpireAt)
values (@invocationData, @arguments, @createdAt, @expireAt);
SELECT last_insert_rowid()", _storage.GetSchemaName());

            var invocationData = InvocationData.Serialize(job);

            return _storage.UseConnection(connection =>
            {
                var jobId = connection.Query<int>(
                    createJobSql,
                    new
                    {
                        invocationData = JobHelper.ToJson(invocationData),
                        arguments = invocationData.Arguments,
                        createdAt,
                        expireAt = createdAt.Add(expireIn)
                    }).Single().ToString();

                if (parameters.Count > 0)
                {
                    var parameterArray = new object[parameters.Count];
                    var parameterIndex = 0;
                    foreach (var parameter in parameters)
                    {
                        parameterArray[parameterIndex++] = new
                        {
                            jobId,
                            name = parameter.Key,
                            value = parameter.Value
                        };
                    }

                    var insertParameterSql = string.Format(@"
insert into [{0}.JobParameter] (JobId, Name, Value)
values (@jobId, @name, @value)", _storage.GetSchemaName());

                    connection.Execute(insertParameterSql, parameterArray);
                }

                return jobId;
            }, true);
        }

        public override JobData GetJobData(string id)
        {
            if (id == null) throw new ArgumentNullException("id");

            var sql =
                string.Format(@"select InvocationData, StateName, Arguments, CreatedAt from [{0}.Job] where Id = @id",
                    _storage.GetSchemaName());

            return _storage.UseConnection(connection =>
            {
                var jobData = connection.Query<SqlJob>(sql, new {id})
                    .SingleOrDefault();

                if (jobData == null) return null;

                // TODO: conversion exception could be thrown.
                var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
                invocationData.Arguments = jobData.Arguments;

                Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.Deserialize();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = jobData.StateName,
                    CreatedAt = jobData.CreatedAt,
                    LoadException = loadException
                };
            });
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

            var sql = string.Format(@"
select s.Name, s.Reason, s.Data
from [{0}.State] s
inner join [{0}.Job] j on j.StateId = s.Id
where j.Id = @jobId", _storage.GetSchemaName());

            return _storage.UseConnection(connection =>
            {
                var sqlState = connection.Query<SqlState>(sql, new {jobId}).SingleOrDefault();
                if (sqlState == null)
                {
                    return null;
                }

                var data = new Dictionary<string, string>(
                    JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data),
                    StringComparer.OrdinalIgnoreCase);

                return new StateData
                {
                    Name = sqlState.Name,
                    Reason = sqlState.Reason,
                    Data = data
                };
            });
        }

        public override void SetJobParameter(string jobId, string name, string value)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");
            if (name == null) throw new ArgumentNullException("name");

            _storage.UseConnection(connection =>
            {
                var tableName = string.Format("[{0}.JobParameter]", _storage.GetSchemaName());
                var fetchedParam = connection
                    .Query<JobParameter>(
                        string.Format("select * from {0} where JobId = @jobId and Name = @name", tableName),
                        new {jobId, name}).Any();

                if (!fetchedParam)
                {
                    // insert
                    connection.Execute(
                        string.Format(@"insert into {0} (JobId, Name, Value) values (@jobId, @name, @value);",
                            tableName),
                        new {jobId, name, value});
                }
                else
                {
                    // update
                    connection.Execute(
                        string.Format(@"update {0} set Name = @name, Value = @value where JobId = @jobId;", tableName),
                        new {jobId, name, value});
                }
            }, true);
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException("id");
            if (name == null) throw new ArgumentNullException("name");

            return _storage.UseConnection(connection => connection.Query<string>(
                    string.Format(@"select Value from [{0}.JobParameter] where JobId = @id and Name = @name",
                        _storage.GetSchemaName()),
                    new {id, name})
                .SingleOrDefault());
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var result = connection.Query<string>(
                    string.Format(@"select Value from [{0}.Set] where [Key] = @key", _storage.GetSchemaName()),
                    new {key});

                return new HashSet<string>(result);
            });
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return _storage.UseConnection(connection => connection.Query<string>(
                    string.Format(
                        @"select Value from [{0}.Set] where [Key] = @key and Score between @from and @to order by Score limit 1",
                        _storage.GetSchemaName()),
                    new {key, from = fromScore, to = toScore})
                .SingleOrDefault());
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

//            const string sql = @"
//;merge [HangFire.Hash] with (holdlock) as Target
//using (VALUES (@key, @field, @value)) as Source ([Key], Field, Value)
//on Target.[Key] = Source.[Key] and Target.Field = Source.Field
//when matched then update set Value = Source.Value
//when not matched then insert ([Key], Field, Value) values (Source.[Key], Source.Field, Source.Value);";

            _storage.UseTransaction(connection =>
            {
                var tableName = string.Format("[{0}.Hash]", _storage.GetSchemaName());
                var selectSqlStr = string.Format("select * from {0} where [Key] = @key and Field = @field", tableName);
                var insertSqlStr = string.Format("insert into {0} ([Key], Field, Value) values (@key, @field, @value)",
                    tableName);
                var updateSqlStr = string.Format("update {0} set Value = @value where [Key] = @key and Field = @field ",
                    tableName);
                foreach (var keyValuePair in keyValuePairs)
                {
                    var fetchedHash = connection.Query<SqlHash>(selectSqlStr,
                        new {key, field = keyValuePair.Key});
                    if (!fetchedHash.Any())
                    {
                        connection.Execute(insertSqlStr,
                            new {key, field = keyValuePair.Key, value = keyValuePair.Value});
                    }
                    else
                    {
                        connection.Execute(updateSqlStr,
                            new {key, field = keyValuePair.Key, value = keyValuePair.Value});
                    }
                }
            });
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection =>
            {
                var result = connection.Query<SqlHash>(
                        string.Format("select Field, Value from [{0}.Hash] where [Key] = @key",
                            _storage.GetSchemaName()),
                        new {key})
                    .ToDictionary(x => x.Field, x => x.Value);

                return result.Count != 0 ? result : null;
            });
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");
            if (context == null) throw new ArgumentNullException("context");

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow
            };

            _storage.UseConnection(connection =>
            {
                var tableName = string.Format("[{0}.Server]", _storage.GetSchemaName());
                // select by serverId
                var serverResult = connection.Query<Entities.Server>(
                    string.Format("select * from {0} where Id = @id", tableName),
                    new {id = serverId}).SingleOrDefault();

                if (serverResult == null)
                {
                    // if not found insert
                    connection.Execute(
                        string.Format("insert into {0} (Id, Data, LastHeartbeat) values (@id, @data, @lastHeartbeat)",
                            tableName),
                        new {id = serverId, data = JobHelper.ToJson(data), lastHeartbeat = DateTime.UtcNow});
                }
                else
                {
                    // if found, update data + heartbeart
                    connection.Execute(
                        string.Format("update {0} set Data = @data, LastHeartbeat = @lastHeartbeat where Id = @id",
                            tableName),
                        new {id = serverId, data = JobHelper.ToJson(data), lastHeartbeat = DateTime.UtcNow});
                }
            }, true);

            //_connection.Execute(
            //    @";merge [HangFire.Server] with (holdlock) as Target "
            //    + @"using (VALUES (@id, @data, @heartbeat)) as Source (Id, Data, Heartbeat) "  // << SOURCE
            //    + @"on Target.Id = Source.Id "
            //    + @"when matched then UPDATE set Data = Source.Data, LastHeartbeat = Source.Heartbeat "
            //    + @"when not matched then INSERT (Id, Data, LastHeartbeat) values (Source.Id, Source.Data, Source.Heartbeat);",
            //    new { id = serverId, data = JobHelper.ToJson(data), heartbeat = DateTime.UtcNow });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    string.Format(@"delete from [{0}.Server] where Id = @id", _storage.GetSchemaName()),
                    new {id = serverId});
            }, true);
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException("serverId");

            _storage.UseConnection(connection =>
            {
                connection.Execute(
                    string.Format(@"update [{0}.Server] set LastHeartbeat = @lastHeartbeat where Id = @id",
                        _storage.GetSchemaName()),
                    new {id = serverId, lastHeartbeat = DateTime.UtcNow});
            }, true);
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            return _storage.UseConnection(connection => connection.Execute(
                string.Format(@"delete from [{0}.Server] where LastHeartbeat < @timeOutAt", _storage.GetSchemaName()),
                new {timeOutAt = DateTime.UtcNow.Add(timeOut.Negate())}), true);
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return _storage.UseConnection(connection => connection.Query<int>(
                string.Format("select count([Key]) from [{0}.Set] where [Key] = @key", _storage.GetSchemaName()),
                new {key}).First());
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException("key");

            var query = string.Format(@"
select [Value] 
from [{0}.Set]
where [Key] = @key 
order by Id asc
limit @limit offset @offset", _storage.GetSchemaName());

            return _storage.UseConnection(connection => connection
                .Query<string>(query, new {key, limit = endingAt - startingFrom + 1, offset = startingFrom})
                .ToList());
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var query = string.Format(@"
select min([ExpireAt]) from [{0}.Set]
where [Key] = @key", _storage.GetSchemaName());

            return _storage.UseConnection(connection =>
            {
                var result = connection.Query<DateTime?>(query, new {key}).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value.ToLocalTime() - DateTime.UtcNow.ToLocalTime();
            });
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var query = string.Format(@"
select sum(s.[Value]) from (select sum([Value]) as [Value] from [{0}.Counter]
where [Key] = @key
union all
select [Value] from [{0}.AggregatedCounter]
where [Key] = @key) as s", _storage.GetSchemaName());

            return _storage.UseConnection(connection =>
                connection.Query<long?>(query, new {key}).Single() ?? 0);
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var query = string.Format(@"
select count([Id]) from [{0}.Hash]
where [Key] = @key", _storage.GetSchemaName());

            return _storage.UseConnection(connection => connection.Query<long>(query, new {key}).Single());
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var query = string.Format(@"
select min([ExpireAt]) from [{0}.Hash]
where [Key] = @key", _storage.GetSchemaName());

            return _storage.UseConnection(connection =>
            {
                var result = connection.Query<DateTime?>(query, new {key}).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value.ToLocalTime() - DateTime.UtcNow.ToLocalTime();
            });
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (name == null) throw new ArgumentNullException("name");

            var query = string.Format(@"
select [Value] from [{0}.Hash]
where [Key] = @key and [Field] = @field", _storage.GetSchemaName());

            return _storage.UseConnection(connection => connection
                .Query<string>(query, new {key, field = name}).SingleOrDefault());
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var query = string.Format(@"
select count([Id]) from [{0}.List]
where [Key] = @key", _storage.GetSchemaName());

            return _storage.UseConnection(connection => connection.Query<long>(query, new {key}).Single());
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var query = string.Format(@"
select min([ExpireAt]) from [{0}.List]
where [Key] = @key", _storage.GetSchemaName());

            return _storage.UseConnection(connection =>
            {
                var result = connection.Query<DateTime?>(query, new {key}).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value.ToLocalTime() - DateTime.UtcNow.ToLocalTime();
            });
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException("key");

            var query = string.Format(@"
	select [Value] 
	from [{0}.List]
	where [Key] = @key 
	order by Id desc
	limit @limit offset @offset", _storage.GetSchemaName());

            return _storage.UseConnection(connection => connection
                .Query<string>(query, new {key, limit = endingAt - startingFrom + 1, offset = startingFrom})
                .ToList());
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var query = string.Format(@"
select [Value] from [{0}.List]
where [Key] = @key
order by [Id] desc", _storage.GetSchemaName());

            return _storage.UseConnection(connection => connection.Query<string>(query, new {key}).ToList());
        }
    }
}