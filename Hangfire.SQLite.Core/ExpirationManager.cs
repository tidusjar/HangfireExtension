using System;
using System.Threading;
using Dapper;
using Hangfire.Server;

namespace Hangfire.SQLite.Core
{
    internal class ExpirationManager : IServerComponent
    {
        private const string DistributedLockKey = "locks:expirationmanager";
        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromMinutes(5);
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);

        private static readonly string[] ProcessedTables =
        {
            "AggregatedCounter",
            "Job",
            "List",
            "Set",
            "Hash"
        };

        private readonly TimeSpan _checkInterval;

        private readonly SQLiteStorage _storage;

        public ExpirationManager(SQLiteStorage storage)
            : this(storage, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(SQLiteStorage storage, TimeSpan checkInterval)
        {
            if (storage == null) throw new ArgumentNullException("storage");

            _storage = storage;
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in ProcessedTables)
            {
                var removedCount = 0;

                do
                {
                    _storage.UseConnection(connection =>
                    {
                        removedCount = connection.Execute(
                            string.Format(@"
                                delete from [{0}.{1}] where Id in (
                                    select Id from [{0}.{1}]
                                    where ExpireAt < @expireAt
                                    limit @limit)", _storage.GetSchemaName(), table),
                            new {limit = NumberOfRecordsInSinglePass, expireAt = DateTime.UtcNow});
                    }, true);

                    if (removedCount > 0)
                    {
                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount != 0);
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }
    }
}