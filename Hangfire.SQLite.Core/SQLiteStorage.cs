// This file is part of Hangfire.
// Copyright ?2013-2014 Sergey Odinokov.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using Hangfire.Annotations;
using Hangfire.Server;
using Hangfire.Storage;
using System.Threading;
using Hangfire.SQLite.Core;
using Microsoft.Data.Sqlite;

namespace Hangfire.SQLite
{
    public class SQLiteStorage : JobStorage
    {
        private readonly SqliteConnection _existingConnection;
        private readonly SQLiteStorageOptions _options;
        private readonly string _connectionString;
        private static readonly TimeSpan ReaderWriterLockTimeout = TimeSpan.FromSeconds(30);
        private static Dictionary<string, ReaderWriterLockSlim> _dbMonitorCache = new Dictionary<string, ReaderWriterLockSlim>();

        public SQLiteStorage(string nameOrConnectionString)
            : this(nameOrConnectionString, new SQLiteStorageOptions())
        {
        }

        /// <summary>
        /// Initializes SqlServerStorage from the provided SQLiteStorageOptions and either the provided connection
        /// string or the connection string with provided name pulled from the application config file.       
        /// </summary>
        /// <param name="nameOrConnectionString">Either a SQL Server connection string or the name of 
        /// a SQL Server connection string located in the connectionStrings node in the application config</param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="nameOrConnectionString"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="nameOrConnectionString"/> argument is neither 
        /// a valid SQL Server connection string nor the name of a connection string in the application
        /// config file.</exception>
        public SQLiteStorage(string nameOrConnectionString, SQLiteStorageOptions options)
        {
            if (string.IsNullOrEmpty(nameOrConnectionString)) throw new ArgumentNullException(nameof(nameOrConnectionString));

            _options = options ?? throw new ArgumentNullException(nameof(options));

            if (IsConnectionString(nameOrConnectionString))
            {
                _connectionString = nameOrConnectionString;
            }
            //else if (IsConnectionStringInConfiguration(nameOrConnectionString))
            //{
            //_connectionString = ConfigurationManager.ConnectionStrings[nameOrConnectionString].ConnectionString;
            //}
            else
            {
                throw new ArgumentException(
                    string.Format("Could not find connection string with name '{0}' in application config file",
                                  nameOrConnectionString));
            }

            if (!_dbMonitorCache.ContainsKey(_connectionString))
            {
                _dbMonitorCache.Add(_connectionString, new ReaderWriterLockSlim());
            }

            if (options.PrepareSchemaIfNecessary)
            {
                UseConnection(connection =>
                {
                    SQLiteObjectsInstaller.Install(connection, options.SchemaName);
                });
            }

            InitializeQueueProviders();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlServerStorage"/> class with
        /// explicit instance of the <see cref="SqlConnection"/> class that will be used
        /// to query the data.
        /// </summary>
        /// <param name="existingConnection">Existing connection</param>
        public SQLiteStorage([NotNull] SqliteConnection existingConnection)
        {
            _existingConnection = existingConnection ?? throw new ArgumentNullException(nameof(existingConnection));
            _options = new SQLiteStorageOptions();

            InitializeQueueProviders();
        }

        public PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new SQLiteMonitoringApi(this, _options.DashboardJobListLimit);
        }

        public override IStorageConnection GetConnection()
        {
            return new SQLiteStorageConnection(this);
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _options.JobExpirationCheckInterval);
            //yield return new CountersAggregator(this, _options.CountersAggregateInterval);
        }

        //public override void WriteOptionsToLog(ILog logger)
        //{
        //    logger.Info("Using the following options for SQL Server job storage:");
        //    logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);        
        //}

        public override string ToString()
        {
            const string canNotParseMessage = "<Connection string can not be parsed>";

            try
            {
                var connectionStringBuilder = new SqliteConnectionStringBuilder(_connectionString);
                var builder = new StringBuilder();

                builder.Append("Data Source: ");
                builder.Append(connectionStringBuilder.DataSource);
                builder.Append(", Version: ");

                return builder.Length != 0
                    ? String.Format("SQLite Server: {0}", builder)
                    : canNotParseMessage;
            }
            catch (Exception)
            {
                return canNotParseMessage;
            }
        }

        internal void UseConnection([InstantHandle] Action<SqliteConnection> action, bool isWriteLock = false)
        {
            UseConnection(connection =>
            {
                action(connection);
                return true;
            }, isWriteLock);
        }

        internal T UseConnection<T>([InstantHandle] Func<SqliteConnection, T> func, bool isWriteLock = false)
        {
            SqliteConnection connection = null;

            try
            {
                connection = CreateAndOpenConnection(isWriteLock);
                return func(connection);
            }
            finally
            {
                ReleaseConnection(connection);
            }
        }

        internal void UseTransaction([InstantHandle] Action<SqliteConnection> action)
        {
            UseTransaction(connection =>
            {
                action(connection);
                return true;
            }, null);
        }

        internal T UseTransaction<T>([InstantHandle] Func<SqliteConnection, T> func, IsolationLevel? isolationLevel)
        {
            var result = UseConnection(func, true);
            return result;
        }

        internal SqliteConnection CreateAndOpenConnection(bool isWriteLock = false)
        {
            if (_existingConnection != null)
            {
                return _existingConnection;
            }

            if (isWriteLock)
            {
                //_dbMonitorCache[_connectionString].AcquireWriterLock(ReaderWriterLockTimeout);
                _dbMonitorCache[_connectionString].TryEnterWriteLock(ReaderWriterLockTimeout);
            }

            var connection = new SqliteConnection(_connectionString);
            connection.Open();

            return connection;
        }

        internal void ReleaseConnection(IDbConnection connection)
        {
            if (connection != null && !ReferenceEquals(connection, _existingConnection))
            {
                connection.Close();
                connection.Dispose();

                ReleaseDbWriteLock();
            }
        }

        internal void ReleaseDbWriteLock()
        {
            var dbMonitor = _dbMonitorCache[_connectionString];
            if (dbMonitor.IsWriteLockHeld)
            {
                dbMonitor.ExitWriteLock();
            }
        }

        internal string GetSchemaName()
        {
            return _options.SchemaName;
        }

        private void InitializeQueueProviders()
        {
            var defaultQueueProvider = new SQLiteJobQueueProvider(this, _options);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }

        private bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }

        //private bool IsConnectionStringInConfiguration(string connectionStringName)
        //{
        //    var connectionStringSetting = ConfigurationManager.ConnectionStrings[connectionStringName];

        //    return connectionStringSetting != null;
        //}
    }
}