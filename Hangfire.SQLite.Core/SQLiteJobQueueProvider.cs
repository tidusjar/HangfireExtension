﻿// This file is part of Hangfire.
// Copyright © 2013-2014 Sergey Odinokov.
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
using Hangfire.Annotations;
using Hangfire.SQLite.Core;

namespace Hangfire.SQLite
{
    internal class SQLiteJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly IPersistentJobQueue _jobQueue;
        private readonly IPersistentJobQueueMonitoringApi _monitoringApi;

        public SQLiteJobQueueProvider([NotNull] SQLiteStorage storage, [NotNull] SQLiteStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (options == null) throw new ArgumentNullException("options");

            _jobQueue = new SQLiteJobQueue(storage, options);
            _monitoringApi = new SQLiteJobQueueMonitoringApi(storage);
        }

        public IPersistentJobQueue GetJobQueue()
        {
            return _jobQueue;
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi()
        {
            return _monitoringApi;
        }
    }
}